#!/usr/bin/env node
/**
 * Dealality Dev OS — Worktree Lifecycle CLI
 *
 *   npm run dealality:worktree-lifecycle -- inventory
 *   npm run dealality:worktree-lifecycle -- inventory --json --out reports/...
 *   npm run dealality:worktree-lifecycle -- propose [--batch-limit 8] [--out ...]
 *   npm run dealality:worktree-lifecycle -- assess-after-merge --pr <n> --merged-branch <name> [--write-registry]
 *   npm run dealality:worktree-lifecycle -- retire --proposal <file> --founder-approve <PHRASE> [--apply]
 *
 * Never deletes without exact founder approval. Never uses git worktree remove --force.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessTemporaryWorktreesAfterMerge,
  buildRetirementProposal,
  formatInventorySummary,
  inventoryAllWorktrees,
  retireApprovedBatch,
} from "../lib/dealality-dev-os/worktree-lifecycle-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function getArg(argv, name) {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : "";
}

function has(argv, name) {
  return argv.includes(name);
}

const argv = process.argv.slice(2);
const cmd = argv.find((a) => !a.startsWith("-")) || "inventory";
const json = has(argv, "--json");
const out = getArg(argv, "--out");

if (has(argv, "--force") || has(argv, "--force-remove")) {
  console.error("[dealality:worktree-lifecycle] BLOCKED: --force is refused by lifecycle policy");
  process.exit(2);
}

function writeOut(payload) {
  const text = typeof payload === "string" ? payload : `${JSON.stringify(payload, null, 2)}\n`;
  if (out) {
    fs.mkdirSync(path.dirname(path.resolve(ROOT, out)), { recursive: true });
    fs.writeFileSync(path.resolve(ROOT, out), text, "utf8");
    console.error(`[dealality:worktree-lifecycle] wrote ${out}`);
  }
  if (json || out) {
    if (!out || json) process.stdout.write(typeof payload === "string" ? payload : text);
  }
}

if (cmd === "inventory") {
  const inventory = inventoryAllWorktrees({ repoRoot: ROOT });
  if (json || out) {
    writeOut(inventory);
    if (!json && out) console.log(formatInventorySummary(inventory));
  } else {
    console.log(formatInventorySummary(inventory));
  }
  process.exit(0);
}

if (cmd === "propose") {
  const inventory = inventoryAllWorktrees({ repoRoot: ROOT });
  const batchLimit = Number(getArg(argv, "--batch-limit") || 8);
  const proposal = buildRetirementProposal(inventory, { batchLimit });
  const defaultOut =
    out ||
    path.join(
      "reports",
      "ai-build-system",
      `worktree-lifecycle-retirement-proposal-${proposal.batchId}.json`,
    );
  fs.mkdirSync(path.dirname(path.resolve(ROOT, defaultOut)), { recursive: true });
  fs.writeFileSync(path.resolve(ROOT, defaultOut), `${JSON.stringify(proposal, null, 2)}\n`, "utf8");

  // Human-readable summary alongside
  const mdPath = defaultOut.replace(/\.json$/i, ".md");
  const md = [
    `# Worktree lifecycle retirement proposal`,
    ``,
    `**Batch ID:** \`${proposal.batchId}\``,
    `**Created:** ${proposal.createdAt}`,
    `**origin/main:** \`${proposal.originMain}\``,
    `**Worktrees inventoried:** ${proposal.worktreeCount}`,
    ``,
    `## Classification summary`,
    ``,
    ...Object.entries(proposal.summary).map(([k, v]) => `- **${k}:** ${v}`),
    ``,
    `## Proposed next batch (${proposal.proposedBatch.length}) — LOW risk`,
    ``,
    `| # | Path | Stream | Recoverable (est.) | Junctions |`,
    `|---|---|---|---|---|`,
    ...proposal.proposedBatch.map(
      (r) =>
        `| ${r.order} | \`${r.path}\` | ${r.stream} | ${r.recoverableBytesEstimate} | ${r.junctions?.length || 0} |`,
    ),
    ``,
    `**Estimated recoverable (batch):** ${proposal.proposedBatchRecoverableBytesEstimate} bytes`,
    ``,
    `## Safety`,
    ``,
    `- Never auto-delete`,
    `- Never \`git worktree remove --force\``,
    `- Junction targets preserved`,
    `- Permanent workspaces preserved`,
    `- Branches managed separately`,
    ``,
    `## Founder approval`,
    ``,
    `Reply with exactly:`,
    ``,
    `\`${proposal.founderApproval.phrase}\``,
    ``,
    `Then run:`,
    ``,
    `\`\`\`bash`,
    `npm run dealality:worktree-lifecycle -- retire --proposal ${defaultOut.replace(/\\/g, "/")} --founder-approve "${proposal.founderApproval.phrase}"`,
    `# review dry-run, then:`,
    `npm run dealality:worktree-lifecycle -- retire --proposal ${defaultOut.replace(/\\/g, "/")} --founder-approve "${proposal.founderApproval.phrase}" --apply`,
    `\`\`\``,
    ``,
    `## Recommended next historical cleanup batch`,
    ``,
    `| # | Risk | Path | Dirty | Blockers |`,
    `|---|---|---|---|---|`,
    ...(proposal.recommendedNextHistoricalBatch || []).map(
      (r) =>
        `| ${r.order} | ${r.risk} | \`${r.path}\` | ${r.dirtyCount} | ${(r.blockers || []).join(", ") || "-"} |`,
    ),
    ``,
    `## Hold groups (not auto-retired)`,
    ``,
    `- PRESERVATION_REQUIRED: ${proposal.groups.preservationRequired.length}`,
    `- lowRiskAfterPreservationAck: ${proposal.groups.lowRiskAfterPreservationAck?.length || 0}`,
    `- BLOCKED: ${proposal.groups.blocked.length}`,
    `- ACTIVE: ${proposal.groups.active.length}`,
    `- PERMANENT: ${proposal.groups.permanent.length}`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.resolve(ROOT, mdPath), md, "utf8");

  if (json) {
    process.stdout.write(`${JSON.stringify(proposal, null, 2)}\n`);
  } else {
    console.log(`[dealality:worktree-lifecycle] proposal batchId=${proposal.batchId}`);
    console.log(`  summary=${JSON.stringify(proposal.summary)}`);
    console.log(`  proposedBatch=${proposal.proposedBatch.length}`);
    console.log(`  wrote ${defaultOut}`);
    console.log(`  wrote ${mdPath}`);
    console.log(`  approval phrase: ${proposal.founderApproval.phrase}`);
  }
  process.exit(0);
}

if (cmd === "assess-after-merge") {
  const pr = getArg(argv, "--pr");
  const mergedBranch = getArg(argv, "--merged-branch");
  if (!pr && !mergedBranch) {
    console.error("[dealality:worktree-lifecycle] assess-after-merge requires --pr and/or --merged-branch");
    process.exit(2);
  }
  const result = assessTemporaryWorktreesAfterMerge({
    repoRoot: ROOT,
    pr: pr || null,
    mergedBranch: mergedBranch || null,
    writeRegistry: has(argv, "--write-registry"),
  });
  writeOut(result);
  if (!json) {
    console.log(
      `[dealality:worktree-lifecycle] assess-after-merge matched=${result.matchedCount} candidates=${result.retirementCandidates} registryUpdated=${result.registryUpdated}`,
    );
  }
  process.exit(0);
}

if (cmd === "retire") {
  const proposalPath = getArg(argv, "--proposal");
  const phrase = getArg(argv, "--founder-approve") || getArg(argv, "--founder-approval");
  const apply = has(argv, "--apply");
  if (!proposalPath) {
    console.error("[dealality:worktree-lifecycle] retire requires --proposal <file>");
    process.exit(2);
  }
  const proposal = JSON.parse(fs.readFileSync(path.resolve(ROOT, proposalPath), "utf8"));
  const result = retireApprovedBatch({
    repoRoot: ROOT,
    proposal,
    founderApprovalPhrase: phrase,
    apply,
  });
  writeOut(result);
  if (!json) {
    console.log(
      `[dealality:worktree-lifecycle] retire status=${result.status} ok=${result.ok} applied=${result.applied} partial=${Boolean(result.partial)}`,
    );
    for (const r of result.results || []) {
      console.log(`  - ${r.path} ok=${r.ok} applied=${Boolean(r.applied)} status=${r.status || "-"}`);
    }
  }
  process.exit(result.ok ? 0 : 1);
}

console.error(`Unknown command '${cmd}'. Use: inventory | propose | assess-after-merge | retire`);
process.exit(2);
