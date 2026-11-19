#!/usr/bin/env node
/**
 * Universal PR completion + workspace ownership report (read-only).
 *
 *   npm run dealality:pr-completion-report -- --stream he --pr 429
 *   npm run dealality:pr-completion-report -- --audit
 *   npm run dealality:pr-completion-report -- --stream gdi --pr 429 --json
 *
 * Prefer GitHub PR evidence (`gh pr view`). Null/unknown merge or CI cannot COMPLETE.
 * Never resets, cleans, stashes, or switches branches.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  auditWorkspaceOwnership,
  buildPrCompletionReport,
  formatPrCompletionReport,
  getOwnershipEnforcementCoverage,
} from "../lib/dealality-dev-os/pr-completion-ownership.js";

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
const json = has(argv, "--json");
const audit = has(argv, "--audit");
const coverage = has(argv, "--coverage");
const stream = (getArg(argv, "--stream") || "").toLowerCase();
const pr = getArg(argv, "--pr") || null;
const mergeSha = getArg(argv, "--merge-sha") || null;
const cursorActiveBranch = getArg(argv, "--cursor-branch") || null;
const featureBranchRetired = getArg(argv, "--feature-branch-retired") || null;
const peerImpact = getArg(argv, "--peer-impact") || null;
const ignoredEvidence = getArg(argv, "--ignored-evidence") || null;
const noGithub = has(argv, "--no-github");

const mergedRaw = getArg(argv, "--merged");
const ciRaw = getArg(argv, "--ci-passed");
const merged =
  mergedRaw === "" ? null : mergedRaw === "true" || mergedRaw === "1" ? true : mergedRaw === "false" || mergedRaw === "0" ? false : null;
const ciPassed =
  ciRaw === "" ? null : ciRaw === "true" || ciRaw === "1" ? true : ciRaw === "false" || ciRaw === "0" ? false : null;

if (coverage) {
  const cov = getOwnershipEnforcementCoverage();
  if (json) console.log(JSON.stringify(cov, null, 2));
  else {
    console.log(`[dealality:pr-completion] enforcement=${cov.enforcementLevel}`);
    for (const row of cov.machineEnforced) console.log(`  MACHINE ${row.entry}`);
    for (const row of cov.instructionGovernedOnly) console.log(`  INSTRUCTION_ONLY ${row.entry}`);
    for (const b of cov.unavoidableBypasses) console.log(`  BYPASS ${b}`);
  }
  process.exit(0);
}

if (audit) {
  const snap = auditWorkspaceOwnership(ROOT);
  if (json) {
    console.log(JSON.stringify(snap, null, 2));
  } else {
    console.log(`[dealality:pr-completion] ownership audit (${snap.rows.length} permanent workspaces)`);
    console.log(`  enforcement=${snap.enforcement.enforcementLevel}`);
    for (const row of snap.rows) {
      console.log(
        `  [${row.status}] ${row.stream} path=${row.path} branch=${row.branch} dirty=${row.dirtyCount}`,
      );
    }
  }
  const blocked = snap.rows.some((r) => String(r.status).startsWith("BLOCKED"));
  process.exit(blocked ? 2 : 0);
}

if (!stream) {
  console.error(
    "Usage: dealality:pr-completion-report -- --stream <he|adp|gdi|integration> [--pr n] [--audit] [--coverage]",
  );
  process.exit(2);
}

const report = buildPrCompletionReport({
  registryRoot: ROOT,
  stream,
  pr,
  mergeSha,
  merged,
  ciPassed,
  cursorActiveBranch,
  featureBranchRetired,
  peerImpact,
  ignoredEvidencePreserved: ignoredEvidence,
  queryGithub: !noGithub && Boolean(pr),
});

if (json) console.log(JSON.stringify(report, null, 2));
else console.log(formatPrCompletionReport(report));

process.exit(report.ok ? 0 : 2);
