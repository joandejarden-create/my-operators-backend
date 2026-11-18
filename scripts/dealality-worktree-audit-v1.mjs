#!/usr/bin/env node
/**
 * Read-only live worktree audit (does not write the shared registry).
 *
 *   npm run dealality:worktree-audit
 *   npm run dealality:worktree-audit -- --json
 *   npm run dealality:worktree-audit -- --classify
 */
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  findWorkspaceByPath,
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  normalizePath,
} from "../lib/dealality-dev-os/worktree-registry.js";
import { collectDirtyPaths, parseWorktreeList, resolveGitRoot } from "../lib/dealality-dev-os/git-preflight.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const json = process.argv.includes("--json");
const classify = process.argv.includes("--classify");
const withPrs = process.argv.includes("--with-prs");

function git(root, args) {
  try {
    return execFileSync("git", args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  } catch {
    return "";
  }
}

function openPrForBranch(branch) {
  if (!branch || branch === "(detached)") return null;
  try {
    const out = execFileSync(
      "gh",
      ["pr", "list", "--state", "open", "--head", branch, "--json", "number,url,title", "--limit", "1"],
      { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    );
    const arr = JSON.parse(out || "[]");
    return arr[0] || null;
  } catch {
    return null;
  }
}

const root = resolveGitRoot(ROOT);
const registry = loadWorktreeRegistry(root);
const originMain = git(root, ["rev-parse", "origin/main"]);
const worktrees = parseWorktreeList(root);

const rows = worktrees.map((wt) => {
  const abs = wt.path;
  const exists = true;
  const dirty = collectDirtyPaths(abs);
  const branch = wt.branch || "(detached)";
  const ab = git(abs, ["rev-list", "--left-right", "--count", `origin/main...HEAD`]);
  const [behindStr, aheadStr] = String(ab || "0\t0").split(/\r?\n/)[0].split(/\s+/);
  const behind = Number(behindStr || 0);
  const ahead = Number(aheadStr || 0);
  const registered = findWorkspaceByPath(registry, abs);
  let className = "STALE_UNCERTAIN";
  if (registered?.lifecycle === "permanent" || registered?.lifecycle === "temporary") {
    className = dirty.length ? "VALUABLE_WIP" : registered.status === "active" || registered.status === "post_merge_retain" || registered.id === "integration" ? "ACTIVE" : "ACTIVE";
  } else if (dirty.length) {
    className = "VALUABLE_WIP";
  } else if (ahead === 0) {
    className = "MERGED_CLEAN";
  }
  const pr = withPrs ? openPrForBranch(branch) : null;
  return {
    path: abs,
    branch,
    head: wt.head,
    dirtyCount: dirty.length,
    dirtySample: dirty.slice(0, 5).map((d) => d.path),
    behindMain: behind,
    aheadMain: ahead,
    registeredId: registered?.id || null,
    stream: registered?.stream || null,
    unexpectedOwnership: !registered,
    lifecycle: registered?.lifecycle || "unregistered",
    class: classify ? className : className,
    openPr: pr,
    safeRemovalEligibility:
      !dirty.length && ahead === 0 && !registered
        ? "candidate_after_founder_manifest"
        : registered?.safeRemovalEligibility || "not_eligible_without_review",
  };
});

const summary = {
  auditedAt: new Date().toISOString(),
  originMain,
  worktreeCount: rows.length,
  registeredPermanent: (registry.workspaces || []).length,
  classes: rows.reduce((acc, r) => {
    acc[r.class] = (acc[r.class] || 0) + 1;
    return acc;
  }, {}),
  unregisteredCount: rows.filter((r) => r.unexpectedOwnership).length,
  dirtyCount: rows.filter((r) => r.dirtyCount > 0).length,
};

if (json) {
  process.stdout.write(`${JSON.stringify({ summary, rows }, null, 2)}\n`);
} else {
  console.log(`[dealality:worktree-audit] worktrees=${summary.worktreeCount} dirty=${summary.dirtyCount} unregistered=${summary.unregisteredCount}`);
  console.log(`  origin/main=${originMain}`);
  console.log(`  classes=${JSON.stringify(summary.classes)}`);
  for (const r of rows.filter((x) => x.registeredId || x.dirtyCount > 0).slice(0, 40)) {
    console.log(
      `  - ${r.path} | ${r.branch} | dirty=${r.dirtyCount} | ahead=${r.aheadMain} behind=${r.behindMain} | reg=${r.registeredId || "-"} | ${r.class}`,
    );
  }
  if (rows.length > 40) console.log(`  … ${rows.length - 40} more (use --json)`);
}

process.exit(0);
