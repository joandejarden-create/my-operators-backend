#!/usr/bin/env node
/**
 * Canonical tests for dealality:post-merge-sync (classifier + planner + #394 safeguards).
 * No live GitHub mutations; no product workspace switches.
 */
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  collectArgValues,
  pathsAllowed,
  parseAllowDirty,
  preflightFromArgv,
  runGitPreflight,
} from "../lib/dealality-dev-os/git-preflight.js";
import {
  allowDirtyForPreflight,
  classifyProductWorkspaceSync,
  findOverwriteRisks,
  planPostMergeWorkspaceSync,
  refuseDestructiveFlags,
  resolveMergedPrHead,
  SYNC_ACTIONS,
} from "../lib/dealality-dev-os/post-merge-workspace-sync.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);

{
  const empty = allowDirtyForPreflight([]);
  assert.equal(empty, "");
  const runtimeOnly = allowDirtyForPreflight([
    "data/group-demand-intelligence/hotels/recX/full-e2e-fourteen-segment-state/a.json",
    "reports/gdi-canonical-full-e2e-entry-v1/run/summary.json",
    "tmp/gitleaks-bin/gitleaks.exe",
  ]);
  assert.equal(runtimeOnly, "data,reports,tmp");
  const withSource = allowDirtyForPreflight([
    "data/foo.json",
    "lib/dealality-dev-os/post-merge-workspace-sync.js",
    ".gitignore",
  ]);
  const parts = new Set(withSource.split(","));
  assert.ok(parts.has("data") && parts.has("reports") && parts.has("tmp"));
  assert.ok(parts.has("lib/dealality-dev-os/post-merge-workspace-sync.js"));
  assert.ok(parts.has(".gitignore"));
  assert.equal(parts.has("*"), false);
  console.log("PASS allowDirtyForPreflight runtime prefixes + explicit non-runtime paths");
}

function expectAction(label, input, action) {
  const got = classifyProductWorkspaceSync(input);
  assert.equal(got.action, action, `${label}: expected ${action}, got ${got.action} (${got.reason})`);
  console.log(`PASS classify: ${label}`);
  return got;
}

expectAction(
  "home already synced",
  {
    stream: "he",
    homeBranch: "ws/hotel-explorer",
    branch: "ws/hotel-explorer",
    behindMain: 0,
    dirtyPaths: [],
  },
  SYNC_ACTIONS.ALREADY_SYNCED,
);

expectAction(
  "home behind clean → FF",
  {
    stream: "adp",
    homeBranch: "ws/adp",
    branch: "ws/adp",
    behindMain: 3,
    dirtyPaths: [],
  },
  SYNC_ACTIONS.FAST_FORWARD_HOME,
);

expectAction(
  "home behind dirty no-overlap → FF preserve",
  {
    stream: "he",
    homeBranch: "ws/hotel-explorer",
    branch: "ws/hotel-explorer",
    behindMain: 2,
    dirtyPaths: ["tmp/local-probe.mjs"],
    overlapPaths: [],
  },
  SYNC_ACTIONS.FAST_FORWARD_HOME,
);

expectAction(
  "home behind dirty overlap → blocked",
  {
    stream: "he",
    homeBranch: "ws/hotel-explorer",
    branch: "ws/hotel-explorer",
    behindMain: 2,
    dirtyPaths: ["package.json"],
    overlapPaths: ["package.json"],
  },
  SYNC_ACTIONS.BLOCKED_UPSTREAM_DIRTY_OVERLAP,
);

expectAction(
  "active unmerged task → fetch only",
  {
    stream: "gdi",
    homeBranch: "ws/gdi",
    branch: "feat/gdi-full-e2e-powershell-deep-research-certification-v1",
    behindMain: 5,
    dirtyPaths: ["package.json"],
  },
  SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK,
);

expectAction(
  "new branch at main tip with dirty WIP is NOT auto-return",
  {
    stream: "he",
    homeBranch: "ws/hotel-explorer",
    branch: "chore/dev-os-post-merge-sync-v1",
    behindMain: 0,
    dirtyPaths: ["lib/dealality-dev-os/post-merge-sync.js"],
  },
  SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK,
);

expectAction(
  "merged clean task → return home",
  {
    stream: "adp",
    homeBranch: "ws/adp",
    branch: "fix/adp-done-task-v1",
    behindMain: 0,
    dirtyPaths: [],
    isAffectedStream: true,
    mergedBranch: "fix/adp-done-task-v1",
  },
  SYNC_ACTIONS.RETURN_HOME_THEN_FF,
);

expectAction(
  "merged dirty task → preserve then return",
  {
    stream: "adp",
    homeBranch: "ws/adp",
    branch: "fix/adp-done-task-v1",
    dirtyPaths: ["tmp/notes.md"],
    isAffectedStream: true,
    mergedBranch: "fix/adp-done-task-v1",
  },
  SYNC_ACTIONS.PRESERVE_THEN_RETURN_HOME,
);

expectAction(
  "pr head match → return home",
  {
    stream: "he",
    homeBranch: "ws/hotel-explorer",
    branch: "feat/radar-hpc-dynamic-status-query-v1",
    dirtyPaths: [],
    prHeadRef: "feat/radar-hpc-dynamic-status-query-v1",
    isAffectedStream: true,
  },
  SYNC_ACTIONS.RETURN_HOME_THEN_FF,
);

expectAction(
  "missing path",
  {
    stream: "he",
    homeBranch: "ws/hotel-explorer",
    branch: "ws/hotel-explorer",
    pathExists: false,
  },
  SYNC_ACTIONS.SKIP_MISSING_PATH,
);

// --- #394 safeguards: allow-dirty prefix + repeated flags ---
assert.equal(
  pathsAllowed(
    [{ path: "tmp/gitleaks-bin/gitleaks.exe" }, { path: "tmp/gitleaks-bin/README.md" }],
    ["tmp/gitleaks-bin"],
  ),
  true,
);
assert.equal(pathsAllowed([{ path: "lib/other.js" }], ["tmp/gitleaks-bin"]), false);
assert.deepEqual(
  parseAllowDirty(collectArgValues(["--allow-dirty", "a.json", "--allow-dirty", "tmp/x/"], "--allow-dirty")),
  ["a.json", "tmp/x/"],
);
{
  const argvResult = preflightFromArgv(
    [
      "--op",
      "status-check",
      "--allow-dirty",
      "tmp/gitleaks-bin/",
      "--allow-dirty",
      "reports/probe.json",
    ],
    ROOT,
  );
  assert.ok(argvResult);
  assert.equal(
    pathsAllowed(
      [{ path: "tmp/gitleaks-bin/LICENSE" }, { path: "reports/probe.json" }],
      ["tmp/gitleaks-bin/", "reports/probe.json"],
    ),
    true,
  );
}
console.log("PASS allow-dirty directory prefix + repeated flags");

assert.equal(
  findOverwriteRisks({
    dirtyPaths: [
      { status: " M", path: "package.json" },
      { status: "??", path: "tmp/local.md" },
    ],
    changedBetweenTips: ["package.json", "tmp/local.md"],
  }).join(","),
  "package.json",
  "untracked is not overwrite risk",
);
console.log("PASS overwrite risks ignore untracked");

assert.match(refuseDestructiveFlags({ reset: true }) || "", /destructive/);
assert.equal(refuseDestructiveFlags({}), null);
{
  const blocked = planPostMergeWorkspaceSync({
    registryRoot: ROOT,
    registry,
    mode: "post-merge",
    reset: true,
    fetch: false,
    _skipLiveFetch: true,
  });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.status, "BLOCKED");
  assert.match(blocked.error || "", /destructive/);
}
console.log("PASS refuse destructive flags");

{
  const notMerged = resolveMergedPrHead({
    pr: 1,
    _prInfo: { state: "OPEN", headRefName: "feat/x" },
  });
  assert.equal(notMerged.ok, false);
  const merged = resolveMergedPrHead({
    pr: 390,
    verifyMergeAncestry: false,
    _prInfo: {
      state: "MERGED",
      headRefName: "feat/radar-hpc-dynamic-status-query-v1",
      mergeCommit: { oid: "abc" },
      baseRefName: "main",
    },
  });
  assert.equal(merged.ok, true);
  assert.equal(merged.headRefName, "feat/radar-hpc-dynamic-status-query-v1");
}
console.log("PASS merged PR verification gate");

// Registry ownership still enforced by preflight
{
  const result = runGitPreflight({
    cwd: ROOT,
    registryRoot: ROOT,
    op: "commit",
    allowDirty: "*",
    _simulate: {
      branch: "feat/adp-example",
      registered: { ...registry.workspaces.find((w) => w.stream === "adp"), lifecycle: "permanent" },
      dirty: [{ status: " M", path: "docs/ai-build-system/worktree-registry.json" }],
      commitPaths: ["docs/ai-build-system/worktree-registry.json"],
    },
  });
  assert.equal(result.ok, false);
  assert.ok(result.errors.join("\n").includes("only Integration Controller may commit worktree-registry.json"));
}
console.log("PASS registry ownership fail-closed");

const plan = planPostMergeWorkspaceSync({
  registryRoot: ROOT,
  registry,
  mode: "post-merge",
  affectedStream: "he",
  mergedBranch: "feat/hotel-explorer-merged-v1",
  fetch: false,
  _skipLiveFetch: true,
  _liveByStream: {
    he: {
      exists: true,
      branch: "feat/hotel-explorer-merged-v1",
      head: "aaa",
      dirtyPaths: [],
      dirtyCount: 0,
      behindMain: 0,
      aheadMain: 0,
      overlapPaths: [],
    },
    adp: {
      exists: true,
      branch: "fix/adp-phase-2b1-durable-evidence-recovery-v1",
      head: "bbb",
      dirtyPaths: ["lib/ai-demand-positioning/published-snapshot.js"],
      dirtyCount: 1,
      behindMain: 4,
      aheadMain: 2,
      overlapPaths: [],
    },
    gdi: {
      exists: true,
      branch: "ws/gdi",
      head: "ccc",
      dirtyPaths: [],
      dirtyCount: 0,
      behindMain: 2,
      aheadMain: 0,
      overlapPaths: [],
    },
    integration: {
      exists: true,
      branch: "main",
      head: "ddd",
      dirtyPaths: ["lib/ai-demand-positioning/share/adp-owner-app-admin-inventory-v1.js"],
      dirtyCount: 1,
      behindMain: 6,
      aheadMain: 0,
      overlapPaths: [],
    },
  },
});

assert.equal(plan.dryRun, true);
assert.equal(plan.status, "READY");
const byStream = Object.fromEntries(plan.productPlans.map((p) => [p.stream, p]));
assert.equal(byStream.he.action, SYNC_ACTIONS.RETURN_HOME_THEN_FF);
assert.equal(byStream.adp.action, SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK);
assert.equal(byStream.gdi.action, SYNC_ACTIONS.FAST_FORWARD_HOME);
assert.equal(plan.integrationPlan.action, SYNC_ACTIONS.SKIP_INTEGRATION_DIRTY);
assert.equal(plan.summary.actionableCount, 2);
console.log("PASS plan: HE return-home; ADP fetch-only; GDI FF; dirty primary skipped");

for (const stream of ["he", "adp", "gdi"]) {
  const ws = registry.workspaces.find((w) => w.stream === stream);
  assert.ok(ws?.defaultBranch?.startsWith("ws/"), `${stream} home branch`);
}
console.log("PASS registry: product home branches present");
console.log("PASS: dealality post-merge sync v1 (canonical)");
