#!/usr/bin/env node
/**
 * Unit tests for post-merge product workspace sync classifier + planner (simulated live state).
 */
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyProductWorkspaceSync,
  planPostMergeWorkspaceSync,
  SYNC_ACTIONS,
} from "../lib/dealality-dev-os/post-merge-workspace-sync.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";
import { pathsAllowed, parseAllowDirty } from "../lib/dealality-dev-os/git-preflight.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);

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

const ff = expectAction(
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
assert.equal(ff.safeToMutate, true);

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
  "new branch at main tip with dirty WIP is NOT auto-return (needs explicit merged identity)",
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

assert.equal(
  pathsAllowed([{ path: "tmp/a" }, { path: "tmp/b/c" }], parseAllowDirty("tmp")),
  true,
);
console.log("PASS pathsAllowed directory prefix");

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
const byStream = Object.fromEntries(plan.productPlans.map((p) => [p.stream, p]));
assert.equal(byStream.he.action, SYNC_ACTIONS.RETURN_HOME_THEN_FF);
assert.equal(byStream.adp.action, SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK);
assert.equal(byStream.gdi.action, SYNC_ACTIONS.FAST_FORWARD_HOME);
assert.equal(plan.integrationPlan.action, SYNC_ACTIONS.SKIP_INTEGRATION_DIRTY);
assert.equal(plan.summary.actionableCount, 2);
console.log("PASS plan: affected HE returns home; ADP active fetch-only; GDI FF; dirty primary skipped");

for (const stream of ["he", "adp", "gdi"]) {
  const ws = registry.workspaces.find((w) => w.stream === stream);
  assert.ok(ws?.defaultBranch?.startsWith("ws/"), `${stream} home branch`);
}
console.log("PASS registry: product home branches present");
console.log("PASS: dealality post-merge workspace sync v1");
