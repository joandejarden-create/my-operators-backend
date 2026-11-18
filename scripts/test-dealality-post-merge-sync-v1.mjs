#!/usr/bin/env node
/**
 * Deterministic tests for dealality:post-merge-sync (no live GitHub / no destructive ops).
 */
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  collectArgValues,
  pathsAllowed,
  parseAllowDirty,
} from "../lib/dealality-dev-os/git-preflight.js";
import {
  detectActiveTask,
  findOverwriteRisks,
  runPostMergeSync,
} from "../lib/dealality-dev-os/post-merge-sync.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);

function runStreamReady(stream) {
  const ws = registry.workspaces.find((w) => w.stream === stream);
  const result = runPostMergeSync({
    stream,
    pr: 390,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: ws.defaultBranch,
      head: "aaa111",
      originMain: "aaa111",
      dirty: [],
      aheadMain: 0,
      behindMain: 0,
      changedOnMain: [],
      prInfo: {
        state: "MERGED",
        mergeCommit: { oid: "aaa111" },
        headRefName: "feat/example",
        baseRefName: "main",
      },
      preflightFinal: {
        branch: ws.defaultBranch,
        head: "aaa111",
        dirty: [],
        registered: ws,
        behindMain: 0,
        aheadMain: 0,
      },
      finalHead: "aaa111",
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, true, `${stream}: ${result.errors.join("; ")}`);
  assert.equal(result.status, "READY");
  console.log(`PASS READY stream=${stream} home=${ws.defaultBranch}`);
}

// --- allow-dirty prefix + multi-flag parsing ---
assert.equal(
  pathsAllowed(
    [{ path: "tmp/gitleaks-bin/gitleaks.exe" }, { path: "tmp/gitleaks-bin/README.md" }],
    ["tmp/gitleaks-bin"]
  ),
  true,
  "directory prefix without slash covers children"
);
assert.equal(
  pathsAllowed([{ path: "tmp/gitleaks-bin/" }], ["tmp/gitleaks-bin/"]),
  true
);
assert.equal(
  pathsAllowed([{ path: "lib/other.js" }], ["tmp/gitleaks-bin"]),
  false,
  "unrelated dirty remains blocked"
);
assert.deepEqual(
  parseAllowDirty(collectArgValues(
    ["--allow-dirty", "a.json", "--allow-dirty", "tmp/gitleaks-bin/"],
    "--allow-dirty"
  )),
  ["a.json", "tmp/gitleaks-bin/"]
);
console.log("PASS allow-dirty directory prefix + multi-flag");

// --- active task detection ---
assert.equal(
  detectActiveTask({
    branch: "ws/hotel-explorer",
    homeBranch: "ws/hotel-explorer",
    prHeadRef: "feat/x",
    aheadMain: 0,
  }).active,
  false
);
assert.equal(
  detectActiveTask({
    branch: "feat/radar-hpc-dynamic-status-query-v1",
    homeBranch: "ws/hotel-explorer",
    prHeadRef: "feat/radar-hpc-dynamic-status-query-v1",
    aheadMain: 0,
  }).active,
  false,
  "merged PR head eligible"
);
assert.equal(
  detectActiveTask({
    branch: "feat/other-active-task",
    homeBranch: "ws/hotel-explorer",
    prHeadRef: "feat/radar-hpc-dynamic-status-query-v1",
    aheadMain: 2,
  }).active,
  true,
  "separate active task blocked"
);
console.log("PASS active task detection");

assert.deepEqual(
  findOverwriteRisks({
    dirtyPaths: [
      { status: " M", path: "package.json" },
      { status: "??", path: "tmp/probe.json" },
    ],
    changedBetweenTips: ["package.json", "README.md"],
  }),
  ["package.json"]
);
console.log("PASS overwrite risk detection");

// --- HE / ADP / GDI happy paths ---
runStreamReady("he");
runStreamReady("adp");
runStreamReady("gdi");

// --- PR not merged ---
{
  const ws = registry.workspaces.find((w) => w.stream === "he");
  const result = runPostMergeSync({
    stream: "he",
    pr: 390,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: ws.defaultBranch,
      head: "aaa",
      originMain: "aaa",
      dirty: [],
      prInfo: { state: "OPEN", headRefName: "feat/x", baseRefName: "main" },
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, false);
  assert.match(result.errors.join("\n"), /not MERGED/);
  console.log("PASS block unmerged PR");
}

// --- mismatched workspace path ---
{
  const result = runPostMergeSync({
    stream: "adp",
    pr: 390,
    cwd: registry.workspaces.find((w) => w.stream === "he").path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: "ws/adp",
      head: "aaa",
      originMain: "aaa",
      dirty: [],
      prInfo: { state: "MERGED", mergeCommit: { oid: "aaa" }, headRefName: "feat/x", baseRefName: "main" },
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, false);
  assert.match(result.errors.join("\n"), /registered path mismatch/);
  console.log("PASS block mismatched stream path");
}

// --- dirty overwrite risk ---
{
  const ws = registry.workspaces.find((w) => w.stream === "gdi");
  const result = runPostMergeSync({
    stream: "gdi",
    pr: 391,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: ws.defaultBranch,
      head: "old",
      originMain: "new",
      dirty: [{ status: " M", path: "package.json" }],
      changedOnMain: ["package.json"],
      aheadMain: 0,
      behindMain: 1,
      prInfo: {
        state: "MERGED",
        mergeCommit: { oid: "new" },
        headRefName: "fix/gdi-x",
        baseRefName: "main",
      },
      mergeReachable: true,
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, false);
  assert.match(result.errors.join("\n"), /would be overwritten/);
  console.log("PASS block dirty overwrite risk");
}

// --- active separate task ---
{
  const ws = registry.workspaces.find((w) => w.stream === "he");
  const result = runPostMergeSync({
    stream: "he",
    pr: 390,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: "feat/unrelated-active",
      head: "ccc",
      originMain: "aaa",
      dirty: [],
      aheadMain: 3,
      behindMain: 0,
      prInfo: {
        state: "MERGED",
        mergeCommit: { oid: "aaa" },
        headRefName: "feat/radar-hpc-dynamic-status-query-v1",
        baseRefName: "main",
      },
      mergeReachable: true,
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, false);
  assert.match(result.errors.join("\n"), /active task branch/);
  console.log("PASS block separate active task");
}

// --- stale main unreachable merge ---
{
  const ws = registry.workspaces.find((w) => w.stream === "adp");
  const result = runPostMergeSync({
    stream: "adp",
    pr: 390,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: ws.defaultBranch,
      head: "tip",
      originMain: "tip",
      dirty: [],
      prInfo: {
        state: "MERGED",
        mergeCommit: { oid: "oldmerge" },
        headRefName: "feat/x",
        baseRefName: "main",
      },
      mergeReachable: false,
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, false);
  assert.match(result.errors.join("\n"), /not an ancestor/);
  console.log("PASS block unreachable merge on stale main");
}

// --- mismatched branch / wrong home simulation via path ok but refuse destructive ---
{
  const ws = registry.workspaces.find((w) => w.stream === "he");
  const result = runPostMergeSync({
    stream: "he",
    pr: 390,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    reset: true,
    _simulate: {
      skipFetch: true,
      branch: ws.defaultBranch,
      head: "aaa",
      originMain: "aaa",
      dirty: [],
      prInfo: {
        state: "MERGED",
        mergeCommit: { oid: "aaa" },
        headRefName: "feat/x",
        baseRefName: "main",
      },
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, false);
  assert.match(result.errors.join("\n"), /destructive flags/);
  console.log("PASS refuse destructive flags");
}

// --- switch from merged PR head to home (READY) ---
{
  const ws = registry.workspaces.find((w) => w.stream === "he");
  const result = runPostMergeSync({
    stream: "he",
    pr: 390,
    cwd: ws.path,
    registryRoot: ROOT,
    dryRun: true,
    apply: false,
    _simulate: {
      skipFetch: true,
      branch: "feat/radar-hpc-dynamic-status-query-v1",
      head: "prhead",
      originMain: "mergecommit",
      dirty: [{ status: "??", path: "tmp/probe.json" }],
      aheadMain: 0,
      behindMain: 1,
      changedOnMain: ["lib/hotel-intelligence/radar-internal-preview-hotels-v1.js"],
      prInfo: {
        state: "MERGED",
        mergeCommit: { oid: "mergecommit" },
        headRefName: "feat/radar-hpc-dynamic-status-query-v1",
        baseRefName: "main",
      },
      mergeReachable: true,
      preflightSwitch: {
        branch: "feat/radar-hpc-dynamic-status-query-v1",
        head: "prhead",
        dirty: [{ status: "??", path: "tmp/probe.json" }],
        registered: ws,
      },
      preflightMerge: {
        branch: ws.defaultBranch,
        head: "mergecommit",
        dirty: [{ status: "??", path: "tmp/probe.json" }],
        registered: ws,
        behindMain: 0,
        aheadMain: 0,
      },
      preflightFinal: {
        branch: ws.defaultBranch,
        head: "mergecommit",
        dirty: [{ status: "??", path: "tmp/probe.json" }],
        registered: ws,
        behindMain: 0,
        aheadMain: 0,
      },
      finalHead: "mergecommit",
      backup: {
        backupDir: "C:/Backups/Dealality/test-preserve",
        manifestPath: "C:/Backups/Dealality/test-preserve/MANIFEST.json",
        manifest: { entries: [{ path: "tmp/probe.json" }] },
      },
    },
    deps: { gitRunner: () => "", ghRunner: () => "{}" },
  });
  assert.equal(result.ok, true, result.errors.join("; "));
  assert.equal(result.status, "READY");
  assert.ok(result.steps.some((s) => s.step === "switch_home" && s.ok));
  console.log("PASS sync from merged PR head with preserved dirty probe");
}

console.log("\nAll dealality:post-merge-sync scenario tests PASSED");
