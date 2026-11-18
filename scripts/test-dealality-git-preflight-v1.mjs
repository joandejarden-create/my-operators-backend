#!/usr/bin/env node
/**
 * Isolated failure-scenario tests for dealality:git-preflight.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runGitPreflight } from "../lib/dealality-dev-os/git-preflight.js";
import { loadWorktreeRegistry, normalizePath } from "../lib/dealality-dev-os/worktree-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const registry = loadWorktreeRegistry(ROOT);
const currentBranch = execFileSync("git", ["branch", "--show-current"], {
  cwd: ROOT,
  encoding: "utf8",
}).trim();

function expectFail(label, input, errorFragment) {
  const result = runGitPreflight({ cwd: ROOT, registryRoot: ROOT, ...input });
  assert.equal(result.ok, false, `${label}: expected failure`);
  const blob = result.errors.join("\n");
  assert.ok(
    blob.includes(errorFragment),
    `${label}: expected error containing "${errorFragment}", got:\n${blob}`,
  );
  console.log(`PASS fail-closed: ${label}`);
}

function expectOk(label, input) {
  const result = runGitPreflight({ cwd: ROOT, registryRoot: ROOT, ...input });
  assert.equal(result.ok, true, `${label}: expected ok, errors=${result.errors.join("; ")}`);
  console.log(`PASS allow: ${label}`);
}

// Baseline identity — accept current registered permanent/temporary workspace
expectOk("status-check in registered workspace", {
  op: "status-check",
});

// Wrong branch
expectFail(
  "wrong branch",
  {
    op: "commit",
    expectBranch: "main",
    allowDirty: "*",
  },
  "branch mismatch",
);

// Wrong worktree / stream path (HE registered elsewhere)
expectFail(
  "wrong worktree for HE stream",
  {
    op: "commit",
    stream: "he",
    allowDirty: "*",
  },
  "expected path",
);

// Dirty checkout blocked for merge
expectFail(
  "dirty checkout blocks merge",
  {
    op: "merge",
    targetRef: "origin/main",
    _simulate: {
      dirty: [{ status: " M", path: "lib/example.js" }],
    },
  },
  "dirty worktree blocked",
);

// Commit on protected main
expectFail(
  "commit on protected main",
  {
    op: "commit",
    allowDirty: "*",
    _simulate: { branch: "main" },
  },
  "refusing mutation on local main",
);

// Accidental preservation-branch merge
expectFail(
  "merge while on preservation branch",
  {
    op: "merge",
    targetRef: "origin/main",
    allowDirty: "*",
    _simulate: {
      branch: "feat/hotel-explorer-overview-adp-enrichment-v1",
      dirty: [],
    },
  },
  "protected preservation branch",
);

// Normal merge of origin/main into feature branch must be allowed when clean
expectOk("merge origin/main into feature branch", {
  op: "merge",
  targetRef: "origin/main",
  allowDirty: "*",
  _simulate: {
    branch: "feat/hotel-explorer-example",
    dirty: [],
  },
});

// Force-push request
expectFail(
  "force-push without founder auth",
  {
    op: "force-push",
    force: true,
  },
  "force-push blocked",
);

// Unregistered worktree mutation
expectFail(
  "unregistered worktree",
  {
    op: "commit",
    allowDirty: "*",
    _simulate: {
      root: "C:/Dev/dealality/ws-unregistered-example",
      registered: null,
      branch: "feat/tmp",
      dirty: [{ status: " M", path: "x.js" }],
    },
  },
  "unregistered worktree",
);

// Unregistered worktree-add
expectFail(
  "unregistered worktree-add",
  {
    op: "worktree-add",
    expectPath: "C:/Dev/dealality/ws-unregistered-new",
  },
  "unregistered worktree-add",
);

// Shared-file registry ownership
expectFail(
  "product stream cannot commit registry",
  {
    op: "commit",
    allowDirty: "*",
    _simulate: {
      branch: "feat/adp-example",
      registered: { ...registry.workspaces.find((w) => w.stream === "adp"), lifecycle: "permanent" },
      dirty: [{ status: " M", path: "docs/ai-build-system/worktree-registry.json" }],
      commitPaths: ["docs/ai-build-system/worktree-registry.json"],
    },
  },
  "only Integration Controller may commit worktree-registry.json",
);

// Stale upstream overlap with dirty files
expectFail(
  "stale upstream overlaps dirty files",
  {
    op: "merge",
    targetRef: "origin/main",
    allowDirty: "lib/hotel-intelligence/radar-internal-preview-hotels-v1.js",
    _simulate: {
      branch: "feat/example",
      dirty: [{ status: " M", path: "lib/hotel-intelligence/radar-internal-preview-hotels-v1.js" }],
      behindMain: 3,
      aheadMain: 1,
      changedOnMain: ["lib/hotel-intelligence/radar-internal-preview-hotels-v1.js"],
    },
  },
  "upstream origin/main overlaps locally dirty files",
);

// Allow force-push with founder auth + still needs preservation? force-push is destructive set
expectFail(
  "force-push with auth still needs preservation evidence",
  {
    op: "force-push",
    force: true,
    founderAuth: "FOUNDER_AUTH_FORCE_PUSH",
  },
  "requires --preservation-ref or --backup-manifest",
);

// Positive: commit allowed in current registered integration/temp workspace
expectOk("commit in registered integration wt with allow-dirty *", {
  op: "commit",
  stream: "integration",
  expectBranch: currentBranch,
  allowDirty: "*",
});

console.log("\nAll git-preflight scenario tests PASSED");
console.log(`registryRoot=${ROOT}`);
console.log(`normalized=${normalizePath(ROOT)}`);
