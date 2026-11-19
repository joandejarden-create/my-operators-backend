#!/usr/bin/env node
/**
 * Regression: cross-workstream checkout, unresolved merge, stash workaround, dirty preserve.
 */
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runGitPreflight } from "../lib/dealality-dev-os/git-preflight.js";
import {
  assertBranchAllowedInWorkspace,
  classifyHomeSyncStatus,
  evaluateWorkspaceOwnership,
  inferBranchStream,
  WORKSPACE_STATUS,
} from "../lib/dealality-dev-os/workspace-ownership-guard-v1.js";
import { loadWorktreeRegistry, normalizePath } from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);
const gdi = registry.workspaces.find((w) => w.stream === "gdi");
const adp = registry.workspaces.find((w) => w.stream === "adp");
const he = registry.workspaces.find((w) => w.stream === "he");
const integration = registry.workspaces.find((w) => w.stream === "integration");

assert.ok(gdi && adp && he && integration);

// Infer stream
assert.equal(inferBranchStream("fix/adp-phase-2b1-durable-evidence-recovery-v1", registry), "adp");
assert.equal(inferBranchStream("feat/gdi-dev-os-workspace-ownership-guard-v1", registry), "gdi");
assert.equal(inferBranchStream("ws/gdi", registry), "gdi");
assert.equal(inferBranchStream("feat/he-intel-001-p3b-m1-readiness", registry), "he");

// GDI must not take ADP branch
{
  const r = assertBranchAllowedInWorkspace({
    workspace: gdi,
    branch: "fix/adp-phase-2b1-durable-evidence-recovery-v1",
    registry,
    op: "switch",
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH);
  assert.equal(r.foreignStream, "adp");
  console.log("PASS GDI refuses ADP feature branch");
}

// ADP must not take GDI branch
{
  const r = assertBranchAllowedInWorkspace({
    workspace: adp,
    branch: "feat/gdi-ai-agent-exa-parity-e1-live-harness-v1",
    registry,
    op: "switch",
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH);
  assert.equal(r.foreignStream, "gdi");
  console.log("PASS ADP refuses GDI feature branch");
}

// HE home ok on HE
{
  const r = assertBranchAllowedInWorkspace({
    workspace: he,
    branch: "ws/hotel-explorer",
    registry,
  });
  assert.equal(r.ok, true);
  console.log("PASS HE home branch allowed");
}

// Integration must not checkout product feature (remote merge only)
{
  const r = evaluateWorkspaceOwnership({
    op: "switch",
    root: normalizePath(integration.path),
    workspace: integration,
    registry,
    targetRef: "feat/gdi-example",
    branch: "feat/gdi-example",
    dirtyCount: 0,
    allowDirtySatisfied: true,
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_REMOTE_MERGE_ONLY);
  console.log("PASS Integration refuses product feature checkout");
}

// Unresolved merge blocks
{
  const r = evaluateWorkspaceOwnership({
    op: "switch",
    root: normalizePath(gdi.path),
    workspace: gdi,
    registry,
    targetRef: "ws/gdi",
    dirtyCount: 0,
    allowDirtySatisfied: true,
    _simulate: { unresolvedGitOp: true, unresolvedGitOpKind: "merge" },
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_UNRESOLVED_GIT_OPERATION);
  console.log("PASS unresolved merge blocks switch");
}

// Stash without founder auth blocked
{
  const r = evaluateWorkspaceOwnership({
    op: "stash",
    root: normalizePath(gdi.path),
    workspace: gdi,
    registry,
    dirtyCount: 2,
    allowDirtySatisfied: true,
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED);
  console.log("PASS stash workaround blocked");
}

// Dirty without allow → preservation required
{
  const r = evaluateWorkspaceOwnership({
    op: "switch",
    root: normalizePath(gdi.path),
    workspace: gdi,
    registry,
    targetRef: "ws/gdi",
    dirtyCount: 3,
    allowDirtySatisfied: false,
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED);
  console.log("PASS dirty switch requires preservation/allow");
}

// Preflight integration: GDI root switching to ADP branch fails
{
  const result = runGitPreflight({
    cwd: ROOT,
    registryRoot: ROOT,
    op: "switch",
    stream: "gdi",
    targetRef: "fix/adp-phase-2b1-durable-evidence-recovery-v1",
    allowDirty: "*",
    _simulate: {
      root: normalizePath(gdi.path),
      registered: gdi,
      branch: "ws/gdi",
      dirty: [],
    },
  });
  assert.equal(result.ok, false, result.errors.join("; "));
  assert.ok(
    result.errors.some((e) => String(e).includes("CROSS_WORKSTREAM")),
    result.errors.join("\n"),
  );
  console.log("PASS preflight blocks GDI→ADP switch");
}

// Failed recovery / preserve without auth
{
  const r = assertBranchAllowedInWorkspace({
    workspace: gdi,
    branch: "preserve/ws-gdi-adp-crossover-orphan-uu-20261119",
    registry,
  });
  assert.equal(r.ok, false);
  assert.equal(r.statusCode, WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED);
  console.log("PASS preserve branch requires founder auth");
}

// Status codes
assert.equal(
  classifyHomeSyncStatus({
    merged: true,
    onHomeBranch: true,
    headEqualsOriginMain: true,
    dirtyCount: 0,
  }),
  WORKSPACE_STATUS.MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE,
);
assert.equal(
  classifyHomeSyncStatus({
    merged: true,
    onHomeBranch: true,
    headEqualsOriginMain: true,
    dirtyCount: 2,
  }),
  WORKSPACE_STATUS.MERGED_AND_HOME_SYNCED,
);
assert.ok(WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH.includes("CROSS_WORKSTREAM"));
console.log("PASS status code classification");

// Cursor metadata note always present
{
  const r = evaluateWorkspaceOwnership({
    op: "status-check",
    root: normalizePath(gdi.path),
    workspace: gdi,
    registry,
    dirtyCount: 0,
    allowDirtySatisfied: true,
  });
  assert.equal(r.cursorSetActiveBranchIsNotAuthorization, true);
  console.log("PASS Cursor SetActiveBranch is not authorization");
}

console.log("\nAll workspace-ownership-guard tests PASSED");
