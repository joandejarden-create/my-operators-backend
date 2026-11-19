#!/usr/bin/env node
/**
 * Regression tests for DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1.
 * Pure simulations — does not mutate product workspaces.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runGitPreflight } from "../lib/dealality-dev-os/git-preflight.js";
import {
  PR_COMPLETION_STATUSES,
  buildPrCompletionReport,
  classifyCrossWorkstreamBranch,
  detectUnresolvedGitOperation,
  ownershipMutationErrors,
} from "../lib/dealality-dev-os/pr-completion-ownership.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const registry = loadWorktreeRegistry(ROOT);
const gdi = registry.workspaces.find((w) => w.stream === "gdi");
const adp = registry.workspaces.find((w) => w.stream === "adp");
const he = registry.workspaces.find((w) => w.stream === "he");

// 1) Cross-workstream branch checkout attempts
{
  const cross = classifyCrossWorkstreamBranch(
    registry,
    gdi,
    "fix/adp-phase-2b1-durable-evidence-recovery-v1",
  );
  assert.equal(cross.crossWorkstream, true);
  assert.equal(cross.matchedForeignStream, "adp");
  const errs = ownershipMutationErrors({
    op: "switch",
    registry,
    workspace: gdi,
    currentBranch: "ws/gdi",
    targetRef: "fix/adp-phase-2b1-durable-evidence-recovery-v1",
    unresolved: { active: false },
  });
  assert.ok(errs.some((e) => e.includes("CROSS_WORKSTREAM_BRANCH")));
  console.log("PASS cross-workstream branch checkout blocked");
}

// 2) Dirty worktree preservation — completion reports PRESERVED_DIRTY, never invents clean
{
  const report = buildPrCompletionReport({
    registryRoot: ROOT,
    stream: "he",
    merged: true,
    ciPassed: true,
  });
  // Live HE may be on a feature branch → not COMPLETE. Status must never claim CLEAN when dirty>0 on home.
  if (report.onHomeBranch && report.homeSynchronizedWithOriginMain && report.trackedModificationCount + report.visibleUntrackedCount > 0) {
    assert.equal(report.status, PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY);
  }
  assert.ok(!String(report.status).includes("CLEAN_WORKTREE") || report.trackedModificationCount + report.visibleUntrackedCount === 0);
  console.log("PASS dirty worktree preservation reporting");
}

// 3) Unresolved merge/rebase detection
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "dealality-unresolved-"));
  const gitDir = path.join(tmp, ".git");
  fs.mkdirSync(gitDir, { recursive: true });
  fs.writeFileSync(path.join(gitDir, "MERGE_HEAD"), "deadbeef\n");
  const unresolved = detectUnresolvedGitOperation(tmp, { gitDir });
  assert.equal(unresolved.active, true);
  assert.equal(unresolved.kind, "merge");
  const errs = ownershipMutationErrors({
    op: "switch",
    registry,
    workspace: he,
    currentBranch: "ws/hotel-explorer",
    targetRef: "feat/he-demo",
    unresolved,
  });
  assert.ok(errs.some((e) => e.includes("UNRESOLVED_GIT_OPERATION")));
  console.log("PASS unresolved merge/rebase detection");
}

// 4) Remote PR merge with dirty Integration Controller checkout — preflight does not require clean primary for status
{
  const primary = "C:/Dev/dealality/my-operators-backend";
  const result = runGitPreflight({
    cwd: primary,
    registryRoot: ROOT,
    op: "status-check",
    stream: "integration",
    allowDirty: "*",
    _simulate: {
      root: primary,
      registered: registry.workspaces.find((w) => w.stream === "integration"),
      branch: "main",
      dirty: [{ status: " M", path: "reports/example.json" }],
      unresolvedGitOperation: { active: false },
    },
  });
  assert.equal(result.ok, true, result.errors.join("; "));
  console.log("PASS remote-merge-safe status-check with dirty Integration checkout");
}

// 5) Safe post-merge home sync classification remains non-destructive (cross-stream refuse)
{
  const result = runGitPreflight({
    cwd: ROOT,
    registryRoot: ROOT,
    op: "switch",
    stream: "gdi",
    targetRef: "fix/adp-something",
    allowDirty: "*",
    _simulate: {
      root: gdi.path,
      registered: gdi,
      branch: "ws/gdi",
      dirty: [],
      unresolvedGitOperation: { active: false },
    },
  });
  assert.equal(result.ok, false);
  assert.ok(result.errors.join("\n").includes("CROSS_WORKSTREAM_BRANCH"));
  console.log("PASS safe post-merge: refuse cross-stream switch");
}

// 6) Accurate clean-versus-preserved-dirty status strings
{
  assert.match(PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE, /CLEAN_WORKTREE/);
  assert.match(PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY, /PRESERVED_DIRTY/);
  assert.match(PR_COMPLETION_STATUSES.BLOCKED_PRESERVATION_REQUIRED, /PRESERVATION_REQUIRED/);
  assert.match(PR_COMPLETION_STATUSES.BLOCKED_CROSS_WORKSTREAM_BRANCH, /CROSS_WORKSTREAM_BRANCH/);
  assert.match(PR_COMPLETION_STATUSES.BLOCKED_UNRESOLVED_GIT_OPERATION, /UNRESOLVED_GIT_OPERATION/);
  console.log("PASS clean-versus-preserved-dirty status vocabulary");
}

// Contract docs present
{
  const doc = fs.readFileSync(
    path.join(ROOT, "docs/ai-build-system/PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md"),
    "utf8",
  );
  assert.match(doc, /DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1/);
  assert.match(doc, /MERGED_AND_HOME_SYNCED/);
  assert.match(doc, /CROSS_WORKSTREAM_BRANCH/);
  console.log("PASS contract document present");
}

console.log("PASS: dealality pr completion + workspace ownership v1");
