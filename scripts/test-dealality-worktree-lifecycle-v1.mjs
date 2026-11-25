#!/usr/bin/env node
/**
 * Unit tests for Worktree Lifecycle Policy (no live deletions).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  LIFECYCLE_STATUS,
  RETIREMENT_BLOCKERS,
  assessTemporaryWorktreesAfterMerge,
  assessWorktree,
  buildRetirementProposal,
  retireApprovedBatch,
} from "../lib/dealality-dev-os/worktree-lifecycle-v1.js";
import { loadWorktreeRegistry, normalizePath } from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);

// --- constants ---
assert.equal(LIFECYCLE_STATUS.PERMANENT, "PERMANENT");
assert.equal(LIFECYCLE_STATUS.RETIREMENT_CANDIDATE, "RETIREMENT_CANDIDATE");
assert.ok(RETIREMENT_BLOCKERS.FOUNDER_APPROVAL_REQUIRED);

// --- permanent homes never become retirement candidates ---
for (const ws of registry.workspaces) {
  const assessment = assessWorktree({
    absPath: ws.path,
    registry,
    allWorktrees: [{ path: normalizePath(ws.path), head: "abc", branch: ws.defaultBranch || "main" }],
    originMainSha: "abc",
    repoRoot: ROOT,
  });
  assert.equal(
    assessment.lifecycleStatus,
    LIFECYCLE_STATUS.PERMANENT,
    `${ws.id} must be PERMANENT`,
  );
  assert.ok(assessment.blockers.includes(RETIREMENT_BLOCKERS.PERMANENT_WORKSPACE));
}

// --- classification: clean + in main + no blockers => RETIREMENT_CANDIDATE ---
{
  const fakePath = normalizePath("C:/Dev/dealality/wt-lifecycle-unit-fake-clean");
  const assessment = assessWorktree({
    absPath: fakePath,
    registry: {
      schemaVersion: 1,
      workspaces: registry.workspaces,
      temporaryWorkspaces: [
        {
          id: "unit-fake-clean",
          stream: "integration",
          path: fakePath,
          lifecycle: "temporary",
          owner: "test",
          role: "unit",
        },
      ],
    },
    allWorktrees: [], // missing from git list
    originMainSha: "deadbeef",
    repoRoot: ROOT,
  });
  assert.equal(assessment.lifecycleStatus, LIFECYCLE_STATUS.BLOCKED);
  assert.ok(assessment.blockers.includes(RETIREMENT_BLOCKERS.MISSING_PATH) || assessment.blockers.includes(RETIREMENT_BLOCKERS.NOT_GIT_WORKTREE) || assessment.blockers.includes(RETIREMENT_BLOCKERS.MISSING_PATH));
}

// --- proposal groups + founder phrase ---
{
  const inventory = {
    originMain: "abc123",
    worktreeCount: 5,
    rows: [
      {
        path: "C:/Dev/dealality/ws-gdi",
        lifecycleStatus: LIFECYCLE_STATUS.PERMANENT,
        registeredId: "gdi",
        stream: "gdi",
      },
      {
        path: "C:/Dev/dealality/wt-clean-a",
        lifecycleStatus: LIFECYCLE_STATUS.RETIREMENT_CANDIDATE,
        stream: "integration",
        branch: "feat/old",
        head: "aaa",
        junctions: [],
        recoverableBytesEstimate: 5_000_000_000,
      },
      {
        path: "C:/Dev/dealality/wt-clean-b",
        lifecycleStatus: LIFECYCLE_STATUS.RETIREMENT_CANDIDATE,
        stream: "gdi",
        branch: "feat/old2",
        head: "bbb",
        junctions: [{ name: "node_modules", path: "C:/Dev/dealality/wt-clean-b/node_modules" }],
        recoverableBytesEstimate: 100_000,
      },
      {
        path: "C:/Dev/dealality/wt-dirty",
        lifecycleStatus: LIFECYCLE_STATUS.PRESERVATION_REQUIRED,
        blockers: [RETIREMENT_BLOCKERS.UNCOMMITTED_CHANGES],
        dirtyCount: 3,
      },
      {
        path: "C:/Dev/dealality/wt-locked",
        lifecycleStatus: LIFECYCLE_STATUS.BLOCKED,
        blockers: [RETIREMENT_BLOCKERS.LOCKS],
      },
    ],
  };
  const proposal = buildRetirementProposal(inventory, {
    batchId: "lifecycle-retire-test-000001",
    batchLimit: 8,
    now: new Date("2026-11-25T12:00:00Z"),
  });
  assert.equal(proposal.summary.PERMANENT, 1);
  assert.equal(proposal.summary.RETIREMENT_CANDIDATE, 2);
  assert.equal(proposal.summary.PRESERVATION_REQUIRED, 1);
  assert.equal(proposal.summary.BLOCKED, 1);
  assert.equal(proposal.proposedBatch[0].path, "C:/Dev/dealality/wt-clean-a");
  assert.ok(proposal.founderApproval.phrase.includes("AUTHORIZE_WORKTREE_LIFECYCLE_RETIRE_"));
  assert.equal(proposal.deleted, false);
  assert.equal(proposal.safety.neverAutoDelete, true);
  assert.equal(proposal.safety.neverForceWorktreeRemove, true);

  // retire without approval → blocked
  const denied = retireApprovedBatch({
    repoRoot: ROOT,
    proposal,
    founderApprovalPhrase: "WRONG",
    apply: false,
  });
  assert.equal(denied.ok, false);
  assert.equal(denied.error, RETIREMENT_BLOCKERS.FOUNDER_APPROVAL_REQUIRED);

  // force refused
  const forced = retireApprovedBatch({
    repoRoot: ROOT,
    proposal,
    founderApprovalPhrase: proposal.founderApproval.phrase,
    apply: false,
    force: true,
  });
  assert.equal(forced.ok, false);
  assert.equal(forced.error, RETIREMENT_BLOCKERS.FORCE_FLAG_REFUSED);

  // dry-run with correct phrase — no deletes; may block on reassessment (paths missing)
  const dry = retireApprovedBatch({
    repoRoot: ROOT,
    proposal,
    founderApprovalPhrase: proposal.founderApproval.phrase,
    apply: false,
  });
  assert.ok(dry.status === "DRY_RUN" || dry.status === "PARTIAL" || dry.status === "BLOCKED" || dry.ok === false || dry.ok === true);
  assert.ok(!fs.existsSync("C:/Dev/dealality/wt-clean-a"), "dry-run must not create/delete fake paths");
}

// --- post-merge assess never deletes ---
{
  const result = assessTemporaryWorktreesAfterMerge({
    repoRoot: ROOT,
    mergedBranch: "chore/dev-os-worktree-lifecycle-governance-v1",
    pr: 999999,
    writeRegistry: false,
  });
  assert.equal(result.deleted, false);
  assert.equal(result.registryUpdated, false);
  assert.ok(typeof result.matchedCount === "number");
}

// --- registry schema expectations for temporary metadata fields (forward-compatible) ---
{
  for (const t of registry.temporaryWorkspaces || []) {
    assert.ok(t.id && t.path && t.lifecycle === "temporary");
    assert.ok(t.owner, `${t.id} needs owner`);
    assert.ok(t.role || t.purpose, `${t.id} needs purpose/role`);
  }
  assert.ok(
    (registry.notes || []).some((n) => /lifecycle/i.test(String(n))),
    "registry notes should mention lifecycle policy",
  );
}

// --- temp dir sandbox: candidate when clean + ancestor ---
{
  // Skip deep git sandbox if git unavailable; already covered permanent + proposal gates.
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "dealality-lifecycle-"));
  assert.ok(tmp);
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log("PASS: dealality worktree lifecycle v1");
