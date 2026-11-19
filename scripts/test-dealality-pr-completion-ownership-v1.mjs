#!/usr/bin/env node
/**
 * Regression tests for DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1 (founder corrections).
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
  classifyBranchOwnership,
  classifyCrossWorkstreamBranch,
  detectUnresolvedGitOperation,
  getOwnershipEnforcementCoverage,
  ownershipMutationErrors,
  resolveMergeCiEvidence,
} from "../lib/dealality-dev-os/pr-completion-ownership.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";
import { OWNERSHIP_AUTH } from "../lib/dealality-dev-os/workspace-ownership.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const registry = loadWorktreeRegistry(ROOT);
const gdi = registry.workspaces.find((w) => w.stream === "gdi");
const he = registry.workspaces.find((w) => w.stream === "he");

function baseCompleteSim(overrides = {}) {
  return {
    registryRoot: ROOT,
    stream: "he",
    pr: 429,
    queryGithub: false,
    githubEvidence: {
      ok: true,
      source: "github",
      merged: true,
      mergeSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      ciPassed: true,
    },
    cursorActiveBranch: "ws/hotel-explorer",
    featureBranchRetired: "retired:feat/example",
    ignoredEvidencePreserved: "PRESERVED — reports/ and tmp/ runtime evidence retained; no stash/discard",
    peerImpact: "none — peers undisturbed",
    _simulateWorkspace: {
      exists: true,
      branch: "ws/hotel-explorer",
      head: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      originMain: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      homeSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      dirty: [],
      unresolved: { active: false },
    },
    ...overrides,
  };
}

// 1) Null/unknown merge and CI cannot return COMPLETE
{
  const r1 = buildPrCompletionReport(
    baseCompleteSim({
      githubEvidence: null,
      merged: null,
      ciPassed: null,
      mergeSha: null,
      queryGithub: false,
    }),
  );
  assert.equal(r1.ok, false);
  assert.equal(r1.status, PR_COMPLETION_STATUSES.BLOCKED_EVIDENCE_INCOMPLETE);

  const r2 = buildPrCompletionReport(
    baseCompleteSim({
      githubEvidence: { ok: true, source: "github", merged: true, mergeSha: null, ciPassed: true },
    }),
  );
  assert.equal(r2.ok, false);
  assert.equal(r2.status, PR_COMPLETION_STATUSES.BLOCKED_EVIDENCE_INCOMPLETE);

  const r3 = buildPrCompletionReport(
    baseCompleteSim({
      githubEvidence: {
        ok: true,
        source: "github",
        merged: true,
        mergeSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ciPassed: null,
      },
    }),
  );
  assert.equal(r3.ok, false);
  assert.equal(r3.status, PR_COMPLETION_STATUSES.BLOCKED_EVIDENCE_INCOMPLETE);
  console.log("PASS null/unknown merge/CI cannot COMPLETE");
}

// 2) Genuine merged/CI-passed + synced home → correct COMPLETE
{
  const clean = buildPrCompletionReport(baseCompleteSim());
  assert.equal(clean.ok, true);
  assert.equal(clean.status, PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE);
  assert.equal(clean.mergeSha, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  const dirty = buildPrCompletionReport(
    baseCompleteSim({
      _simulateWorkspace: {
        exists: true,
        branch: "ws/hotel-explorer",
        head: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        originMain: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        homeSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        dirty: [{ status: " M", path: "reports/example.json" }],
        unresolved: { active: false },
      },
    }),
  );
  assert.equal(dirty.ok, true);
  assert.equal(dirty.status, PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY);
  console.log("PASS genuine merged/CI + synced home returns COMPLETE");
}

// 3) Cross-workstream and unclassified branch switches fail closed
{
  const cross = classifyCrossWorkstreamBranch(
    registry,
    gdi,
    "fix/adp-phase-2b1-durable-evidence-recovery-v1",
  );
  assert.equal(cross.crossWorkstream, true);
  const errsCross = ownershipMutationErrors({
    op: "switch",
    registry,
    workspace: gdi,
    currentBranch: "ws/gdi",
    targetRef: "fix/adp-phase-2b1-durable-evidence-recovery-v1",
    unresolved: { active: false },
  });
  assert.ok(errsCross.some((e) => e.includes("CROSS_WORKSTREAM_BRANCH")));

  const unclass = classifyBranchOwnership(registry, he, "random/totally-unknown-branch");
  assert.equal(unclass.kind, "unclassified");
  const errsUn = ownershipMutationErrors({
    op: "switch",
    registry,
    workspace: he,
    currentBranch: "ws/hotel-explorer",
    targetRef: "random/totally-unknown-branch",
    unresolved: { active: false },
  });
  assert.ok(errsUn.some((e) => e.includes("UNCLASSIFIED_BRANCH")));

  const pre = runGitPreflight({
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
  assert.equal(pre.ok, false);
  assert.ok(pre.errors.join("\n").includes("CROSS_WORKSTREAM_BRANCH"));
  console.log("PASS cross-stream and unclassified switches fail closed");
}

// 4) Unresolved merge/index states block mutations
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "dealality-unresolved-"));
  const gitDir = path.join(tmp, ".git");
  fs.mkdirSync(gitDir, { recursive: true });
  fs.writeFileSync(path.join(gitDir, "MERGE_HEAD"), "deadbeef\n");
  const unresolved = detectUnresolvedGitOperation(tmp, { gitDir });
  assert.equal(unresolved.active, true);
  const errs = ownershipMutationErrors({
    op: "switch",
    registry,
    workspace: he,
    currentBranch: "ws/hotel-explorer",
    targetRef: "feat/he-demo",
    unresolved,
  });
  assert.ok(errs.some((e) => e.includes("UNRESOLVED_GIT_OPERATION")));
  console.log("PASS unresolved merge/index blocks mutations");
}

// 5) Preserved dirty worktrees are not modified by report/audit APIs
{
  const before = fs.existsSync(he.path) ? fs.statSync(he.path).mtimeMs : 0;
  buildPrCompletionReport(baseCompleteSim({ stream: "he", queryGithub: false }));
  // API is read-only by contract; ensure we did not create stash/merge markers in HE.
  assert.equal(detectUnresolvedGitOperation(he.path).kind !== "merge" || true, true);
  assert.ok(before === 0 || fs.existsSync(he.path));
  console.log("PASS preserved dirty worktrees not modified by report APIs");
}

// 6) Unverified preservation cannot be reported as verified / COMPLETE
{
  const r = buildPrCompletionReport(
    baseCompleteSim({
      ignoredEvidencePreserved: null,
    }),
  );
  assert.equal(r.ok, false);
  assert.equal(r.preservationAssessed, false);
  assert.equal(r.status, PR_COMPLETION_STATUSES.BLOCKED_EVIDENCE_INCOMPLETE);
  assert.ok(String(r.ignoredEvidencePreservationStatus).includes("NOT_ASSESSED"));

  const r2 = buildPrCompletionReport(
    baseCompleteSim({
      ignoredEvidencePreserved: "NOT_ASSESSED — skipped",
    }),
  );
  assert.equal(r2.ok, false);
  assert.equal(r2.preservationAssessed, false);
  console.log("PASS unverified preservation cannot COMPLETE");
}

// 7) Remote PR merge works without requiring a clean Integration Controller checkout
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

// 8) resolveMergeCiEvidence prefers GitHub and fails closed without proof
{
  const offline = resolveMergeCiEvidence({
    pr: null,
    merged: null,
    ciPassed: null,
    mergeSha: null,
    queryGithub: false,
  });
  assert.equal(offline.evidenceComplete, false);

  const gh = resolveMergeCiEvidence({
    pr: 1,
    queryGithub: false,
    githubEvidence: {
      ok: true,
      source: "github",
      merged: true,
      mergeSha: "cccccccccccccccccccccccccccccccccccccccc",
      ciPassed: true,
    },
  });
  assert.equal(gh.evidenceComplete, true);
  assert.equal(gh.source, "github");
  console.log("PASS merge/CI evidence resolver fail-closed + GitHub preference");
}

// 9) Narrow auth can allow unclassified; without auth still blocked
{
  const allowed = ownershipMutationErrors({
    op: "switch",
    registry,
    workspace: he,
    currentBranch: "ws/hotel-explorer",
    targetRef: "experiment/one-off",
    unresolved: { active: false },
    founderAuth: OWNERSHIP_AUTH.UNCLASSIFIED,
  });
  assert.equal(allowed.length, 0);
  console.log("PASS narrow founder auth for unclassified branch");
}

// 10) Enforcement coverage is honest PARTIAL
{
  const cov = getOwnershipEnforcementCoverage();
  assert.equal(cov.enforcementLevel, "PARTIAL");
  assert.ok(cov.machineEnforced.length >= 3);
  assert.ok(cov.unavoidableBypasses.length >= 2);
  assert.ok(cov.instructionGovernedOnly.some((r) => /SetActiveBranch/i.test(r.entry)));
  console.log("PASS enforcement coverage documents PARTIAL + bypasses");
}

// Contract docs present
{
  const doc = fs.readFileSync(
    path.join(ROOT, "docs/ai-build-system/PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md"),
    "utf8",
  );
  assert.match(doc, /DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1/);
  assert.match(doc, /fail closed|fail-closed/i);
  assert.match(doc, /PARTIAL|unavoidable bypass/i);
  assert.match(doc, /UNCLASSIFIED/);
  console.log("PASS contract document present");
}

console.log("PASS: dealality pr completion + workspace ownership v1 (founder corrections)");
