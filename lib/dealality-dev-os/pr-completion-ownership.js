/**
 * Dealality Dev OS — universal PR completion + workspace ownership reporting.
 *
 * Reuses the worktree registry, git-preflight dirty collection, and post-merge sync.
 * Does not create a competing workspace-management system.
 *
 * Status/report modes surface BLOCKED_* without auto-resetting or discarding work.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import { collectDirtyPaths } from "./git-preflight.js";
import {
  findWorkspaceByPath,
  findWorkspaceByStream,
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  normalizePath,
} from "./worktree-registry.js";
import {
  PRODUCT_STREAMS,
  classifyCrossWorkstreamBranch,
  detectUnresolvedGitOperation,
  ownershipMutationErrors,
} from "./workspace-ownership.js";

export { PRODUCT_STREAMS, classifyCrossWorkstreamBranch, detectUnresolvedGitOperation, ownershipMutationErrors };

export const PR_COMPLETION_STATUSES = Object.freeze({
  COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE:
    "COMPLETE — MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE",
  COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY:
    "COMPLETE — MERGED_AND_HOME_SYNCED — PRESERVED_DIRTY",
  BLOCKED_PRESERVATION_REQUIRED: "BLOCKED — PRESERVATION_REQUIRED",
  BLOCKED_CROSS_WORKSTREAM_BRANCH: "BLOCKED — CROSS_WORKSTREAM_BRANCH",
  BLOCKED_UNRESOLVED_GIT_OPERATION: "BLOCKED — UNRESOLVED_GIT_OPERATION",
  BLOCKED_NOT_MERGED: "BLOCKED — NOT_MERGED",
  BLOCKED_HOME_NOT_SYNCED: "BLOCKED — HOME_NOT_SYNCED",
  BLOCKED_CI_INCOMPLETE: "BLOCKED — CI_INCOMPLETE",
});

function runGit(root, args) {
  try {
    return execFileSync("git", args, {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  } catch {
    return "";
  }
}

function countDirty(dirty) {
  const tracked = dirty.filter((d) => !String(d.status || "").startsWith("??")).length;
  const untracked = dirty.filter((d) => String(d.status || "").startsWith("??")).length;
  return { tracked, untracked, total: dirty.length };
}

/**
 * Build the mandatory PR completion report (read-only; never mutates Git).
 */
export function buildPrCompletionReport({
  registryRoot,
  stream,
  pr = null,
  mergeSha = null,
  merged = null,
  ciPassed = null,
  cursorActiveBranch = null,
  featureBranchRetired = null,
  peerImpact = null,
  ignoredEvidencePreserved = null,
} = {}) {
  const registry = loadWorktreeRegistry(registryRoot);
  const ws = findWorkspaceByStream(registry, stream);
  if (!ws) {
    return {
      ok: false,
      status: PR_COMPLETION_STATUSES.BLOCKED_PRESERVATION_REQUIRED,
      error: `unknown stream '${stream}'`,
    };
  }

  const root = ws.path;
  const exists = fs.existsSync(root);
  const branch = exists ? runGit(root, ["branch", "--show-current"]) || "(detached)" : null;
  const head = exists ? runGit(root, ["rev-parse", "HEAD"]) : null;
  const originMain = exists ? runGit(root, ["rev-parse", "origin/main"]) : null;
  const home = ws.defaultBranch;
  const homeSha = exists ? runGit(root, ["rev-parse", home]) || null : null;
  const dirty = exists ? collectDirtyPaths(root) : [];
  const counts = countDirty(dirty);
  const unresolved = exists ? detectUnresolvedGitOperation(root) : { active: false };
  const cross = classifyCrossWorkstreamBranch(registry, ws, branch);
  const onHome = branch === home;
  const headSyncedToMain = Boolean(head && originMain && head === originMain);
  const homeSyncedToMain = Boolean(homeSha && originMain && homeSha === originMain);

  let status = PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE;
  const blockers = [];

  if (unresolved.active) {
    status = PR_COMPLETION_STATUSES.BLOCKED_UNRESOLVED_GIT_OPERATION;
    blockers.push(unresolved);
  } else if (cross.crossWorkstream) {
    status = PR_COMPLETION_STATUSES.BLOCKED_CROSS_WORKSTREAM_BRANCH;
    blockers.push(cross);
  } else if (merged === false) {
    status = PR_COMPLETION_STATUSES.BLOCKED_NOT_MERGED;
  } else if (ciPassed === false) {
    status = PR_COMPLETION_STATUSES.BLOCKED_CI_INCOMPLETE;
  } else if (!onHome || !homeSyncedToMain || !headSyncedToMain) {
    status = PR_COMPLETION_STATUSES.BLOCKED_HOME_NOT_SYNCED;
    if (!onHome) blockers.push({ reason: "not_on_home_branch", branch, home });
  } else if (counts.total > 0) {
    status = PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY;
  }

  const complete =
    status === PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE ||
    status === PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY;

  return {
    ok: complete,
    status,
    contract: "DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1",
    pr: pr == null ? null : Number(pr) || pr,
    mergeSha: mergeSha || null,
    owningWorkstream: ws.stream,
    worktreePath: normalizePath(root),
    permanentHomeBranch: home,
    currentBranch: branch,
    head,
    originMain,
    homeSha,
    headSynchronizedWithOriginMain: headSyncedToMain,
    homeSynchronizedWithOriginMain: homeSyncedToMain,
    onHomeBranch: onHome,
    trackedModificationCount: counts.tracked,
    visibleUntrackedCount: counts.untracked,
    dirtyPathsSample: dirty.slice(0, 40).map((d) => `${d.status} ${d.path}`),
    ignoredEvidencePreservationStatus:
      ignoredEvidencePreserved == null
        ? "NOT_ASSESSED — report explicitly when runtime/research evidence was preserved"
        : ignoredEvidencePreserved,
    cursorActiveBranch: cursorActiveBranch || null,
    temporaryFeatureBranchRetirementStatus:
      featureBranchRetired == null ? "NOT_ASSESSED" : featureBranchRetired,
    peerWorktreeImpact: peerImpact || "NOT_ASSESSED",
    unresolvedGitOperation: unresolved,
    crossWorkstream: cross,
    blockers,
    note:
      "Opening a PR, passing CI, or merging into GitHub is not sufficient for completion. Permanent home + explicit working-tree status are mandatory.",
  };
}

export function formatPrCompletionReport(report) {
  const lines = [
    `[dealality:pr-completion] status=${report.status}`,
    `  pr=${report.pr ?? "(n/a)"} mergeSha=${report.mergeSha || "(n/a)"}`,
    `  stream=${report.owningWorkstream} path=${report.worktreePath}`,
    `  home=${report.permanentHomeBranch} current=${report.currentBranch}`,
    `  HEAD=${report.head || "(n/a)"}`,
    `  origin/main=${report.originMain || "(n/a)"}`,
    `  headSync=${report.headSynchronizedWithOriginMain} homeSync=${report.homeSynchronizedWithOriginMain} onHome=${report.onHomeBranch}`,
    `  tracked=${report.trackedModificationCount} untracked=${report.visibleUntrackedCount}`,
    `  ignoredEvidence=${report.ignoredEvidencePreservationStatus}`,
    `  cursorBranch=${report.cursorActiveBranch || "(n/a)"}`,
    `  featureBranchRetirement=${report.temporaryFeatureBranchRetirementStatus}`,
    `  peerImpact=${report.peerWorktreeImpact}`,
  ];
  if (report.crossWorkstream?.crossWorkstream) {
    lines.push(
      `  CROSS_WORKSTREAM foreign=${report.crossWorkstream.matchedForeignStream} branch=${report.currentBranch}`,
    );
  }
  if (report.unresolvedGitOperation?.active) {
    lines.push(`  UNRESOLVED op=${report.unresolvedGitOperation.kind}`);
  }
  lines.push(`  FINAL ${report.status}`);
  return lines.join("\n");
}

/** Snapshot ownership for all permanent workspaces (recovery audit). */
export function auditWorkspaceOwnership(registryRoot) {
  const registry = loadWorktreeRegistry(registryRoot);
  const rows = [];
  for (const ws of listRegisteredWorkspaces(registry).filter((w) => w.lifecycle === "permanent")) {
    const root = ws.path;
    const exists = fs.existsSync(root);
    const branch = exists ? runGit(root, ["branch", "--show-current"]) || "(detached)" : null;
    const unresolved = exists ? detectUnresolvedGitOperation(root) : { active: false };
    const cross = classifyCrossWorkstreamBranch(registry, ws, branch);
    const dirty = exists ? collectDirtyPaths(root) : [];
    let status = "OK";
    if (!exists) status = "MISSING_PATH";
    else if (unresolved.active) status = PR_COMPLETION_STATUSES.BLOCKED_UNRESOLVED_GIT_OPERATION;
    else if (cross.crossWorkstream) status = PR_COMPLETION_STATUSES.BLOCKED_CROSS_WORKSTREAM_BRANCH;
    rows.push({
      id: ws.id,
      stream: ws.stream,
      path: normalizePath(root),
      branch,
      dirtyCount: dirty.length,
      status,
      cross,
      unresolved,
    });
  }
  return { contract: "DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1", rows };
}

export function findWorkspaceByPathSafe(registryRoot, absPath) {
  const registry = loadWorktreeRegistry(registryRoot);
  return findWorkspaceByPath(registry, absPath);
}
