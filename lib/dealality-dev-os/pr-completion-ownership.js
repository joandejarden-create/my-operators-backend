/**
 * Dealality Dev OS — universal PR completion + workspace ownership reporting.
 *
 * COMPLETE is fail-closed: missing/unknown merge, CI, home sync, or preservation
 * evidence cannot yield COMPLETE. Prefer GitHub PR queries over caller booleans.
 *
 * Status/report modes never reset/clean/stash or switch worktrees.
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
  classifyBranchOwnership,
  classifyCrossWorkstreamBranch,
  detectUnresolvedGitOperation,
  getOwnershipEnforcementCoverage,
  ownershipMutationErrors,
} from "./workspace-ownership.js";

export {
  PRODUCT_STREAMS,
  classifyBranchOwnership,
  classifyCrossWorkstreamBranch,
  detectUnresolvedGitOperation,
  getOwnershipEnforcementCoverage,
  ownershipMutationErrors,
};

export const PR_COMPLETION_STATUSES = Object.freeze({
  COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE:
    "COMPLETE — MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE",
  COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY:
    "COMPLETE — MERGED_AND_HOME_SYNCED — PRESERVED_DIRTY",
  BLOCKED_PRESERVATION_REQUIRED: "BLOCKED — PRESERVATION_REQUIRED",
  BLOCKED_CROSS_WORKSTREAM_BRANCH: "BLOCKED — CROSS_WORKSTREAM_BRANCH",
  BLOCKED_UNCLASSIFIED_BRANCH: "BLOCKED — UNCLASSIFIED_BRANCH",
  BLOCKED_UNRESOLVED_GIT_OPERATION: "BLOCKED — UNRESOLVED_GIT_OPERATION",
  BLOCKED_NOT_MERGED: "BLOCKED — NOT_MERGED",
  BLOCKED_HOME_NOT_SYNCED: "BLOCKED — HOME_NOT_SYNCED",
  BLOCKED_CI_INCOMPLETE: "BLOCKED — CI_INCOMPLETE",
  BLOCKED_EVIDENCE_INCOMPLETE: "BLOCKED — EVIDENCE_INCOMPLETE",
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

function isSha(value) {
  return typeof value === "string" && /^[0-9a-f]{7,40}$/i.test(value.trim());
}

function isExplicitOutstanding(value) {
  return typeof value === "string" && /^OUTSTANDING\b/i.test(value.trim());
}

function isVerifiedLabel(value) {
  return typeof value === "string" && value.trim().length > 0 && !/^NOT_ASSESSED\b/i.test(value);
}

/**
 * Query GitHub for PR merge + CI evidence via `gh`.
 * Never trusts caller booleans alone when pr number is available.
 */
export function queryGithubPrEvidence(prNumber, { execFn = execFileSync } = {}) {
  const n = Number(prNumber);
  if (!Number.isFinite(n) || n <= 0) {
    return {
      ok: false,
      source: "github",
      error: "invalid_pr_number",
      merged: null,
      mergeSha: null,
      ciPassed: null,
    };
  }
  try {
    const raw = execFn(
      "gh",
      [
        "pr",
        "view",
        String(n),
        "--json",
        "state,mergedAt,mergeCommit,statusCheckRollup,commits",
      ],
      { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    );
    const data = JSON.parse(raw);
    const merged = data.state === "MERGED" || Boolean(data.mergedAt);
    const mergeSha = data.mergeCommit?.oid || null;
    const checks = Array.isArray(data.statusCheckRollup) ? data.statusCheckRollup : [];
    let ciPassed = null;
    if (!checks.length) {
      ciPassed = merged ? true : null; // merged with empty rollup → treat as no failing required checks observed
      if (!merged) ciPassed = null;
    } else {
      const failing = checks.filter(
        (c) =>
          c.conclusion &&
          !["SUCCESS", "NEUTRAL", "SKIPPED"].includes(String(c.conclusion).toUpperCase()),
      );
      const pending = checks.filter((c) => {
        const st = String(c.status || "").toUpperCase();
        return st && st !== "COMPLETED";
      });
      if (pending.length) ciPassed = null;
      else if (failing.length) ciPassed = false;
      else ciPassed = true;
    }
    return {
      ok: true,
      source: "github",
      merged,
      mergeSha,
      ciPassed,
      state: data.state || null,
      checkCount: checks.length,
    };
  } catch (err) {
    return {
      ok: false,
      source: "github",
      error: String(err?.message || err),
      merged: null,
      mergeSha: null,
      ciPassed: null,
    };
  }
}

/**
 * Resolve merge/CI evidence. Prefer GitHub when --pr is provided.
 * Caller-supplied values alone are insufficient unless githubEvidence is injected
 * (tests) or queryGithub=false with fully explicit verified fields.
 */
export function resolveMergeCiEvidence({
  pr = null,
  mergeSha = null,
  merged = null,
  ciPassed = null,
  queryGithub = true,
  githubEvidence = null,
  execFn,
} = {}) {
  let gh = githubEvidence;
  if (!gh && queryGithub && pr != null && pr !== "") {
    gh = queryGithubPrEvidence(pr, { execFn });
  }

  if (gh) {
    return {
      merged: gh.merged === true ? true : gh.merged === false ? false : null,
      mergeSha: isSha(gh.mergeSha) ? gh.mergeSha : null,
      ciPassed: gh.ciPassed === true ? true : gh.ciPassed === false ? false : null,
      source: gh.source || "github",
      github: gh,
      evidenceComplete: gh.merged === true && isSha(gh.mergeSha) && gh.ciPassed === true,
    };
  }

  // No GitHub: only accept explicit true + SHA + explicit CI true (tests / offline with full proof).
  const explicitMerged = merged === true;
  const explicitSha = isSha(mergeSha);
  const explicitCi = ciPassed === true;
  return {
    merged: merged === true ? true : merged === false ? false : null,
    mergeSha: explicitSha ? mergeSha : null,
    ciPassed: ciPassed === true ? true : ciPassed === false ? false : null,
    source: "caller",
    github: null,
    evidenceComplete: explicitMerged && explicitSha && explicitCi,
  };
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
  queryGithub = true,
  githubEvidence = null,
  execFn,
  // Test-only workspace fact injection (never used to invent merge/CI success).
  _simulateWorkspace = null,
} = {}) {
  const registry = loadWorktreeRegistry(registryRoot);
  const ws = findWorkspaceByStream(registry, stream);
  if (!ws) {
    return {
      ok: false,
      status: PR_COMPLETION_STATUSES.BLOCKED_PRESERVATION_REQUIRED,
      error: `unknown stream '${stream}'`,
      enforcement: getOwnershipEnforcementCoverage(),
    };
  }

  const evidence = resolveMergeCiEvidence({
    pr,
    mergeSha,
    merged,
    ciPassed,
    queryGithub,
    githubEvidence,
    execFn,
  });

  const root = ws.path;
  const sim = _simulateWorkspace || {};
  const exists = sim.exists != null ? sim.exists : fs.existsSync(root);
  const branch =
    sim.branch != null
      ? sim.branch
      : exists
        ? runGit(root, ["branch", "--show-current"]) || "(detached)"
        : null;
  const head = sim.head != null ? sim.head : exists ? runGit(root, ["rev-parse", "HEAD"]) : null;
  const originMain =
    sim.originMain != null ? sim.originMain : exists ? runGit(root, ["rev-parse", "origin/main"]) : null;
  const home = ws.defaultBranch;
  const homeSha =
    sim.homeSha != null ? sim.homeSha : exists ? runGit(root, ["rev-parse", home]) || null : null;
  const dirty = sim.dirty != null ? sim.dirty : exists ? collectDirtyPaths(root) : [];
  const counts = countDirty(dirty);
  const unresolved =
    sim.unresolved != null
      ? sim.unresolved
      : exists
        ? detectUnresolvedGitOperation(root)
        : { active: false };
  const branchClass = classifyBranchOwnership(registry, ws, branch);
  const cross = classifyCrossWorkstreamBranch(registry, ws, branch);
  const onHome = branch === home;
  const headSyncedToMain = Boolean(head && originMain && head === originMain);
  const homeSyncedToMain = Boolean(homeSha && originMain && homeSha === originMain);
  const workingTreeInspected = exists === true || sim.dirty != null;

  const preservation = ignoredEvidencePreserved;
  const preservationAssessed = isVerifiedLabel(preservation);
  const cursorOk =
    isVerifiedLabel(cursorActiveBranch) || isExplicitOutstanding(cursorActiveBranch);
  const retireOk =
    isVerifiedLabel(featureBranchRetired) || isExplicitOutstanding(featureBranchRetired);

  const blockers = [];
  let status = PR_COMPLETION_STATUSES.BLOCKED_EVIDENCE_INCOMPLETE;

  if (unresolved.active) {
    status = PR_COMPLETION_STATUSES.BLOCKED_UNRESOLVED_GIT_OPERATION;
    blockers.push(unresolved);
  } else if (cross.crossWorkstream) {
    status = PR_COMPLETION_STATUSES.BLOCKED_CROSS_WORKSTREAM_BRANCH;
    blockers.push(cross);
  } else if (branchClass.kind === "unclassified" && branch && branch !== "(detached)") {
    status = PR_COMPLETION_STATUSES.BLOCKED_UNCLASSIFIED_BRANCH;
    blockers.push(branchClass);
  } else if (evidence.merged === false) {
    status = PR_COMPLETION_STATUSES.BLOCKED_NOT_MERGED;
  } else if (evidence.ciPassed === false) {
    status = PR_COMPLETION_STATUSES.BLOCKED_CI_INCOMPLETE;
  } else if (
    evidence.merged !== true ||
    !isSha(evidence.mergeSha) ||
    evidence.ciPassed !== true ||
    !workingTreeInspected ||
    !preservationAssessed ||
    !cursorOk ||
    !retireOk
  ) {
    status = PR_COMPLETION_STATUSES.BLOCKED_EVIDENCE_INCOMPLETE;
    if (evidence.merged !== true) blockers.push({ reason: "merged_not_verified", merged: evidence.merged });
    if (!isSha(evidence.mergeSha)) blockers.push({ reason: "merge_sha_missing_or_invalid", mergeSha: evidence.mergeSha });
    if (evidence.ciPassed !== true) blockers.push({ reason: "ci_not_verified", ciPassed: evidence.ciPassed });
    if (!workingTreeInspected) blockers.push({ reason: "working_tree_not_inspected" });
    if (!preservationAssessed) {
      blockers.push({
        reason: "preservation_not_assessed",
        ignoredEvidencePreserved: preservation,
      });
    }
    if (!cursorOk) blockers.push({ reason: "cursor_branch_not_verified_or_outstanding" });
    if (!retireOk) blockers.push({ reason: "feature_branch_retirement_not_verified_or_outstanding" });
  } else if (!onHome || !homeSyncedToMain || !headSyncedToMain) {
    status = PR_COMPLETION_STATUSES.BLOCKED_HOME_NOT_SYNCED;
    if (!onHome) blockers.push({ reason: "not_on_home_branch", branch, home });
    if (!homeSyncedToMain) blockers.push({ reason: "home_not_equal_origin_main" });
    if (!headSyncedToMain) blockers.push({ reason: "head_not_equal_origin_main" });
  } else if (counts.total > 0) {
    status = PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY;
  } else {
    status = PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE;
  }

  const complete =
    status === PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE ||
    status === PR_COMPLETION_STATUSES.COMPLETE_MERGED_AND_HOME_SYNCED_PRESERVED_DIRTY;

  return {
    ok: complete,
    status,
    contract: "DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1",
    pr: pr == null || pr === "" ? null : Number(pr) || pr,
    mergeSha: evidence.mergeSha,
    mergedVerified: evidence.merged === true,
    ciPassedVerified: evidence.ciPassed === true,
    evidenceSource: evidence.source,
    github: evidence.github,
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
    workingTreeInspected,
    trackedModificationCount: counts.tracked,
    visibleUntrackedCount: counts.untracked,
    dirtyPathsSample: dirty.slice(0, 40).map((d) => `${d.status || ""} ${d.path}`.trim()),
    ignoredEvidencePreservationStatus:
      preservation == null || preservation === ""
        ? "NOT_ASSESSED — cannot COMPLETE until explicitly assessed"
        : preservation,
    preservationAssessed,
    cursorActiveBranch: cursorActiveBranch || null,
    temporaryFeatureBranchRetirementStatus: featureBranchRetired || null,
    peerWorktreeImpact: peerImpact || "NOT_ASSESSED",
    unresolvedGitOperation: unresolved,
    crossWorkstream: cross,
    branchClassification: branchClass,
    blockers,
    enforcement: getOwnershipEnforcementCoverage(),
    note:
      "COMPLETE requires verified merged+SHA+CI, home/HEAD==origin/main, inspected worktree, assessed preservation, and cursor/retirement verified or OUTSTANDING. Null/unknown evidence fails closed. Enforcement is PARTIAL — see enforcement.unavoidableBypasses.",
  };
}

export function formatPrCompletionReport(report) {
  const lines = [
    `[dealality:pr-completion] status=${report.status}`,
    `  pr=${report.pr ?? "(n/a)"} mergeSha=${report.mergeSha || "(n/a)"} evidence=${report.evidenceSource || "(n/a)"}`,
    `  mergedVerified=${report.mergedVerified} ciPassedVerified=${report.ciPassedVerified}`,
    `  stream=${report.owningWorkstream} path=${report.worktreePath}`,
    `  home=${report.permanentHomeBranch} current=${report.currentBranch}`,
    `  HEAD=${report.head || "(n/a)"}`,
    `  origin/main=${report.originMain || "(n/a)"}`,
    `  headSync=${report.headSynchronizedWithOriginMain} homeSync=${report.homeSynchronizedWithOriginMain} onHome=${report.onHomeBranch}`,
    `  worktreeInspected=${report.workingTreeInspected} tracked=${report.trackedModificationCount} untracked=${report.visibleUntrackedCount}`,
    `  ignoredEvidence=${report.ignoredEvidencePreservationStatus}`,
    `  preservationAssessed=${report.preservationAssessed}`,
    `  cursorBranch=${report.cursorActiveBranch || "(n/a)"}`,
    `  featureBranchRetirement=${report.temporaryFeatureBranchRetirementStatus || "(n/a)"}`,
    `  peerImpact=${report.peerWorktreeImpact}`,
    `  enforcement=${report.enforcement?.enforcementLevel || "(n/a)"}`,
  ];
  if (report.crossWorkstream?.crossWorkstream) {
    lines.push(
      `  CROSS_WORKSTREAM foreign=${report.crossWorkstream.matchedForeignStream} branch=${report.currentBranch}`,
    );
  }
  if (report.unresolvedGitOperation?.active) {
    lines.push(`  UNRESOLVED op=${report.unresolvedGitOperation.kind}`);
  }
  for (const b of report.blockers || []) {
    if (b?.reason) lines.push(`  blocker=${b.reason}`);
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
    const classification = classifyBranchOwnership(registry, ws, branch);
    const dirty = exists ? collectDirtyPaths(root) : [];
    let status = "OK";
    if (!exists) status = "MISSING_PATH";
    else if (unresolved.active) status = PR_COMPLETION_STATUSES.BLOCKED_UNRESOLVED_GIT_OPERATION;
    else if (cross.crossWorkstream) status = PR_COMPLETION_STATUSES.BLOCKED_CROSS_WORKSTREAM_BRANCH;
    else if (classification.kind === "unclassified" && branch && branch !== "(detached)") {
      status = PR_COMPLETION_STATUSES.BLOCKED_UNCLASSIFIED_BRANCH;
    }
    rows.push({
      id: ws.id,
      stream: ws.stream,
      path: normalizePath(root),
      branch,
      dirtyCount: dirty.length,
      status,
      cross,
      classification,
      unresolved,
      note: "Audit only — does not switch/reset/clean worktrees",
    });
  }
  return {
    contract: "DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1",
    enforcement: getOwnershipEnforcementCoverage(),
    rows,
  };
}

export function findWorkspaceByPathSafe(registryRoot, absPath) {
  const registry = loadWorktreeRegistry(registryRoot);
  return findWorkspaceByPath(registry, absPath);
}
