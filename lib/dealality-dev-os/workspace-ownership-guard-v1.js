/**
 * Dealality permanent workspace ownership guard.
 *
 * Authoritative intent: docs/ai-build-system/worktree-registry.json
 * Enforcement: dealality:git-preflight + optional .githooks (not Cursor SetActiveBranch).
 *
 * Cursor active-branch metadata is UI-only and MUST NOT be treated as Git authorization.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  findWorkspaceByPath,
  findWorkspaceByStream,
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  matchesBranchPattern,
  normalizePath,
} from "./worktree-registry.js";

export const OWNERSHIP_GUARD_VERSION = "dealality-workspace-ownership-guard-v1";

/** Canonical completion / blocker codes for agents and post-merge reporting. */
export const WORKSPACE_STATUS = Object.freeze({
  MERGED_AND_HOME_SYNCED: "MERGED_AND_HOME_SYNCED",
  MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE: "MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE",
  BLOCKED_PRESERVATION_REQUIRED: "BLOCKED — PRESERVATION_REQUIRED",
  BLOCKED_CROSS_WORKSTREAM_BRANCH: "BLOCKED — CROSS_WORKSTREAM_BRANCH",
  BLOCKED_UNRESOLVED_GIT_OPERATION: "BLOCKED — UNRESOLVED_GIT_OPERATION",
  BLOCKED_DIRTY_WORKTREE: "BLOCKED — DIRTY_WORKTREE",
  BLOCKED_REMOTE_MERGE_ONLY: "BLOCKED — USE_REMOTE_PR_MERGE",
  OK: "OK",
});

const PRODUCT_STREAMS = new Set(["he", "adp", "gdi"]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function branchName(ref) {
  return text(ref).replace(/^refs\/heads\//, "").replace(/^origin\//, "");
}

/**
 * Detect in-progress merge / rebase / cherry-pick / revert / orphan UU stages.
 * Works for linked worktrees (.git/worktrees/<name>).
 */
export function detectUnresolvedGitOperationSync(gitRoot, sim = null) {
  if (sim?.unresolvedGitOp != null) {
    const unresolved = Boolean(sim.unresolvedGitOp);
    return {
      unresolved,
      operations: unresolved
        ? [{ kind: String(sim.unresolvedGitOpKind || "merge"), path: "(simulated)" }]
        : [],
      gitDir: "(simulated)",
      statusCode: unresolved
        ? WORKSPACE_STATUS.BLOCKED_UNRESOLVED_GIT_OPERATION
        : WORKSPACE_STATUS.OK,
    };
  }
  const root = path.resolve(gitRoot);
  let gd;
  try {
    gd = execFileSync("git", ["rev-parse", "--git-dir"], {
      cwd: root,
      encoding: "utf8",
    }).trim();
    if (!path.isAbsolute(gd)) gd = path.resolve(root, gd);
  } catch {
    gd = path.join(root, ".git");
  }
  const found = [];
  for (const [name, kind] of [
    ["MERGE_HEAD", "merge"],
    ["CHERRY_PICK_HEAD", "cherry-pick"],
    ["REVERT_HEAD", "revert"],
    ["REBASE_HEAD", "rebase"],
    ["rebase-merge", "rebase"],
    ["rebase-apply", "rebase"],
  ]) {
    if (fs.existsSync(path.join(gd, name))) {
      found.push({ kind, path: path.join(gd, name), name });
    }
  }
  try {
    const u = execFileSync("git", ["ls-files", "-u"], {
      cwd: root,
      encoding: "utf8",
    }).trim();
    if (u && !found.some((f) => f.kind === "merge")) {
      found.push({ kind: "orphan-unmerged-index", path: "(index)", name: "ls-files -u" });
    }
  } catch {
    /* ignore */
  }
  return {
    unresolved: found.length > 0,
    operations: found,
    gitDir: gd,
    statusCode: found.length
      ? WORKSPACE_STATUS.BLOCKED_UNRESOLVED_GIT_OPERATION
      : WORKSPACE_STATUS.OK,
  };
}

/** @deprecated use detectUnresolvedGitOperationSync */
export function detectUnresolvedGitOperation(gitRoot, opts = {}) {
  return detectUnresolvedGitOperationSync(gitRoot, opts._simulate || null);
}

/**
 * Which permanent product/integration stream "owns" a branch name, if any.
 */
export function inferBranchStream(branch, registry) {
  const b = branchName(branch);
  if (!b) return null;
  if (b === "main" || b === "origin/main") return "integration";
  const permanent = (registry.workspaces || []).filter((w) => w.lifecycle === "permanent");
  const hits = [];
  for (const ws of permanent) {
    if (b === ws.defaultBranch || matchesBranchPattern(ws, b)) {
      hits.push(ws.stream);
    }
  }
  if (hits.length === 1) return hits[0];
  if (hits.length > 1) {
    const product = hits.filter((s) => PRODUCT_STREAMS.has(s));
    if (product.length === 1) return product[0];
    return hits[0];
  }
  if (/^preserve\//.test(b) || /^refs\/backup\//.test(b)) return "preserve";
  return null;
}

/**
 * Assert a target branch is allowed in a registered workspace.
 */
export function assertBranchAllowedInWorkspace({
  workspace,
  branch,
  registry,
  founderAuth = "",
  op = "switch",
} = {}) {
  const b = branchName(branch);
  const stream = workspace?.stream || null;
  if (!workspace || !stream) {
    return {
      ok: false,
      statusCode: WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH,
      reason: "workspace_not_registered",
      branch: b,
    };
  }
  if (!b) {
    return {
      ok: false,
      statusCode: WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH,
      reason: "missing_branch",
    };
  }

  if (stream !== "integration" && (b === "main" || b === "origin/main")) {
    if (founderAuth !== "FOUNDER_AUTH_PRODUCT_MAIN_CHECKOUT") {
      return {
        ok: false,
        statusCode: WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH,
        reason: "product_workspace_may_not_checkout_main",
        branch: b,
        stream,
      };
    }
  }

  if (matchesBranchPattern(workspace, b) || b === workspace.defaultBranch) {
    return { ok: true, statusCode: WORKSPACE_STATUS.OK, branch: b, stream };
  }

  if (/^preserve\//.test(b)) {
    if (founderAuth === "FOUNDER_AUTH_PRESERVE_BRANCH_CHECKOUT") {
      return { ok: true, statusCode: WORKSPACE_STATUS.OK, branch: b, stream, preserve: true };
    }
    return {
      ok: false,
      statusCode: WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED,
      reason: "preserve_branch_requires_founder_auth",
      branch: b,
      stream,
    };
  }

  const inferred = inferBranchStream(b, registry);
  if (inferred && inferred !== stream && inferred !== "preserve") {
    return {
      ok: false,
      statusCode: WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH,
      reason: `branch '${b}' belongs to stream '${inferred}', not '${stream}' (workspace ${workspace.id} at ${workspace.path})`,
      branch: b,
      stream,
      foreignStream: inferred,
      op,
    };
  }

  return {
    ok: false,
    statusCode: WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH,
    reason: `branch '${b}' does not match ${stream} pattern ${workspace.expectedBranchPattern}`,
    branch: b,
    stream,
    op,
  };
}

/**
 * High-level ownership evaluation for preflight.
 */
export function evaluateWorkspaceOwnership(input = {}) {
  const registry = input.registry || loadWorktreeRegistry(input.registryRoot);
  const rootNorm = normalizePath(input.root || input.cwd);
  const workspace =
    input.workspace ||
    findWorkspaceByPath(registry, rootNorm) ||
    (input.stream ? findWorkspaceByStream(registry, input.stream) : null);
  const branch = branchName(input.branch || input.targetRef || "");
  const unresolved = detectUnresolvedGitOperationSync(
    input.root || input.cwd || process.cwd(),
    input._simulate || null,
  );
  const dirtyCount = Number(input.dirtyCount || 0);

  const result = {
    version: OWNERSHIP_GUARD_VERSION,
    root: rootNorm,
    stream: workspace?.stream || null,
    workspaceId: workspace?.id || null,
    homeBranch: workspace?.defaultBranch || null,
    branch,
    unresolvedGit: unresolved,
    cursorSetActiveBranchIsNotAuthorization: true,
    note:
      "Cursor SetActiveBranch / UI branch chips do not authorize Git checkouts. Run dealality:git-preflight before switch/checkout/merge/rebase/stash/reset.",
  };

  if (unresolved.unresolved) {
    return {
      ...result,
      ok: false,
      statusCode: WORKSPACE_STATUS.BLOCKED_UNRESOLVED_GIT_OPERATION,
      reason: `unresolved git operation: ${unresolved.operations.map((o) => o.kind).join(", ")}`,
    };
  }

  // Integration Controller: product feature checkouts → remote merge only (before generic pattern fail)
  if (
    workspace?.stream === "integration" &&
    ["switch", "checkout"].includes(String(input.op || "")) &&
    branch &&
    branch !== "main"
  ) {
    const inferredEarly = inferBranchStream(branch, registry);
    if (PRODUCT_STREAMS.has(inferredEarly) || !matchesBranchPattern(workspace, branch)) {
      if (PRODUCT_STREAMS.has(inferredEarly)) {
        return {
          ...result,
          ok: false,
          statusCode: WORKSPACE_STATUS.BLOCKED_REMOTE_MERGE_ONLY,
          reason: `Integration Controller must not checkout product branch '${branch}' (${inferredEarly}); use remote gh pr merge instead`,
          foreignStream: inferredEarly,
        };
      }
    }
  }

  if (["switch", "checkout"].includes(String(input.op || "")) && branch) {
    const allowed = assertBranchAllowedInWorkspace({
      workspace,
      branch,
      registry,
      founderAuth: input.founderAuth || "",
      op: input.op,
    });
    if (!allowed.ok) {
      return { ...result, ...allowed, ok: false };
    }
  }

  if (
    ["switch", "checkout", "merge", "rebase", "stash"].includes(String(input.op || "")) &&
    dirtyCount > 0 &&
    input.allowDirtySatisfied !== true
  ) {
    return {
      ...result,
      ok: false,
      statusCode: WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED,
      reason: "dirty_or_untracked_evidence_requires_explicit_allow_or_preserve",
      dirtyCount,
    };
  }

  if (String(input.op || "") === "stash") {
    if (input.founderAuth !== "FOUNDER_AUTH_STASH_PRESERVE") {
      return {
        ...result,
        ok: false,
        statusCode: WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED,
        reason:
          "stash blocked as routine branch-switch workaround; use verified archive/preserve branch or --founder-auth FOUNDER_AUTH_STASH_PRESERVE",
      };
    }
  }

  return {
    ...result,
    ok: true,
    statusCode: WORKSPACE_STATUS.OK,
  };
}

/**
 * Classify post-merge completion from live facts.
 */
export function classifyHomeSyncStatus({
  merged = false,
  onHomeBranch = false,
  headEqualsOriginMain = false,
  dirtyCount = 0,
  blockedReason = null,
} = {}) {
  if (blockedReason === "CROSS_WORKSTREAM_BRANCH") {
    return WORKSPACE_STATUS.BLOCKED_CROSS_WORKSTREAM_BRANCH;
  }
  if (blockedReason === "UNRESOLVED_GIT_OPERATION") {
    return WORKSPACE_STATUS.BLOCKED_UNRESOLVED_GIT_OPERATION;
  }
  if (blockedReason === "PRESERVATION_REQUIRED") {
    return WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED;
  }
  if (!merged || !onHomeBranch || !headEqualsOriginMain) {
    return WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED;
  }
  if (dirtyCount === 0) {
    return WORKSPACE_STATUS.MERGED_AND_HOME_SYNCED_CLEAN_WORKTREE;
  }
  return WORKSPACE_STATUS.MERGED_AND_HOME_SYNCED;
}

export function listPermanentStreamHomes(registry) {
  return (registry.workspaces || [])
    .filter((w) => w.lifecycle === "permanent")
    .map((w) => ({
      stream: w.stream,
      path: normalizePath(w.path),
      homeBranch: w.defaultBranch,
      pattern: w.expectedBranchPattern,
    }));
}

export {
  findWorkspaceByPath,
  findWorkspaceByStream,
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  matchesBranchPattern,
  normalizePath,
};
