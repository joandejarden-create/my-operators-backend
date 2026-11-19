/**
 * Workspace ownership + unresolved Git operation helpers (no git-preflight import).
 * Used by git-preflight (fail-closed mutations) and PR completion reporting.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { matchesBranchPattern, normalizePath } from "./worktree-registry.js";

export const PRODUCT_STREAMS = Object.freeze(["he", "adp", "gdi"]);

/** Narrow founder authorizations for exceptional branch targets. */
export const OWNERSHIP_AUTH = Object.freeze({
  CROSS_STREAM: "FOUNDER_AUTH_CROSS_STREAM_BRANCH",
  UNCLASSIFIED: "FOUNDER_AUTH_UNCLASSIFIED_BRANCH",
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

/** Detect in-progress merge / rebase / cherry-pick / revert / unmerged index. */
export function detectUnresolvedGitOperation(root, { gitDir } = {}) {
  const gd = gitDir || path.join(root, ".git");
  let resolved = gd;
  if (fs.existsSync(gd) && fs.statSync(gd).isFile()) {
    const text = fs.readFileSync(gd, "utf8");
    const m = text.match(/gitdir:\s*(.+)/i);
    if (m) resolved = path.resolve(root, m[1].trim());
  }
  const markers = [
    ["MERGE_HEAD", "merge"],
    ["CHERRY_PICK_HEAD", "cherry-pick"],
    ["REVERT_HEAD", "revert"],
    ["rebase-merge", "rebase"],
    ["rebase-apply", "rebase"],
  ];
  for (const [name, kind] of markers) {
    const p = path.join(resolved, name);
    if (fs.existsSync(p)) {
      return { active: true, kind, marker: name, path: normalizePath(p) };
    }
  }
  if (fs.existsSync(root)) {
    const unmerged = runGit(root, ["ls-files", "-u"])
      .split(/\r?\n/)
      .filter(Boolean);
    if (unmerged.length) {
      return {
        active: true,
        kind: "unmerged-index",
        marker: "ls-files -u",
        path: normalizePath(root),
        sample: unmerged.slice(0, 8),
      };
    }
  }
  return { active: false, kind: null, marker: null, path: null };
}

/**
 * Classify a branch relative to a permanent product workspace.
 * @returns {{ kind: 'owned'|'foreign'|'unclassified'|'mainish'|'empty', matchedForeignStream?: string|null }}
 */
export function classifyBranchOwnership(registry, workspace, branch) {
  const b = String(branch || "").replace(/^refs\/heads\//, "");
  if (!workspace || !b || b === "(detached)") {
    return { kind: "empty", matchedForeignStream: null };
  }
  if (b === "main" || b === "origin/main") {
    return { kind: "mainish", matchedForeignStream: null };
  }
  if (matchesBranchPattern(workspace, b) || b === workspace.defaultBranch) {
    return { kind: "owned", matchedForeignStream: null };
  }
  const foreign = (registry.workspaces || []).find((w) => {
    if (w.id === workspace.id) return false;
    if (w.lifecycle !== "permanent") return false;
    if (!PRODUCT_STREAMS.includes(w.stream)) return false;
    return matchesBranchPattern(w, b) || b === w.defaultBranch;
  });
  if (foreign) {
    return {
      kind: "foreign",
      matchedForeignStream: foreign.stream,
      matchedForeignId: foreign.id,
      matchedForeignPattern: foreign.expectedBranchPattern,
    };
  }
  return { kind: "unclassified", matchedForeignStream: null };
}

/**
 * True when branch belongs to another permanent product stream's pattern
 * and does not match the owning workspace pattern.
 */
export function classifyCrossWorkstreamBranch(registry, workspace, branch) {
  const c = classifyBranchOwnership(registry, workspace, branch);
  if (c.kind === "foreign") {
    return {
      crossWorkstream: true,
      matchedForeignStream: c.matchedForeignStream,
      matchedForeignId: c.matchedForeignId,
      matchedForeignPattern: c.matchedForeignPattern,
    };
  }
  return { crossWorkstream: false, matchedForeignStream: null };
}

/**
 * Fail-closed ownership gate for branch mutation ops.
 * Returns error strings (empty if allowed).
 */
export function ownershipMutationErrors({
  op,
  registry,
  workspace,
  currentBranch,
  targetRef,
  unresolved,
  founderAuth = "",
}) {
  const errors = [];
  if (unresolved?.active) {
    errors.push(
      `BLOCKED — UNRESOLVED_GIT_OPERATION: active ${unresolved.kind} (${unresolved.marker}); reconcile before ${op}`,
    );
  }
  if (!workspace || workspace.lifecycle !== "permanent") return errors;
  if (!PRODUCT_STREAMS.includes(workspace.stream)) return errors;

  const mutationOps = new Set(["switch", "checkout", "merge", "rebase", "worktree-add"]);
  if (!mutationOps.has(op)) return errors;

  const auth = String(founderAuth || "");
  const targetBranch = String(targetRef || "").replace(/^refs\/heads\//, "");

  if (targetBranch) {
    const classification = classifyBranchOwnership(registry, workspace, targetBranch);
    if (classification.kind === "foreign") {
      if (auth !== OWNERSHIP_AUTH.CROSS_STREAM) {
        errors.push(
          `BLOCKED — CROSS_WORKSTREAM_BRANCH: refusing ${op} to '${targetBranch}' in ${workspace.id} (matches ${classification.matchedForeignStream} ownership)`,
        );
      }
    } else if (classification.kind === "unclassified") {
      if (auth !== OWNERSHIP_AUTH.UNCLASSIFIED) {
        errors.push(
          `BLOCKED — UNCLASSIFIED_BRANCH: refusing ${op} to '${targetBranch}' in ${workspace.id} (not owned by this stream; requires ${OWNERSHIP_AUTH.UNCLASSIFIED})`,
        );
      }
    } else if (classification.kind === "mainish" && (op === "switch" || op === "checkout")) {
      // Product permanent workspaces must not check out main; home branches track origin/main tip.
      errors.push(
        `BLOCKED — CROSS_WORKSTREAM_BRANCH: refusing ${op} to '${targetBranch}' in product workspace ${workspace.id}; use home '${workspace.defaultBranch}'`,
      );
    }
  }

  if ((op === "merge" || op === "rebase") && currentBranch) {
    const crossNow = classifyCrossWorkstreamBranch(registry, workspace, currentBranch);
    if (crossNow.crossWorkstream && auth !== OWNERSHIP_AUTH.CROSS_STREAM) {
      errors.push(
        `BLOCKED — CROSS_WORKSTREAM_BRANCH: refusing ${op} while on foreign branch '${currentBranch}' (matches ${crossNow.matchedForeignStream})`,
      );
    }
    const curClass = classifyBranchOwnership(registry, workspace, currentBranch);
    if (curClass.kind === "unclassified" && auth !== OWNERSHIP_AUTH.UNCLASSIFIED) {
      errors.push(
        `BLOCKED — UNCLASSIFIED_BRANCH: refusing ${op} while on unclassified branch '${currentBranch}'`,
      );
    }
  }

  return errors;
}

/**
 * Static enforcement coverage matrix (honest: not every Cursor entry point is interceptable).
 */
export function getOwnershipEnforcementCoverage() {
  return {
    contract: "DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1",
    enforcementLevel: "PARTIAL",
    machineEnforced: [
      {
        entry: "npm run dealality:git-preflight -- --op switch|checkout|merge|rebase",
        mechanism: "ownershipMutationErrors in lib/dealality-dev-os/git-preflight.js",
        blocks: ["cross-stream targets", "unclassified targets", "unresolved merge/index", "product checkout of main"],
      },
      {
        entry: "npm run dealality:post-merge-sync",
        mechanism: "FETCH_ONLY_ACTIVE_TASK / never auto-switch active unfinished tasks; no reset/clean/stash-discard",
        blocks: ["auto home-return while active task", "destructive cleanup"],
      },
      {
        entry: "npm run dealality:pr-completion-report",
        mechanism: "fail-closed COMPLETE requires verified merge SHA + CI + home sync + assessed preservation",
        blocks: ["COMPLETE with null/unknown merge or CI", "COMPLETE with NOT_ASSESSED preservation"],
      },
    ],
    instructionGovernedOnly: [
      {
        entry: "Raw git switch/checkout/merge without dealality:git-preflight",
        note: "Preflight does not monkey-patch git; agents/humans can bypass unless they invoke preflight or hooks.",
      },
      {
        entry: "Cursor SetActiveBranch / UI branch chip",
        note: "Updates conversation/UI metadata only; does not change Git worktree and is not ownership-gated.",
      },
      {
        entry: "Cursor cloud-agent / worktree reattachment / move_agent_to_root",
        note: "Changes agent root/UI attachment; not intercepted by Dealality ownership code.",
      },
      {
        entry: "Workspace startup prompts",
        note: "Instructional only; no runtime gate.",
      },
      {
        entry: "Integration Controller gh pr merge",
        note: "Remote merge is intentional and does not require a clean local Integration checkout; local sync remains separate and preserve-first.",
      },
    ],
    unavoidableBypasses: [
      "Any direct git invocation that skips dealality:git-preflight",
      "IDE Source Control branch UI that calls git without the npm preflight wrapper",
      "Cursor branch-chip / SetActiveBranch (non-Git)",
      "External agents or scripts outside this repo's Dev OS commands",
    ],
  };
}
