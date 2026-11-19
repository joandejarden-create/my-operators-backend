/**
 * Workspace ownership + unresolved Git operation helpers (no git-preflight import).
 * Used by git-preflight (fail-closed mutations) and PR completion reporting.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { matchesBranchPattern, normalizePath } from "./worktree-registry.js";

export const PRODUCT_STREAMS = Object.freeze(["he", "adp", "gdi"]);

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
  // Unmerged index entries (e.g. UU) without MERGE_HEAD still block safe completion.
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
 * True when branch belongs to another permanent product stream's pattern
 * and does not match the owning workspace pattern.
 */
export function classifyCrossWorkstreamBranch(registry, workspace, branch) {
  const b = String(branch || "");
  if (!workspace || !b || b === "(detached)") {
    return { crossWorkstream: false, matchedForeignStream: null };
  }
  if (matchesBranchPattern(workspace, b)) {
    return { crossWorkstream: false, matchedForeignStream: null };
  }
  const foreign = (registry.workspaces || []).find((w) => {
    if (w.id === workspace.id) return false;
    if (w.lifecycle !== "permanent") return false;
    if (!PRODUCT_STREAMS.includes(w.stream)) return false;
    return matchesBranchPattern(w, b);
  });
  if (foreign) {
    return {
      crossWorkstream: true,
      matchedForeignStream: foreign.stream,
      matchedForeignId: foreign.id,
      matchedForeignPattern: foreign.expectedBranchPattern,
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

  const targetBranch = String(targetRef || "").replace(/^refs\/heads\//, "");
  if (targetBranch) {
    const cross = classifyCrossWorkstreamBranch(registry, workspace, targetBranch);
    if (cross.crossWorkstream) {
      errors.push(
        `BLOCKED — CROSS_WORKSTREAM_BRANCH: refusing ${op} to '${targetBranch}' in ${workspace.id} (matches ${cross.matchedForeignStream} ownership)`,
      );
    }
  }

  if ((op === "merge" || op === "rebase") && currentBranch) {
    const crossNow = classifyCrossWorkstreamBranch(registry, workspace, currentBranch);
    if (crossNow.crossWorkstream) {
      errors.push(
        `BLOCKED — CROSS_WORKSTREAM_BRANCH: refusing ${op} while on foreign branch '${currentBranch}' (matches ${crossNow.matchedForeignStream})`,
      );
    }
  }

  return errors;
}
