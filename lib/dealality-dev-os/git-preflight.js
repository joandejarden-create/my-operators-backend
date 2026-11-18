/**
 * Dealality Dev OS — mandatory Git preflight (fail-closed).
 *
 * This module validates intended Git mutations. It does NOT monkey-patch git.
 * Enforcement is via: npm scripts, Cursor rules, optional core.hooksPath hooks,
 * and explicit agent/human invocation before mutations.
 */
import { execFileSync, execSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  findWorkspaceByPath,
  findWorkspaceByStream,
  isProtectedRef,
  loadWorktreeRegistry,
  matchesBranchPattern,
  normalizePath,
} from "./worktree-registry.js";

export const DESTRUCTIVE_OPS = new Set([
  "reset",
  "clean",
  "worktree-remove",
  "force-push",
  "hard-reset",
]);

export const MUTATING_OPS = new Set([
  "commit",
  "push",
  "merge",
  "rebase",
  "switch",
  "checkout",
  "worktree-add",
  "worktree-remove",
  "pr-sync",
  "reset",
  "clean",
  "force-push",
]);

function runGit(root, args, opts = {}) {
  try {
    return execFileSync("git", args, {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      ...opts,
    }).trim();
  } catch (err) {
    if (opts.throwOnFail) throw err;
    return "";
  }
}

export function resolveGitRoot(cwd = process.cwd()) {
  const out = runGit(cwd, ["rev-parse", "--show-toplevel"]);
  if (!out) throw new Error(`not a git worktree: ${cwd}`);
  return path.resolve(out);
}

export function parseWorktreeList(root) {
  const raw = runGit(root, ["worktree", "list", "--porcelain"]);
  const items = [];
  let cur = {};
  for (const line of String(raw || "").split(/\r?\n/)) {
    if (!line) {
      if (cur.path) items.push(cur);
      cur = {};
      continue;
    }
    if (line.startsWith("worktree ")) cur.path = normalizePath(line.slice(9));
    else if (line.startsWith("HEAD ")) cur.head = line.slice(5);
    else if (line.startsWith("branch ")) cur.branch = line.slice(7).replace(/^refs\/heads\//, "");
    else if (line === "detached") cur.detached = true;
    else if (line.startsWith("locked")) cur.locked = true;
  }
  if (cur.path) items.push(cur);
  return items;
}

export function collectDirtyPaths(root) {
  const porcelain = runGit(root, ["status", "--porcelain=v1"]);
  if (!porcelain) return [];
  return porcelain.split(/\r?\n/).filter(Boolean).map((line) => {
    const status = line.slice(0, 2);
    let file = line.slice(3).trim();
    if (file.includes(" -> ")) file = file.split(" -> ").pop();
    return { status, path: file.replace(/^"|"$/g, "") };
  });
}

function parseAllowDirty(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.map(String);
  return String(value)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function pathsAllowed(dirty, allowList) {
  if (!dirty.length) return true;
  if (allowList.includes("*")) return true;
  const allow = new Set(allowList.map((p) => p.replace(/\\/g, "/")));
  return dirty.every((d) => {
    const p = d.path.replace(/\\/g, "/");
    if (allow.has(p)) return true;
    // directory allow
    return [...allow].some((a) => a.endsWith("/") && p.startsWith(a));
  });
}

function branchOwner(worktrees, branch) {
  if (!branch) return null;
  return worktrees.find((w) => w.branch === branch) || null;
}

/**
 * @param {object} input
 * @param {string} input.op
 * @param {string} [input.cwd]
 * @param {string} [input.stream]
 * @param {string} [input.expectPath]
 * @param {string} [input.expectBranch]
 * @param {string} [input.targetRef]
 * @param {string|string[]} [input.allowDirty]
 * @param {string} [input.founderAuth]
 * @param {boolean} [input.force]
 * @param {string} [input.registryRoot]
 */
export function runGitPreflight(input = {}) {
  const op = String(input.op || "").toLowerCase();
  const errors = [];
  const warnings = [];
  const cwd = path.resolve(input.cwd || process.cwd());

  if (!op || (!MUTATING_OPS.has(op) && op !== "status-check")) {
    errors.push(`unknown or missing --op (got '${op || ""}')`);
  }

  let root;
  try {
    root = resolveGitRoot(cwd);
  } catch (err) {
    return {
      ok: false,
      op,
      errors: [String(err.message || err)],
      warnings,
      cwd,
    };
  }

  const registry = loadWorktreeRegistry(input.registryRoot || root);
  const worktrees = parseWorktreeList(root);
  const sim = input._simulate || {};
  const branch = sim.branch || runGit(root, ["branch", "--show-current"]) || "(detached)";
  const head = sim.head || runGit(root, ["rev-parse", "HEAD"]);
  const rootNorm = normalizePath(sim.root || root);
  const registered =
    sim.registered === undefined ? findWorkspaceByPath(registry, rootNorm) : sim.registered;
  const streamWs = input.stream ? findWorkspaceByStream(registry, input.stream) : null;
  const dirty = sim.dirty || collectDirtyPaths(root);
  const allowDirty = parseAllowDirty(input.allowDirty);
  const founderAuth = String(input.founderAuth || "");
  const force = Boolean(input.force) || op === "force-push";

  const facts = {
    cwd: normalizePath(cwd),
    root: rootNorm,
    branch,
    head,
    dirtyCount: dirty.length,
    dirtyPaths: dirty.map((d) => d.path),
    registeredId: registered?.id || null,
    stream: input.stream || registered?.stream || null,
    targetRef: input.targetRef || null,
    op,
  };

  // 1–2 cwd / worktree root
  if (normalizePath(cwd) !== rootNorm && !normalizePath(cwd).startsWith(rootNorm + "/")) {
    // cwd inside worktree is OK; outside is not
    if (!normalizePath(cwd).toLowerCase().startsWith(rootNorm.toLowerCase())) {
      errors.push(`cwd ${facts.cwd} is outside git root ${rootNorm}`);
    }
  }

  // Expected path / stream ownership
  if (input.expectPath) {
    if (normalizePath(input.expectPath) !== rootNorm) {
      errors.push(`worktree root mismatch: expected ${normalizePath(input.expectPath)}, actual ${rootNorm}`);
    }
  }
  if (streamWs) {
    if (streamWs.status === "planned" && normalizePath(streamWs.path) !== rootNorm) {
      errors.push(
        `stream '${input.stream}' permanent path ${streamWs.path} is not provisioned; refusing mutation in ${rootNorm}`,
      );
    } else if (streamWs.status !== "planned" && normalizePath(streamWs.path) !== rootNorm) {
      // temporary governance worktree may serve integration stream
      const tempOk =
        input.stream === "integration" &&
        (registry.temporaryWorkspaces || []).some((t) => normalizePath(t.path) === rootNorm);
      if (!tempOk) {
        errors.push(
          `stream '${input.stream}' expected path ${streamWs.pathNormalized}, actual ${rootNorm}`,
        );
      }
    }
  }

  if (!registered && op !== "status-check" && op !== "worktree-add") {
    errors.push(`unregistered worktree: ${rootNorm} (add via Integration Controller registry PR first)`);
  }

  // Expected branch
  if (input.expectBranch && branch !== input.expectBranch) {
    errors.push(`branch mismatch: expected '${input.expectBranch}', actual '${branch}'`);
  }

  // Dirty gate
  const dirtyOpsNeedingClean = new Set(["switch", "checkout", "merge", "rebase", "pr-sync", "worktree-remove"]);
  if (dirtyOpsNeedingClean.has(op) && dirty.length && !pathsAllowed(dirty, allowDirty)) {
    errors.push(
      `dirty worktree blocked for op=${op} (${dirty.length} paths). Pass --allow-dirty with explicit paths or preserve first.`,
    );
  }
  if (op === "commit" && dirty.length === 0) {
    warnings.push("nothing to commit (clean worktree)");
  }

  // Protected main / preservation
  if ((op === "commit" || op === "merge" || op === "rebase") && branch === "main") {
    if (founderAuth !== "FOUNDER_AUTH_MAIN_MUTATION") {
      errors.push("refusing mutation on local main without --founder-auth FOUNDER_AUTH_MAIN_MUTATION");
    }
  }
  if (op === "merge" || op === "rebase") {
    const target = String(input.targetRef || "").replace(/^refs\/heads\//, "");
    const isMainish = (name) => {
      const n = String(name || "").replace(/^refs\/heads\//, "");
      return n === "main" || n === "origin/main";
    };
    const isWorkspaceHome = (name) => /^ws\//.test(String(name || "").replace(/^refs\/heads\//, ""));
    // Merging origin/main into a feature/home branch is normal. Only block merges
    // that target preservation/backup refs, or merges while ON those refs.
    if (target && isProtectedRef(registry, target) && !isMainish(target) && !isWorkspaceHome(target)) {
      if (founderAuth !== "FOUNDER_AUTH_PRESERVE_REF_MUTATION") {
        errors.push(
          `refusing ${op} affecting protected ref '${target}' without --founder-auth FOUNDER_AUTH_PRESERVE_REF_MUTATION`,
        );
      }
    }
    if (
      isProtectedRef(registry, branch) &&
      !isMainish(branch) &&
      !isWorkspaceHome(branch) &&
      founderAuth !== "FOUNDER_AUTH_PRESERVE_REF_MUTATION"
    ) {
      errors.push(`refusing ${op} while on protected preservation branch '${branch}'`);
    }
  }

  // Push / force-push to main (product workspaces must never update main directly)
  const isMainRefName = (name) => {
    const n = String(name || "")
      .trim()
      .replace(/^refs\/heads\//, "")
      .replace(/^refs\/remotes\//, "");
    return n === "main" || n === "origin/main";
  };
  if (op === "push" || op === "force-push") {
    const pushTarget = String(input.targetRef || "").trim();
    if (
      branch === "main" ||
      isMainRefName(pushTarget) ||
      (force && isMainRefName(pushTarget))
    ) {
      if (founderAuth !== "FOUNDER_AUTH_MAIN_MUTATION") {
        errors.push(
          "refusing push to main/origin/main without --founder-auth FOUNDER_AUTH_MAIN_MUTATION",
        );
      }
    }
  }

  // Force-push
  if (force || op === "force-push") {
    if (founderAuth !== "FOUNDER_AUTH_FORCE_PUSH") {
      errors.push("force-push blocked without --founder-auth FOUNDER_AUTH_FORCE_PUSH");
    }
  }

  // Destructive ops
  if (DESTRUCTIVE_OPS.has(op)) {
    const destructiveAuthOk =
      founderAuth.startsWith("FOUNDER_AUTH_DESTRUCTIVE_") ||
      (op === "force-push" && founderAuth === "FOUNDER_AUTH_FORCE_PUSH");
    if (!destructiveAuthOk) {
      errors.push(`destructive op '${op}' requires --founder-auth FOUNDER_AUTH_DESTRUCTIVE_<REASON>`);
    }
    if (!input.preservationRef && !input.backupManifest) {
      errors.push(`destructive op '${op}' requires --preservation-ref or --backup-manifest`);
    }
    if (input.preservationRef) {
      const resolved = runGit(root, ["rev-parse", "--verify", input.preservationRef]);
      if (!resolved) errors.push(`preservation ref not found: ${input.preservationRef}`);
    }
    if (input.backupManifest && !fs.existsSync(input.backupManifest)) {
      errors.push(`backup manifest missing: ${input.backupManifest}`);
    }
  }

  // Branch ownership for switch / worktree-add
  if ((op === "switch" || op === "checkout") && input.targetRef) {
    const targetBranch = String(input.targetRef).replace(/^refs\/heads\//, "");
    const owner = branchOwner(worktrees, targetBranch);
    if (owner && owner.path !== rootNorm) {
      errors.push(
        `branch '${targetBranch}' already checked out in ${owner.path}; refuse switch in ${rootNorm}`,
      );
    }
  }

  if (op === "worktree-add") {
    const newPath = input.expectPath || input.targetPath;
    if (!newPath) errors.push("worktree-add requires --expect-path (new worktree path)");
    else if (fs.existsSync(newPath)) errors.push(`worktree-add path already exists: ${newPath}`);
    const intended = findWorkspaceByPath(registry, newPath);
    if (!intended && founderAuth !== "FOUNDER_AUTH_UNREGISTERED_WORKTREE") {
      errors.push(
        "refusing unregistered worktree-add without --founder-auth FOUNDER_AUTH_UNREGISTERED_WORKTREE",
      );
    }
  }

  if (op === "worktree-remove") {
    if (registered?.lifecycle === "permanent") {
      errors.push(`refusing removal of permanent workspace ${registered.id}`);
    }
  }

  // Behind origin/main + overlap with dirty files
  const originMain = runGit(root, ["rev-parse", "origin/main"]) || runGit(root, ["rev-parse", "main"]);
  if (originMain || sim.behindMain != null) {
    if (sim.behindMain != null) {
      facts.behindMain = Number(sim.behindMain);
      facts.aheadMain = Number(sim.aheadMain || 0);
    } else {
      const ab = runGit(root, ["rev-list", "--left-right", "--count", `origin/main...HEAD`]);
      const [behindStr, aheadStr] = String(ab || "0\t0").split(/\s+/);
      facts.behindMain = Number(behindStr || 0);
      facts.aheadMain = Number(aheadStr || 0);
    }
    if (facts.behindMain > 0 && ["merge", "push", "pr-sync", "commit"].includes(op)) {
      warnings.push(`branch is behind origin/main by ${facts.behindMain} commit(s)`);
      const changedOnMain = Array.isArray(sim.changedOnMain)
        ? sim.changedOnMain
        : runGit(root, ["diff", "--name-only", "HEAD...origin/main"])
            .split(/\r?\n/)
            .filter(Boolean);
      const dirtySet = new Set(dirty.map((d) => d.path.replace(/\\/g, "/")));
      const overlap = changedOnMain.filter((f) => dirtySet.has(f.replace(/\\/g, "/")));
      facts.upstreamDirtyOverlap = overlap;
      if (overlap.length && ["merge", "pr-sync", "switch", "checkout"].includes(op)) {
        errors.push(
          `upstream origin/main overlaps locally dirty files: ${overlap.slice(0, 12).join(", ")}`,
        );
      }
    }
  }

  // Branch pattern hint for registered permanent product workspaces
  if (registered && registered.lifecycle === "permanent" && registered.allowsProductFeatureBranches) {
    if (branch !== "main" && !matchesBranchPattern(registered, branch)) {
      warnings.push(
        `branch '${branch}' does not match expected pattern ${registered.expectedBranchPattern}`,
      );
    }
  }

  // Shared-file ownership: product streams must not edit the registry
  if (op === "commit" && registered && registered.stream !== "integration") {
    const staged = runGit(root, ["diff", "--name-only", "--cached"])
      .split(/\r?\n/)
      .filter(Boolean);
    const candidates = [
      ...staged,
      ...dirty.map((d) => d.path),
      ...(Array.isArray(sim.commitPaths) ? sim.commitPaths : []),
    ];
    if (candidates.some((f) => String(f).replace(/\\/g, "/").endsWith("worktree-registry.json"))) {
      errors.push("only Integration Controller may commit worktree-registry.json");
    }
  }

  return {
    ok: errors.length === 0,
    op,
    facts,
    errors,
    warnings,
    registered,
  };
}

export function formatPreflightResult(result) {
  const lines = [];
  lines.push(`[dealality:git-preflight] op=${result.op} ok=${result.ok}`);
  if (result.facts) {
    lines.push(`  root=${result.facts.root}`);
    lines.push(`  branch=${result.facts.branch}`);
    lines.push(`  head=${result.facts.head}`);
    lines.push(`  dirty=${result.facts.dirtyCount}`);
    lines.push(`  registered=${result.facts.registeredId || "(none)"}`);
    if (result.facts.behindMain != null) {
      lines.push(`  vs origin/main: behind=${result.facts.behindMain} ahead=${result.facts.aheadMain}`);
    }
  }
  for (const w of result.warnings || []) lines.push(`  WARN ${w}`);
  for (const e of result.errors || []) lines.push(`  ERROR ${e}`);
  return lines.join("\n");
}

/** Thin helper for hooks that only know argv flags. */
export function preflightFromArgv(argv = process.argv.slice(2), cwd = process.cwd()) {
  const get = (name) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : "";
  };
  const has = (name) => argv.includes(name);
  return runGitPreflight({
    op: get("--op") || "status-check",
    cwd,
    stream: get("--stream"),
    expectPath: get("--expect-path"),
    expectBranch: get("--expect-branch"),
    targetRef: get("--target-ref") || get("--target-path"),
    targetPath: get("--target-path"),
    allowDirty: get("--allow-dirty"),
    founderAuth: get("--founder-auth"),
    preservationRef: get("--preservation-ref"),
    backupManifest: get("--backup-manifest"),
    force: has("--force"),
  });
}
