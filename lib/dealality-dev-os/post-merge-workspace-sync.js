/**
 * Dealality Dev OS — post-merge product workspace synchronization.
 *
 * After a PR merges into origin/main, safely:
 *   1) return the affected product workspace to its registered home branch
 *   2) fast-forward that home branch to origin/main
 *   3) verify git-preflight status-check
 *   4) fetch / FF idle peer product workspaces (never switch active task branches)
 *
 * Hard safety: never reset, clean, stash-discard, force-push, or overwrite dirty work.
 * Default plan mode is dry-run; mutations require apply=true.
 */
import { execFileSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  collectDirtyPaths,
  formatPreflightResult,
  runGitPreflight,
} from "./git-preflight.js";
import {
  findWorkspaceByStream,
  loadWorktreeRegistry,
  normalizePath,
} from "./worktree-registry.js";
import {
  classifyHomeSyncStatus,
  WORKSPACE_STATUS,
} from "./workspace-ownership-guard-v1.js";

export const PRODUCT_STREAMS = ["he", "adp", "gdi", "be"];

export const SYNC_ACTIONS = Object.freeze({
  ALREADY_SYNCED: "ALREADY_SYNCED",
  FAST_FORWARD_HOME: "FAST_FORWARD_HOME",
  RETURN_HOME_THEN_FF: "RETURN_HOME_THEN_FF",
  PRESERVE_THEN_RETURN_HOME: "PRESERVE_THEN_RETURN_HOME",
  FETCH_ONLY_ACTIVE_TASK: "FETCH_ONLY_ACTIVE_TASK",
  BLOCKED_DIRTY_SWITCH: "BLOCKED_DIRTY_SWITCH",
  BLOCKED_UPSTREAM_DIRTY_OVERLAP: "BLOCKED_UPSTREAM_DIRTY_OVERLAP",
  SKIP_MISSING_PATH: "SKIP_MISSING_PATH",
  SKIP_INTEGRATION_DIRTY: "SKIP_INTEGRATION_DIRTY",
  SKIP_INTEGRATION: "SKIP_INTEGRATION",
});

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

function parseLeftRight(root) {
  const ab = runGit(root, ["rev-list", "--left-right", "--count", "origin/main...HEAD"]);
  const [behindStr, aheadStr] = String(ab || "0\t0").split(/\s+/);
  return {
    behindMain: Number(behindStr || 0),
    aheadMain: Number(aheadStr || 0),
  };
}

function upstreamDirtyOverlap(root, dirtyPaths) {
  if (!dirtyPaths.length) return [];
  const changed = runGit(root, ["diff", "--name-only", "HEAD...origin/main"])
    .split(/\r?\n/)
    .filter(Boolean)
    .map((p) => p.replace(/\\/g, "/"));
  const dirtySet = new Set(dirtyPaths.map((p) => p.replace(/\\/g, "/")));
  return changed.filter((f) => dirtySet.has(f));
}

function isAncestor(root, maybeAncestor, descendant) {
  try {
    execFileSync("git", ["merge-base", "--is-ancestor", maybeAncestor, descendant], {
      cwd: root,
      stdio: ["ignore", "ignore", "ignore"],
    });
    return true;
  } catch {
    return false;
  }
}

function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

/** Copy dirty files to an external verified backup before branch switches. */
export function createVerifiedBackup({ root, dirtyPaths, backupRoot, now = new Date() }) {
  const stamp = now.toISOString().replace(/[:.]/g, "-");
  const dest = path.join(backupRoot, `dealality-post-merge-preserve-${stamp}`);
  fs.mkdirSync(dest, { recursive: true });
  const entries = [];
  for (const relRaw of dirtyPaths || []) {
    const rel = String(relRaw?.path || relRaw).replace(/\\/g, "/");
    const src = path.join(root, rel);
    if (!fs.existsSync(src)) {
      entries.push({ path: rel, missing: true });
      continue;
    }
    const stat = fs.statSync(src);
    if (stat.isDirectory()) continue;
    const target = path.join(dest, rel);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.copyFileSync(src, target);
    const digest = sha256File(target);
    if (digest !== sha256File(src)) {
      throw new Error(`backup hash mismatch for ${rel}`);
    }
    entries.push({ path: rel, bytes: stat.size, sha256: digest, verified: true });
  }
  const manifestPath = path.join(dest, "MANIFEST.json");
  const manifest = {
    version: "dealality_post_merge_workspace_sync_v1",
    createdAt: now.toISOString(),
    sourceRoot: normalizePath(root),
    entries,
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  return { backupDir: dest, manifestPath, manifest };
}

export function refuseDestructiveFlags(input = {}) {
  if (input.reset || input.clean || input.discard || input.forceClean || input.force) {
    return "refusing destructive flags (reset/clean/discard/force are prohibited)";
  }
  return null;
}

/**
 * Tracked dirty paths that also changed between tips (overwrite risk).
 * Untracked (??) paths are preserved across switch/FF and are not overwrite risks.
 */
export function findOverwriteRisks({ dirtyPaths = [], changedBetweenTips = [] } = {}) {
  const changed = new Set(
    (changedBetweenTips || []).map((p) => String(p).replace(/\\/g, "/")),
  );
  return (dirtyPaths || [])
    .filter((d) => {
      const p = String(d.path || d).replace(/\\/g, "/");
      const status = String(d.status || "");
      const tracked = !status.startsWith("??");
      return tracked && changed.has(p);
    })
    .map((d) => String(d.path || d).replace(/\\/g, "/"));
}

export function resolveMergedPrHead(opts = {}) {
  const pr = Number(String(opts.pr || "").replace(/^#/, ""));
  if (!Number.isInteger(pr) || pr <= 0) return null;
  if (opts._prInfo) {
    const injected = opts._prInfo;
    if (injected.ok === false) return injected;
    if (injected.ok === true) return injected;
    // Allow raw gh-shaped objects via _prInfo for tests.
    if (String(injected.state || "").toUpperCase() === "MERGED") {
      return {
        ok: true,
        pr,
        headRefName: injected.headRefName || null,
        mergeCommit: injected.mergeCommit?.oid || injected.mergeCommit || null,
        url: injected.url || null,
        info: injected,
      };
    }
    return { ok: false, error: `PR #${pr} is not MERGED (state=${injected.state})`, info: injected };
  }
  try {
    const raw = execFileSync(
      "gh",
      ["pr", "view", String(pr), "--json", "state,mergedAt,mergeCommit,headRefName,baseRefName,url"],
      { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    ).trim();
    const info = JSON.parse(raw);
    if (String(info.state || "").toUpperCase() !== "MERGED") {
      return { ok: false, error: `PR #${pr} is not MERGED (state=${info.state})`, info };
    }
    const mergeCommit = info.mergeCommit?.oid || info.mergeCommit || null;
    const ancestryRoot = opts.ancestryRoot || opts.registryRoot || process.cwd();
    if (mergeCommit && opts.verifyMergeAncestry !== false) {
      const originMain = runGit(ancestryRoot, ["rev-parse", "origin/main"]);
      if (originMain && mergeCommit !== originMain && !isAncestor(ancestryRoot, mergeCommit, "origin/main")) {
        return {
          ok: false,
          error: `merge commit ${mergeCommit} is not an ancestor of origin/main tip ${originMain}`,
          info,
        };
      }
    }
    return {
      ok: true,
      pr,
      headRefName: info.headRefName || null,
      mergeCommit,
      url: info.url || null,
      info,
    };
  } catch (err) {
    return { ok: false, error: String(err?.stderr || err?.message || err) };
  }
}

/**
 * Pure classifier used by tests and the planner.
 *
 * Important: do NOT treat "HEAD is ancestor of origin/main" alone as a merged
 * task branch — a brand-new branch from main with only dirty WIP looks the same.
 * Return-home requires an explicit merged identity (`mergedBranch` / `prHeadRef`).
 */
export function classifyProductWorkspaceSync(input = {}) {
  const stream = String(input.stream || "");
  const homeBranch = String(input.homeBranch || "");
  const branch = String(input.branch || "(detached)");
  const behindMain = Number(input.behindMain || 0);
  const dirtyPaths = Array.isArray(input.dirtyPaths) ? input.dirtyPaths : [];
  const overlapPaths = Array.isArray(input.overlapPaths) ? input.overlapPaths : [];
  const pathExists = input.pathExists !== false;
  const isAffected = Boolean(input.isAffectedStream);
  const mergedBranch = String(input.mergedBranch || "");
  const prHeadRef = String(input.prHeadRef || "");

  if (!pathExists) {
    return {
      action: SYNC_ACTIONS.SKIP_MISSING_PATH,
      safeToMutate: false,
      reason: "registered path missing",
      stream,
      homeBranch,
      branch,
    };
  }

  const onHome = branch === homeBranch;
  const dirty = dirtyPaths.length > 0;
  const onExplicitMergedHead =
    !onHome &&
    branch !== "main" &&
    branch !== "(detached)" &&
    ((mergedBranch && branch === mergedBranch) || (prHeadRef && branch === prHeadRef));

  // Active unfinished task: any non-home branch without explicit merged identity.
  if (!onHome && !onExplicitMergedHead) {
    return {
      action: SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK,
      safeToMutate: false,
      mutateOps: ["fetch"],
      reason: `active task branch '${branch}' — fetch only; do not auto-switch`,
      stream,
      homeBranch,
      branch,
      dirtyCount: dirtyPaths.length,
    };
  }

  if (onExplicitMergedHead) {
    if (overlapPaths.length) {
      return {
        action: SYNC_ACTIONS.BLOCKED_UPSTREAM_DIRTY_OVERLAP,
        safeToMutate: false,
        mutateOps: ["fetch"],
        reason: "cannot return home: dirty paths overlap upstream origin/main changes",
        stream,
        homeBranch,
        branch,
        overlapPaths,
        dirtyPaths,
      };
    }
    if (dirty) {
      return {
        action: SYNC_ACTIONS.PRESERVE_THEN_RETURN_HOME,
        safeToMutate: true,
        mutateOps: ["fetch", "backup", "switch", "ff-merge", "preflight"],
        reason: isAffected
          ? "affected merged head has dirty WIP — verified backup then return home + FF"
          : "merged head has dirty WIP — verified backup then return home + FF",
        stream,
        homeBranch,
        branch,
        dirtyPaths,
        requireBackup: true,
      };
    }
    return {
      action: SYNC_ACTIONS.RETURN_HOME_THEN_FF,
      safeToMutate: true,
      mutateOps: ["fetch", "switch", "ff-merge", "preflight"],
      reason: isAffected
        ? "affected stream post-merge: return clean workspace to home then fast-forward"
        : "merged task branch is clean: return to home then fast-forward",
      stream,
      homeBranch,
      branch,
    };
  }

  // On home branch
  if (onHome) {
    if (behindMain <= 0) {
      return {
        action: SYNC_ACTIONS.ALREADY_SYNCED,
        safeToMutate: false,
        mutateOps: ["fetch"],
        reason: "home branch already at origin/main tip",
        stream,
        homeBranch,
        branch,
        dirtyCount: dirtyPaths.length,
      };
    }
    if (overlapPaths.length) {
      return {
        action: SYNC_ACTIONS.BLOCKED_UPSTREAM_DIRTY_OVERLAP,
        safeToMutate: false,
        mutateOps: ["fetch"],
        reason: "origin/main overlaps locally dirty paths; preserve WIP and refuse FF",
        stream,
        homeBranch,
        branch,
        overlapPaths,
        dirtyPaths,
      };
    }
    return {
      action: SYNC_ACTIONS.FAST_FORWARD_HOME,
      safeToMutate: true,
      mutateOps: ["fetch", "ff-merge", "preflight"],
      reason: dirty
        ? "on home; no upstream dirty overlap — safe fast-forward while preserving dirty files"
        : "on home and behind origin/main — safe fast-forward",
      stream,
      homeBranch,
      branch,
      dirtyCount: dirtyPaths.length,
      behindMain,
    };
  }

  return {
    action: SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK,
    safeToMutate: false,
    mutateOps: ["fetch"],
    reason: `unexpected branch state '${branch}' — fetch only`,
    stream,
    homeBranch,
    branch,
  };
}

export function inspectWorkspaceLiveState(wsPath, homeBranch) {
  const root = path.resolve(wsPath);
  const exists = fs.existsSync(root);
  if (!exists) {
    return {
      path: normalizePath(root),
      exists: false,
      branch: null,
      head: null,
      dirtyPaths: [],
      behindMain: 0,
      aheadMain: 0,
      overlapPaths: [],
      branchMergedIntoMain: false,
    };
  }
  const branch = runGit(root, ["branch", "--show-current"]) || "(detached)";
  const head = runGit(root, ["rev-parse", "HEAD"]);
  const dirty = collectDirtyPaths(root);
  const dirtyPaths = dirty.map((d) => d.path);
  const { behindMain, aheadMain } = parseLeftRight(root);
  const overlapPaths = behindMain > 0 ? upstreamDirtyOverlap(root, dirtyPaths) : [];
  // Informational only — never sufficient alone to authorize return-home.
  const tipContainedInMain =
    Boolean(head) &&
    branch !== homeBranch &&
    branch !== "main" &&
    branch !== "(detached)" &&
    isAncestor(root, head, "origin/main");

  return {
    path: normalizePath(root),
    exists: true,
    branch,
    head,
    dirtyPaths,
    dirtyCount: dirtyPaths.length,
    behindMain,
    aheadMain,
    overlapPaths,
    tipContainedInMain,
    homeHead: runGit(root, ["rev-parse", homeBranch]) || null,
    originMain: runGit(root, ["rev-parse", "origin/main"]) || null,
  };
}

/**
 * Build a sync plan for product workspaces (+ optional integration note).
 */
export function planPostMergeWorkspaceSync(opts = {}) {
  const destructive = refuseDestructiveFlags(opts);
  if (destructive) {
    return {
      ok: false,
      mode: opts.mode || "post-merge",
      dryRun: opts.apply !== true,
      status: "BLOCKED",
      error: destructive,
      productPlans: [],
      integrationPlan: null,
      summary: {
        productCount: 0,
        actionableCount: 0,
        blockedCount: 1,
        fetchOnlyCount: 0,
        alreadySyncedCount: 0,
      },
    };
  }

  const registryRoot = opts.registryRoot || process.cwd();
  const registry = opts.registry || loadWorktreeRegistry(registryRoot);
  const affectedStream = opts.affectedStream ? String(opts.affectedStream).toLowerCase() : null;
  let mergedBranch = opts.mergedBranch ? String(opts.mergedBranch) : "";
  let prHeadRef = opts.prHeadRef ? String(opts.prHeadRef) : "";
  const prResolution = opts.pr
    ? resolveMergedPrHead({
        pr: opts.pr,
        _prInfo: opts._prInfo,
        registryRoot,
        ancestryRoot: registryRoot,
        verifyMergeAncestry: opts.verifyMergeAncestry,
      })
    : null;
  if (prResolution) {
    if (prResolution.ok === false) {
      return {
        ok: false,
        mode: opts.mode || "post-merge",
        dryRun: opts.apply !== true,
        affectedStream,
        mergedBranch: mergedBranch || null,
        error: prResolution.error,
        productPlans: [],
        integrationPlan: null,
        summary: {
          productCount: 0,
          actionableCount: 0,
          blockedCount: 1,
          fetchOnlyCount: 0,
          alreadySyncedCount: 0,
        },
      };
    }
    prHeadRef = prHeadRef || prResolution.headRefName || "";
    mergedBranch = mergedBranch || prHeadRef;
  }
  const streams = Array.isArray(opts.streams) && opts.streams.length
    ? opts.streams.map((s) => String(s).toLowerCase())
    : PRODUCT_STREAMS.slice();
  const fetchFirst = opts.fetch !== false;
  const liveOverride = opts._liveByStream || null;

  const fetchNote = [];
  if (fetchFirst && !opts._skipLiveFetch) {
    // One fetch updates shared refs for all worktrees of this repo.
    const anyProduct = findWorkspaceByStream(registry, streams[0]);
    const fetchCwd =
      (anyProduct && fs.existsSync(anyProduct.path) && anyProduct.path) ||
      registryRoot;
    try {
      runGit(fetchCwd, ["fetch", "origin", "--prune"], { throwOnFail: true });
      fetchNote.push({ ok: true, cwd: normalizePath(fetchCwd) });
    } catch (err) {
      fetchNote.push({
        ok: false,
        cwd: normalizePath(fetchCwd),
        error: String(err?.stderr || err?.message || err),
      });
    }
  }

  const productPlans = [];
  for (const stream of streams) {
    const ws = findWorkspaceByStream(registry, stream);
    if (!ws) {
      productPlans.push({
        stream,
        action: SYNC_ACTIONS.SKIP_MISSING_PATH,
        safeToMutate: false,
        reason: `stream '${stream}' not in registry`,
      });
      continue;
    }
    const homeBranch = ws.defaultBranch || `ws/${stream === "he" ? "hotel-explorer" : stream}`;
    const live =
      liveOverride?.[stream] ||
      inspectWorkspaceLiveState(ws.path, homeBranch);
    const decision = classifyProductWorkspaceSync({
      stream,
      homeBranch,
      branch: live.branch || "(detached)",
      behindMain: live.behindMain,
      aheadMain: live.aheadMain,
      dirtyPaths: live.dirtyPaths || [],
      overlapPaths: live.overlapPaths || [],
      pathExists: live.exists,
      isAffectedStream: affectedStream === stream,
      mergedBranch,
      prHeadRef,
    });
    productPlans.push({
      ...decision,
      workspaceId: ws.id,
      path: ws.pathNormalized || normalizePath(ws.path),
      live,
    });
  }

  const integration = findWorkspaceByStream(registry, "integration");
  let integrationPlan = null;
  if (integration) {
    const live = liveOverride?.integration || inspectWorkspaceLiveState(integration.path, "main");
    if (!live.exists) {
      integrationPlan = {
        stream: "integration",
        action: SYNC_ACTIONS.SKIP_MISSING_PATH,
        safeToMutate: false,
        reason: "integration path missing",
        path: integration.path,
      };
    } else if ((live.dirtyPaths || []).length > 0) {
      integrationPlan = {
        stream: "integration",
        action: SYNC_ACTIONS.SKIP_INTEGRATION_DIRTY,
        safeToMutate: false,
        reason:
          "primary/integration checkout is dirty — preserve WIP; do not auto FF/reset; sync product workspaces instead",
        path: integration.pathNormalized || normalizePath(integration.path),
        live,
      };
    } else if (live.behindMain > 0 && live.branch === "main") {
      integrationPlan = {
        stream: "integration",
        action: SYNC_ACTIONS.FAST_FORWARD_HOME,
        safeToMutate: true,
        mutateOps: ["fetch", "ff-merge", "preflight"],
        reason: "integration main is clean and behind origin/main",
        path: integration.pathNormalized || normalizePath(integration.path),
        homeBranch: "main",
        branch: live.branch,
        live,
      };
    } else {
      integrationPlan = {
        stream: "integration",
        action: SYNC_ACTIONS.SKIP_INTEGRATION,
        safeToMutate: false,
        reason: "integration handled separately / already current or not on main",
        path: integration.pathNormalized || normalizePath(integration.path),
        live,
      };
    }
  }

  const blocked = productPlans.filter((p) =>
    [
      SYNC_ACTIONS.BLOCKED_DIRTY_SWITCH,
      SYNC_ACTIONS.BLOCKED_UPSTREAM_DIRTY_OVERLAP,
      SYNC_ACTIONS.SKIP_MISSING_PATH,
    ].includes(p.action),
  );
  const actionable = productPlans.filter((p) => p.safeToMutate);

  const ok = fetchNote.every((f) => f.ok !== false) && blocked.length === 0;
  const affectedPlan = productPlans.find((p) => p.stream === affectedStream) || null;
  const completionStatus = (() => {
    if (blocked.length) {
      if (blocked.some((p) => p.action === SYNC_ACTIONS.BLOCKED_DIRTY_SWITCH)) {
        return WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED;
      }
      return WORKSPACE_STATUS.BLOCKED_PRESERVATION_REQUIRED;
    }
    if (!affectedPlan || !opts.pr) return ok ? "READY" : "BLOCKED";
    const live = affectedPlan.live || {};
    return classifyHomeSyncStatus({
      merged: Boolean(prResolution?.ok || opts.pr),
      onHomeBranch: live.branch === affectedPlan.homeBranch,
      headEqualsOriginMain: Number(live.behindMain || 0) === 0 && Number(live.aheadMain || 0) === 0,
      dirtyCount: (live.dirtyPaths || []).length,
    });
  })();
  return {
    ok,
    status: ok ? "READY" : "BLOCKED",
    completionStatus,
    mode: opts.mode || "post-merge",
    dryRun: opts.apply !== true,
    affectedStream,
    mergedBranch: mergedBranch || null,
    prHeadRef: prHeadRef || null,
    pr: prResolution || null,
    fetched: fetchNote,
    productPlans,
    integrationPlan,
    summary: {
      productCount: productPlans.length,
      actionableCount: actionable.length,
      blockedCount: blocked.length,
      fetchOnlyCount: productPlans.filter((p) => p.action === SYNC_ACTIONS.FETCH_ONLY_ACTIVE_TASK)
        .length,
      alreadySyncedCount: productPlans.filter((p) => p.action === SYNC_ACTIONS.ALREADY_SYNCED)
        .length,
    },
  };
}

/**
 * Auditable runtime/preserve directory allows for post-merge switch/FF.
 * Prefer explicit product runtime roots over blanket `data` / `reports` / `tmp`.
 * Overwrite-risk gating for tracked source conflicts remains separate and fail-closed.
 */
export const POST_MERGE_RUNTIME_ALLOW_PREFIXES = Object.freeze([
  "data/group-demand-intelligence",
  "data/ai-demand-positioning",
  "data/ai-visibility/runtime",
  "data/adp-durable-published",
  "data/ava-data",
  "reports/gdi-canonical-full-e2e-entry-v1",
  "reports/gdi-association-events-runs",
  "reports/gdi-scaling-companies-hotel-runner-v1",
  "tmp",
]);

function normalizeRepoPath(raw) {
  return String(typeof raw === "string" ? raw : raw?.path || "")
    .replace(/\\/g, "/")
    .replace(/^\.\//, "")
    .replace(/\/+$/, "");
}

/** `reports/gdi-foo/...` or `reports/adp-foo/...` → pack root for directory allow. */
export function reportPackAllowRoot(repoPath) {
  const p = normalizeRepoPath(repoPath);
  const m = p.match(/^(reports\/(?:gdi|adp)-[^/]+)/);
  return m ? m[1] : null;
}

function isCoveredByAllowPrefix(repoPath, prefix) {
  const p = normalizeRepoPath(repoPath);
  const a = normalizeRepoPath(prefix);
  return p === a || p.startsWith(`${a}/`);
}

/**
 * Build allow-dirty list for preflight during return-home / FF.
 *
 * Seeds only the auditable runtime prefix set (plus discovered gdi/adp report pack roots
 * from the live dirty list) so gitignore flips do not block sync. Non-runtime dirty paths
 * are listed explicitly. Never uses `*`.
 */
export function allowDirtyForPreflight(dirtyPaths) {
  if (!dirtyPaths?.length) return "";
  const allows = new Set(POST_MERGE_RUNTIME_ALLOW_PREFIXES);
  for (const raw of dirtyPaths) {
    const p = normalizeRepoPath(raw);
    if (!p) continue;
    const packRoot = reportPackAllowRoot(p);
    if (packRoot) allows.add(packRoot);
    const underRuntime = [...allows].some((prefix) => isCoveredByAllowPrefix(p, prefix));
    if (!underRuntime) allows.add(p);
  }
  return [...allows].join(",");
}

function verifyPreflight(wsPath, stream, homeBranch) {
  const result = runGitPreflight({
    op: "status-check",
    cwd: wsPath,
    stream,
    expectPath: wsPath,
    expectBranch: homeBranch,
  });
  return {
    ok: result.ok,
    result,
    formatted: formatPreflightResult(result),
  };
}

function ffOnlyOriginMain(wsPath, dirtyPaths, stream, homeBranch) {
  const pre = runGitPreflight({
    op: "merge",
    cwd: wsPath,
    stream,
    expectPath: wsPath,
    expectBranch: homeBranch,
    targetRef: "origin/main",
    allowDirty: allowDirtyForPreflight(dirtyPaths),
  });
  if (!pre.ok) {
    return { ok: false, stage: "preflight-merge", preflight: pre };
  }
  try {
    runGit(wsPath, ["merge", "--ff-only", "origin/main"], { throwOnFail: true });
    const head = runGit(wsPath, ["rev-parse", "HEAD"]);
    const originMain = runGit(wsPath, ["rev-parse", "origin/main"]);
    if (head && originMain && head !== originMain) {
      return {
        ok: false,
        stage: "ff-merge",
        error: `HEAD ${head} differs from origin/main ${originMain} after ff-only`,
      };
    }
    return { ok: true, stage: "ff-merge", head, originMain };
  } catch (err) {
    return {
      ok: false,
      stage: "ff-merge",
      error: String(err?.stderr || err?.message || err),
    };
  }
}

function switchToHome(wsPath, homeBranch, stream, dirtyPaths = []) {
  const allowDirty = allowDirtyForPreflight(dirtyPaths);
  const pre = runGitPreflight({
    op: "switch",
    cwd: wsPath,
    stream,
    expectPath: wsPath,
    targetRef: homeBranch,
    allowDirty,
  });
  if (!pre.ok) {
    return { ok: false, stage: "preflight-switch", preflight: pre };
  }
  try {
    runGit(wsPath, ["switch", homeBranch], { throwOnFail: true });
    return { ok: true, stage: "switch" };
  } catch (err) {
    return {
      ok: false,
      stage: "switch",
      error: String(err?.stderr || err?.message || err),
    };
  }
}

/**
 * Apply a previously built plan. Never destructive.
 */
export function applyPostMergeWorkspaceSync(plan, opts = {}) {
  const destructive = refuseDestructiveFlags(opts);
  if (destructive) {
    return { ok: false, applied: false, dryRun: true, status: "BLOCKED", error: destructive };
  }
  if (!plan) {
    return { ok: false, applied: false, dryRun: true, status: "BLOCKED", error: "missing plan" };
  }
  if (opts.apply !== true) {
    return {
      ok: true,
      applied: false,
      dryRun: true,
      status: plan.status || (plan.ok ? "READY" : "BLOCKED"),
      plan,
      message: "dry-run only — pass --apply to mutate",
    };
  }

  const results = [];
  for (const item of plan.productPlans || []) {
    if (!item.safeToMutate) {
      results.push({
        stream: item.stream,
        action: item.action,
        applied: false,
        skipped: true,
        reason: item.reason,
      });
      continue;
    }

    const steps = [];
    const dirtyPaths = item.live?.dirtyPaths || [];
    const dirtyObjs = (item.live?.dirtyPaths || []).map((p) =>
      typeof p === "string" ? { status: " M", path: p } : p,
    );
    const overwrite = findOverwriteRisks({
      dirtyPaths: dirtyObjs,
      changedBetweenTips: item.live?.overlapPaths || [],
    });
    // overlapPaths already gates classification; re-check tracked overwrite for fail-closed apply.
    if (overwrite.length) {
      steps.push({ ok: false, stage: "overwrite-gate", overwriteRisks: overwrite });
      results.push({ stream: item.stream, action: item.action, applied: false, steps });
      continue;
    }

    if (
      item.action === SYNC_ACTIONS.RETURN_HOME_THEN_FF ||
      item.action === SYNC_ACTIONS.PRESERVE_THEN_RETURN_HOME
    ) {
      if (item.action === SYNC_ACTIONS.PRESERVE_THEN_RETURN_HOME || dirtyPaths.length) {
        try {
          const backupRoot =
            opts.backupRoot ||
            path.join("C:/Backups/Dealality", `ws-${item.stream}-post-merge`);
          const backup = createVerifiedBackup({
            root: item.path,
            dirtyPaths,
            backupRoot,
          });
          steps.push({
            ok: true,
            stage: "preserve-backup",
            backupDir: backup.backupDir,
            entryCount: backup.manifest.entries.length,
          });
        } catch (err) {
          steps.push({ ok: false, stage: "preserve-backup", error: String(err.message || err) });
          results.push({ stream: item.stream, action: item.action, applied: false, steps });
          continue;
        }
      }
      const sw = switchToHome(item.path, item.homeBranch, item.stream, dirtyPaths);
      steps.push(sw);
      if (!sw.ok) {
        results.push({ stream: item.stream, action: item.action, applied: false, steps });
        continue;
      }
      const ff = ffOnlyOriginMain(item.path, dirtyPaths, item.stream, item.homeBranch);
      steps.push(ff);
      if (!ff.ok) {
        results.push({ stream: item.stream, action: item.action, applied: false, steps });
        continue;
      }
    } else if (item.action === SYNC_ACTIONS.FAST_FORWARD_HOME) {
      const ff = ffOnlyOriginMain(item.path, dirtyPaths, item.stream, item.homeBranch);
      steps.push(ff);
      if (!ff.ok) {
        results.push({ stream: item.stream, action: item.action, applied: false, steps });
        continue;
      }
    }

    const verify = verifyPreflight(item.path, item.stream, item.homeBranch);
    steps.push({ ok: verify.ok, stage: "preflight-verify", formatted: verify.formatted });
    results.push({
      stream: item.stream,
      action: item.action,
      applied: verify.ok,
      steps,
      preflightOk: verify.ok,
    });
  }

  // Integration: only auto-FF when plan marked safe (clean main).
  if (plan.integrationPlan?.safeToMutate && plan.integrationPlan.action === SYNC_ACTIONS.FAST_FORWARD_HOME) {
    const ff = ffOnlyOriginMain(plan.integrationPlan.path, [], "integration", "main");
    const verify = verifyPreflight(plan.integrationPlan.path, "integration", "main");
    results.push({
      stream: "integration",
      action: plan.integrationPlan.action,
      applied: ff.ok && verify.ok,
      steps: [ff, { ok: verify.ok, stage: "preflight-verify", formatted: verify.formatted }],
    });
  } else if (plan.integrationPlan) {
    results.push({
      stream: "integration",
      action: plan.integrationPlan.action,
      applied: false,
      skipped: true,
      reason: plan.integrationPlan.reason,
    });
  }

  const failed = results.filter((r) => r.applied === false && !r.skipped);
  return {
    ok: failed.length === 0,
    status: failed.length === 0 ? "READY" : "BLOCKED",
    applied: true,
    dryRun: false,
    results,
  };
}

export function formatSyncPlan(plan) {
  const lines = [];
  lines.push(
    `[dealality:post-merge-sync] status=${plan.status || (plan.ok ? "READY" : "BLOCKED")} mode=${plan.mode} dryRun=${plan.dryRun} ok=${plan.ok} affected=${plan.affectedStream || "(none)"}`,
  );
  if (plan.error) lines.push(`  ERROR ${plan.error}`);
  if (plan.summary) {
    lines.push(
      `  summary actionable=${plan.summary.actionableCount} blocked=${plan.summary.blockedCount} fetchOnly=${plan.summary.fetchOnlyCount} synced=${plan.summary.alreadySyncedCount}`,
    );
  }
  for (const p of plan.productPlans || []) {
    lines.push(
      `  [${p.action}] ${p.stream} branch=${p.branch || p.live?.branch || "-"} home=${p.homeBranch || "-"} dirty=${p.live?.dirtyCount ?? p.dirtyCount ?? 0} — ${p.reason}`,
    );
  }
  if (plan.integrationPlan) {
    lines.push(
      `  [${plan.integrationPlan.action}] integration — ${plan.integrationPlan.reason}`,
    );
  }
  lines.push(`  RESULT ${plan.status || (plan.ok ? "READY" : "BLOCKED")}`);
  return lines.join("\n");
}
