/**
 * Dealality Dev OS — Worktree Lifecycle Policy (permanent).
 *
 * Reuses worktree-registry.json + post-merge sync. Never auto-deletes.
 * Retirement requires one explicit founder approval per batch; git worktree
 * remove without --force; junctions unlinked without deleting shared targets.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  collectDirtyPaths,
  parseWorktreeList,
} from "./git-preflight.js";
import {
  findWorkspaceByPath,
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  normalizePath,
  REGISTRY_RELATIVE_PATH,
} from "./worktree-registry.js";

export const LIFECYCLE_STATUS = Object.freeze({
  PERMANENT: "PERMANENT",
  ACTIVE: "ACTIVE",
  RETIREMENT_CANDIDATE: "RETIREMENT_CANDIDATE",
  PRESERVATION_REQUIRED: "PRESERVATION_REQUIRED",
  BLOCKED: "BLOCKED",
  RETIRED: "RETIRED",
});

export const RETIREMENT_BLOCKERS = Object.freeze({
  PERMANENT_WORKSPACE: "PERMANENT_WORKSPACE",
  UNCOMMITTED_CHANGES: "UNCOMMITTED_CHANGES",
  UNIQUE_COMMITS: "UNIQUE_COMMITS",
  IGNORED_RESEARCH_ARTIFACTS: "IGNORED_RESEARCH_ARTIFACTS",
  NESTED_WORKTREES: "NESTED_WORKTREES",
  LOCKS: "LOCKS",
  ACTIVE_DEPENDENCIES: "ACTIVE_DEPENDENCIES",
  UNPROTECTED_CONFIGURATION: "UNPROTECTED_CONFIGURATION",
  MISSING_PATH: "MISSING_PATH",
  NOT_GIT_WORKTREE: "NOT_GIT_WORKTREE",
  FORCE_FLAG_REFUSED: "FORCE_FLAG_REFUSED",
  FOUNDER_APPROVAL_REQUIRED: "FOUNDER_APPROVAL_REQUIRED",
  BATCH_MISMATCH: "BATCH_MISMATCH",
});

/** Regenerable ignored paths — presence alone does not block retirement. */
const REGENERABLE_IGNORE_RE = [
  /(^|\/)node_modules(\/|$)/i,
  /(^|\/)\.cache(\/|$)/i,
  /(^|\/)tmp(\/|$)/i,
  /gitleaks-bin/i,
  /\.log$/i,
  /(^|\/)coverage(\/|$)/i,
  /(^|\/)\.turbo(\/|$)/i,
  /(^|\/)dist(\/|$)/i,
  /(^|\/)build(\/|$)/i,
];

const CONFIG_SENSITIVE_NAMES = [".env", ".env.local", ".env.production"];

/** Unique research/evidence patterns under ignored trees. */
const UNIQUE_RESEARCH_RE = [
  /research/i,
  /webhound/i,
  /exa[_-]?agent/i,
  /gold[_-]?standard/i,
  /intelligence/i,
  /preservation/i,
  /dossier/i,
  /engagements\//i,
  /full[_-]?e2e/i,
  /sot[_-]?payload/i,
  /hotel[_-]?fqo/i,
];

function runGit(cwd, args, opts = {}) {
  try {
    return execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      ...opts,
    }).trim();
  } catch (err) {
    if (opts.throwOnFail) throw err;
    return "";
  }
}

function pathExists(p) {
  try {
    fs.accessSync(p);
    return true;
  } catch {
    return false;
  }
}

function isAncestor(repoRoot, maybeAncestor, descendant) {
  try {
    execFileSync("git", ["merge-base", "--is-ancestor", maybeAncestor, descendant], {
      cwd: repoRoot,
      stdio: ["ignore", "ignore", "ignore"],
    });
    return true;
  } catch {
    return false;
  }
}

/** Windows reparse-point / junction detection (safe; never follows delete). */
export function detectJunctions(absPath) {
  const junctions = [];
  if (!pathExists(absPath)) return junctions;
  for (const name of ["node_modules"]) {
    const full = path.join(absPath, name);
    if (!pathExists(full)) continue;
    try {
      const st = fs.lstatSync(full);
      // Node treats Windows junctions as symbolic links for lstat/readlink.
      if (!st.isSymbolicLink()) continue;
      let target = null;
      try {
        target = fs.readlinkSync(full);
      } catch {
        target = null;
      }
      junctions.push({
        name,
        path: normalizePath(full),
        target: target ? normalizePath(String(target)) : null,
        attributes: "Directory, ReparsePoint",
      });
    } catch {
      // Best-effort; absence of detection must not enable destructive delete of targets.
    }
  }
  return junctions;
}

export function detectLockFiles(absPath) {
  const locks = [];
  const candidates = [
    path.join(absPath, ".git", "index.lock"),
    path.join(absPath, ".git", "HEAD.lock"),
    path.join(absPath, ".git", "worktrees"),
  ];
  // Per-worktree lock via common git file
  for (const c of [
    path.join(absPath, ".git", "index.lock"),
    path.join(absPath, ".git", "HEAD.lock"),
    path.join(absPath, ".git", "packed-refs.lock"),
  ]) {
    if (pathExists(c) && fs.statSync(c).isFile()) locks.push(normalizePath(c));
  }
  // If .git is a file (linked worktree), check main worktree lock entry
  try {
    const gitFile = path.join(absPath, ".git");
    if (pathExists(gitFile) && fs.statSync(gitFile).isFile()) {
      const content = fs.readFileSync(gitFile, "utf8");
      const m = content.match(/gitdir:\s*(.+)/i);
      if (m) {
        const gitdir = m[1].trim();
        const lock = path.join(gitdir, "locked");
        if (pathExists(lock)) locks.push(normalizePath(lock));
      }
    }
  } catch {
    /* ignore */
  }
  void candidates;
  return locks;
}

export function detectNestedWorktrees(absPath, allWorktrees) {
  const root = normalizePath(absPath).toLowerCase() + "/";
  return (allWorktrees || [])
    .map((w) => normalizePath(w.path || w))
    .filter((p) => {
      const low = p.toLowerCase();
      return low.startsWith(root) && low !== root.slice(0, -1);
    });
}

/**
 * Detect unique ignored/untracked research artifacts (not regenerable caches).
 * Directory existence of reports/data alone is NOT a blocker.
 * Fast path: only scan research/ and engagements/ (true unique dumps).
 */
function listUniqueIgnoredResearchArtifacts(absPath) {
  const found = [];
  for (const rootName of ["research", "engagements"]) {
    const rootDir = path.join(absPath, rootName);
    if (!pathExists(rootDir)) continue;
    let porcelain = "";
    try {
      porcelain = execFileSync(
        "git",
        ["status", "--porcelain=v1", "-uall", "--", rootName],
        { cwd: absPath, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000 },
      );
    } catch {
      continue;
    }
    for (const line of String(porcelain || "").split(/\r?\n/).filter(Boolean)) {
      const code = line.slice(0, 2);
      if (code !== "??" && code !== "!!" && !/^[ ?AMCDR]{2}$/.test(code)) continue;
      // Only untracked / ignored matter for "unique outside git"
      if (code !== "??" && code !== "!!") continue;
      let file = line.slice(3).trim().replace(/^"|"$/g, "");
      if (file.includes(" -> ")) file = file.split(" -> ").pop();
      const norm = file.replace(/\\/g, "/");
      if (REGENERABLE_IGNORE_RE.some((re) => re.test(norm))) continue;
      found.push(normalizePath(path.join(absPath, file)));
    }
  }
  // Also catch research-named ignored files under reports/data via name heuristic (shallow)
  for (const rootName of ["reports", "data"]) {
    const rootDir = path.join(absPath, rootName);
    if (!pathExists(rootDir)) continue;
    let entries = [];
    try {
      entries = fs.readdirSync(rootDir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const ent of entries.slice(0, 80)) {
      const name = ent.name;
      if (!UNIQUE_RESEARCH_RE.some((re) => re.test(name))) continue;
      const rel = `${rootName}/${name}`;
      let ignoredOrUntracked = false;
      try {
        execFileSync("git", ["check-ignore", "-q", rel], {
          cwd: absPath,
          stdio: ["ignore", "ignore", "ignore"],
        });
        ignoredOrUntracked = true;
      } catch {
        const tracked = runGit(absPath, ["ls-files", "--", rel]);
        ignoredOrUntracked = !tracked;
      }
      if (ignoredOrUntracked) found.push(normalizePath(path.join(absPath, rel)));
    }
  }
  return [...new Set(found)].slice(0, 50);
}

function listUnprotectedConfig(absPath) {
  const found = [];
  for (const name of CONFIG_SENSITIVE_NAMES) {
    const full = path.join(absPath, name);
    if (pathExists(full)) found.push(normalizePath(full));
  }
  return found;
}

/** Rough recoverable bytes excluding junction targets (junctions counted as 0). */
export function estimateRecoverableBytes(absPath, junctions = []) {
  const junctionSet = new Set(junctions.map((j) => normalizePath(j.path).toLowerCase()));
  let total = 0;
  const skip = new Set([".git"]);
  function walk(dir, depth) {
    if (depth > 6) return;
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of entries) {
      if (skip.has(ent.name)) continue;
      const full = path.join(dir, ent.name);
      const norm = normalizePath(full).toLowerCase();
      if (junctionSet.has(norm)) continue;
      try {
        if (ent.isSymbolicLink?.() || (ent.isDirectory() && junctionSet.has(norm))) continue;
        if (ent.isDirectory()) {
          // Skip real node_modules content counting beyond top-level size sample
          if (ent.name === "node_modules") {
            try {
              const st = fs.statSync(full);
              // If reparse, already skipped; else add a coarse dir size via PowerShell is expensive — skip deep
              if (!(st.attributes !== undefined)) {
                /* node */
              }
            } catch {
              /* */
            }
            continue;
          }
          walk(full, depth + 1);
        } else if (ent.isFile()) {
          total += fs.statSync(full).size;
        }
      } catch {
        /* */
      }
    }
  }
  if (pathExists(absPath)) walk(absPath, 0);
  // Add regenerable node_modules only when it is NOT a junction
  const nm = path.join(absPath, "node_modules");
  const nmNorm = normalizePath(nm).toLowerCase();
  if (pathExists(nm) && !junctionSet.has(nmNorm)) {
    try {
      // Coarse: sum top-level package dirs only (avoid multi-minute walks)
      for (const ent of fs.readdirSync(nm, { withFileTypes: true })) {
        const full = path.join(nm, ent.name);
        try {
          if (ent.isFile()) total += fs.statSync(full).size;
          else if (ent.isDirectory()) {
            // sample: package.json size * heuristic
            total += 2_000_000; // 2MB heuristic per package top-level
          }
        } catch {
          /* */
        }
      }
    } catch {
      /* */
    }
  }
  return total;
}

export function inferStreamFromPath(absPath, branch = "") {
  const p = normalizePath(absPath).toLowerCase();
  const b = String(branch || "").toLowerCase();
  if (p.includes("/ws-gdi") || p.includes("/wt-gdi") || /\bgdi\b/.test(b)) return "gdi";
  if (p.includes("/ws-adp") || p.includes("/wt-adp") || /\badp\b/.test(b) || /ai-visibility|ai-demand/.test(b))
    return "adp";
  if (
    p.includes("/ws-hotel-explorer") ||
    p.includes("/ws-hif") ||
    p.includes("/wt-he") ||
    /hotel-explorer|radar|hif|hpc/.test(b)
  )
    return "he";
  if (p.includes("/ws-brand-explorer") || p.includes("/wt-be") || /brand-explorer|\bbe\b/.test(b))
    return "be";
  if (p.includes("/my-operators-backend") || /chore\/dev-os|integrate\//.test(b)) return "integration";
  return "integration";
}

/**
 * Assess a single worktree path against lifecycle safety gates.
 * @returns assessment object — never mutates disk.
 */
export function assessWorktree(input = {}) {
  const {
    absPath,
    registry,
    allWorktrees = [],
    originMainSha = "",
    repoRoot = "",
    now = new Date(),
    estimateSize = false,
  } = input;

  const pathNorm = normalizePath(absPath);
  const registered = registry ? findWorkspaceByPath(registry, pathNorm) : null;
  const exists = pathExists(absPath);
  const wtMeta = (allWorktrees || []).find((w) => normalizePath(w.path) === pathNorm) || null;

  const base = {
    path: pathNorm,
    exists,
    registeredId: registered?.id || null,
    registeredLifecycle: registered?.lifecycle || null,
    stream: registered?.stream || inferStreamFromPath(pathNorm, wtMeta?.branch),
    purpose: registered?.role || registered?.purpose || null,
    owner: registered?.owner || null,
    branch: wtMeta?.branch || registered?.branch || (wtMeta?.detached ? "(detached)" : null),
    head: wtMeta?.head || null,
    associatedPr: registered?.associatedPr ?? registered?.pr ?? null,
    associatedTask: registered?.associatedTask ?? registered?.task ?? null,
    createdAt: registered?.createdAt || null,
    assessedAt: now.toISOString(),
    junctions: [],
    locks: [],
    nestedWorktrees: [],
    dirtyCount: 0,
    dirtySample: [],
    aheadMain: 0,
    behindMain: 0,
    headInOriginMain: false,
    ignoredResearchArtifacts: [],
    unprotectedConfig: [],
    blockers: [],
    lifecycleStatus: LIFECYCLE_STATUS.BLOCKED,
    recoverableBytesEstimate: 0,
    notes: [],
  };

  if (registered?.lifecycle === "permanent") {
    base.lifecycleStatus = LIFECYCLE_STATUS.PERMANENT;
    base.blockers = [RETIREMENT_BLOCKERS.PERMANENT_WORKSPACE];
    base.notes.push("Permanent product/integration home — never auto-retire");
    return base;
  }

  if (!exists) {
    base.blockers = [RETIREMENT_BLOCKERS.MISSING_PATH];
    base.lifecycleStatus = LIFECYCLE_STATUS.BLOCKED;
    return base;
  }

  if (!wtMeta) {
    base.blockers = [RETIREMENT_BLOCKERS.NOT_GIT_WORKTREE];
    base.lifecycleStatus = LIFECYCLE_STATUS.BLOCKED;
    base.notes.push("Path exists but is not in git worktree list");
    return base;
  }

  const dirty = collectDirtyPaths(absPath);
  base.dirtyCount = dirty.length;
  base.dirtySample = dirty.slice(0, 8).map((d) => d.path);

  const root = repoRoot || absPath;
  const ab = runGit(absPath, ["rev-list", "--left-right", "--count", "origin/main...HEAD"]);
  const [behindStr, aheadStr] = String(ab || "0\t0").split(/\s+/);
  base.behindMain = Number(behindStr || 0);
  base.aheadMain = Number(aheadStr || 0);
  const head = base.head || runGit(absPath, ["rev-parse", "HEAD"]);
  base.head = head;
  const mainSha = originMainSha || runGit(root, ["rev-parse", "origin/main"]);
  base.headInOriginMain = Boolean(head && mainSha && isAncestor(root, head, mainSha));

  base.junctions = detectJunctions(absPath);
  base.locks = detectLockFiles(absPath);
  base.nestedWorktrees = detectNestedWorktrees(absPath, allWorktrees);
  base.ignoredResearchArtifacts = listUniqueIgnoredResearchArtifacts(absPath);
  base.unprotectedConfig = listUnprotectedConfig(absPath);

  // Junctions to permanent homes are normal (shared node_modules) — note + safe unlink, not a blocker.
  const permanentPaths = listRegisteredWorkspaces(registry || { workspaces: [] })
    .filter((w) => w.lifecycle === "permanent")
    .map((w) => normalizePath(w.path).toLowerCase());
  for (const j of base.junctions) {
    if (j.target && permanentPaths.some((pp) => j.target.toLowerCase().startsWith(pp))) {
      base.notes.push(`Junction target under permanent home (preserve target): ${j.target}`);
    }
  }

  const blockers = [];
  if (base.dirtyCount > 0) blockers.push(RETIREMENT_BLOCKERS.UNCOMMITTED_CHANGES);
  if (base.aheadMain > 0 || !base.headInOriginMain) blockers.push(RETIREMENT_BLOCKERS.UNIQUE_COMMITS);
  if (base.ignoredResearchArtifacts.length) blockers.push(RETIREMENT_BLOCKERS.IGNORED_RESEARCH_ARTIFACTS);
  if (base.nestedWorktrees.length) blockers.push(RETIREMENT_BLOCKERS.NESTED_WORKTREES);
  if (base.locks.length || wtMeta.locked) blockers.push(RETIREMENT_BLOCKERS.LOCKS);
  if (base.unprotectedConfig.length) blockers.push(RETIREMENT_BLOCKERS.UNPROTECTED_CONFIGURATION);

  base.blockers = [...new Set(blockers)];
  base.recoverableBytesEstimate = estimateSize
    ? estimateRecoverableBytes(absPath, base.junctions)
    : 0;

  // Registry-declared active task with open purpose stays ACTIVE even if clean
  const registryStatus = String(registered?.lifecycleStatus || registered?.status || "").toUpperCase();
  if (registered?.status === "active" && base.blockers.includes(RETIREMENT_BLOCKERS.UNIQUE_COMMITS)) {
    base.lifecycleStatus = LIFECYCLE_STATUS.ACTIVE;
  } else if (
    base.blockers.includes(RETIREMENT_BLOCKERS.UNCOMMITTED_CHANGES) ||
    base.blockers.includes(RETIREMENT_BLOCKERS.IGNORED_RESEARCH_ARTIFACTS) ||
    base.blockers.includes(RETIREMENT_BLOCKERS.UNPROTECTED_CONFIGURATION)
  ) {
    base.lifecycleStatus = LIFECYCLE_STATUS.PRESERVATION_REQUIRED;
  } else if (
    base.blockers.includes(RETIREMENT_BLOCKERS.LOCKS) ||
    base.blockers.includes(RETIREMENT_BLOCKERS.NESTED_WORKTREES) ||
    base.blockers.includes(RETIREMENT_BLOCKERS.ACTIVE_DEPENDENCIES)
  ) {
    base.lifecycleStatus = LIFECYCLE_STATUS.BLOCKED;
  } else if (base.blockers.includes(RETIREMENT_BLOCKERS.UNIQUE_COMMITS)) {
    // Unique commits but clean → still active / not candidate
    base.lifecycleStatus = LIFECYCLE_STATUS.ACTIVE;
  } else if (base.blockers.length === 0 && base.headInOriginMain && base.dirtyCount === 0) {
    base.lifecycleStatus = LIFECYCLE_STATUS.RETIREMENT_CANDIDATE;
  } else {
    base.lifecycleStatus = LIFECYCLE_STATUS.BLOCKED;
  }

  // Preserve explicit RETIRED registry mark
  if (registryStatus === "RETIRED") {
    base.lifecycleStatus = LIFECYCLE_STATUS.RETIRED;
  }

  return base;
}

export function inventoryAllWorktrees(opts = {}) {
  const root = opts.repoRoot || opts.registryRoot;
  if (!root) throw new Error("inventoryAllWorktrees requires repoRoot/registryRoot");
  const registry = opts.registry || loadWorktreeRegistry(root);
  const allWorktrees = parseWorktreeList(root);
  const originMainSha = runGit(root, ["rev-parse", "origin/main"]);
  const now = opts.now || new Date();
  const rows = allWorktrees.map((wt) =>
    assessWorktree({
      absPath: wt.path,
      registry,
      allWorktrees,
      originMainSha,
      repoRoot: root,
      now,
      estimateSize: Boolean(opts.estimateSize),
    }),
  );

  const byStatus = rows.reduce((acc, r) => {
    acc[r.lifecycleStatus] = (acc[r.lifecycleStatus] || 0) + 1;
    return acc;
  }, {});

  return {
    inventoriedAt: now.toISOString(),
    originMain: originMainSha,
    worktreeCount: rows.length,
    registeredPermanent: (registry.workspaces || []).length,
    registeredTemporary: (registry.temporaryWorkspaces || []).length,
    byStatus,
    rows,
  };
}

/**
 * After a PR merges into main: assess temporary worktrees tied to that PR/branch.
 * Marks eligible rows RETIREMENT_CANDIDATE. Never deletes.
 */
export function assessTemporaryWorktreesAfterMerge(opts = {}) {
  const root = opts.repoRoot || opts.registryRoot;
  const registry = opts.registry || loadWorktreeRegistry(root);
  const mergedBranch = String(opts.mergedBranch || "").replace(/^refs\/heads\//, "");
  const pr = opts.pr != null && opts.pr !== "" ? Number(opts.pr) : null;
  const allWorktrees = parseWorktreeList(root);
  const originMainSha = runGit(root, ["rev-parse", "origin/main"]);
  const now = opts.now || new Date();

  const temps = (registry.temporaryWorkspaces || []).filter((t) => t.lifecycle === "temporary");
  const matched = [];
  for (const t of temps) {
    const branchMatch =
      mergedBranch &&
      (t.branch === mergedBranch ||
        (t.expectedBranchPattern && new RegExp(t.expectedBranchPattern).test(mergedBranch)));
    const prMatch = pr != null && Number(t.associatedPr || t.pr) === pr;
    const live = allWorktrees.find((w) => normalizePath(w.path) === normalizePath(t.path));
    const liveBranchMatch = mergedBranch && live?.branch === mergedBranch;
    if (!(branchMatch || prMatch || liveBranchMatch)) continue;

    const assessment = assessWorktree({
      absPath: t.path,
      registry,
      allWorktrees,
      originMainSha,
      repoRoot: root,
      now,
    });
    matched.push({
      id: t.id,
      path: normalizePath(t.path),
      matchedBy: branchMatch || liveBranchMatch ? "branch" : "pr",
      assessment,
      priorLifecycleStatus: t.lifecycleStatus || t.status || null,
      nextLifecycleStatus: assessment.lifecycleStatus,
    });
  }

  let registryUpdated = false;
  if (opts.writeRegistry === true && matched.length) {
    const next = structuredClone
      ? structuredClone(registry)
      : JSON.parse(JSON.stringify(registry));
    for (const m of matched) {
      const entry = (next.temporaryWorkspaces || []).find((t) => t.id === m.id);
      if (!entry) continue;
      if (m.nextLifecycleStatus === LIFECYCLE_STATUS.RETIREMENT_CANDIDATE) {
        entry.lifecycleStatus = LIFECYCLE_STATUS.RETIREMENT_CANDIDATE;
        entry.status = "retirement_candidate";
        entry.lastAssessedAt = now.toISOString();
        entry.lastAssessedPr = pr;
        entry.lastAssessedBranch = mergedBranch || null;
      } else if (
        m.nextLifecycleStatus === LIFECYCLE_STATUS.PRESERVATION_REQUIRED ||
        m.nextLifecycleStatus === LIFECYCLE_STATUS.BLOCKED
      ) {
        entry.lifecycleStatus = m.nextLifecycleStatus;
        entry.lastAssessedAt = now.toISOString();
        entry.lastAssessedPr = pr;
        entry.statusNotes = `Post-merge assess: ${m.assessment.blockers.join(",")}`;
      }
    }
    next.updatedAt = now.toISOString();
    next.updatedBy = "dealality-worktree-lifecycle-v1/post-merge-assess";
    const filePath = path.join(root, REGISTRY_RELATIVE_PATH);
    fs.writeFileSync(filePath, `${JSON.stringify(next, null, 2)}\n`, "utf8");
    registryUpdated = true;
  }

  return {
    assessedAt: now.toISOString(),
    mergedBranch: mergedBranch || null,
    pr,
    matchedCount: matched.length,
    retirementCandidates: matched.filter(
      (m) => m.nextLifecycleStatus === LIFECYCLE_STATUS.RETIREMENT_CANDIDATE,
    ).length,
    matched,
    registryUpdated,
    deleted: false,
    note: "Assessment only — no worktrees deleted",
  };
}

function riskRank(status) {
  switch (status) {
    case LIFECYCLE_STATUS.RETIREMENT_CANDIDATE:
      return 1;
    case LIFECYCLE_STATUS.BLOCKED:
      return 2;
    case LIFECYCLE_STATUS.PRESERVATION_REQUIRED:
      return 3;
    case LIFECYCLE_STATUS.ACTIVE:
      return 4;
    case LIFECYCLE_STATUS.PERMANENT:
      return 5;
    default:
      return 9;
  }
}

/**
 * Build a prioritized retirement proposal (no deletes).
 * Groups by risk + recoverable storage.
 */
export function buildRetirementProposal(inventory, opts = {}) {
  const now = opts.now || new Date();
  const batchId =
    opts.batchId ||
    `lifecycle-retire-${now.toISOString().slice(0, 10).replace(/-/g, "")}-${String(now.getTime()).slice(-6)}`;

  const inDealalityRoot = (p) =>
    normalizePath(p).toLowerCase().startsWith("c:/dev/dealality/");

  const candidates = (inventory.rows || []).filter(
    (r) =>
      r.lifecycleStatus === LIFECYCLE_STATUS.RETIREMENT_CANDIDATE && inDealalityRoot(r.path),
  );
  const preservation = (inventory.rows || []).filter(
    (r) => r.lifecycleStatus === LIFECYCLE_STATUS.PRESERVATION_REQUIRED,
  );
  const blocked = (inventory.rows || []).filter(
    (r) => r.lifecycleStatus === LIFECYCLE_STATUS.BLOCKED,
  );
  const active = (inventory.rows || []).filter(
    (r) => r.lifecycleStatus === LIFECYCLE_STATUS.ACTIVE,
  );
  const permanent = (inventory.rows || []).filter(
    (r) => r.lifecycleStatus === LIFECYCLE_STATUS.PERMANENT,
  );

  // Optionally fill size estimates for candidates only (avoid walking all 100+ trees).
  if (opts.estimateCandidateSize !== false) {
    for (const r of candidates) {
      if (!r.recoverableBytesEstimate) {
        r.recoverableBytesEstimate = estimateRecoverableBytes(r.path, r.junctions || []);
      }
    }
  }

  const sortedCandidates = [...candidates].sort(
    (a, b) => (b.recoverableBytesEstimate || 0) - (a.recoverableBytesEstimate || 0),
  );

  // Near-candidates: clean + HEAD in main, only soft blockers (.env / research) — need preservation ack
  const nearCandidates = preservation
    .filter(
      (r) =>
        inDealalityRoot(r.path) &&
        r.headInOriginMain &&
        r.dirtyCount === 0 &&
        r.aheadMain === 0 &&
        !(r.locks || []).length &&
        !(r.nestedWorktrees || []).length &&
        (r.blockers || []).every((b) =>
          [
            RETIREMENT_BLOCKERS.UNPROTECTED_CONFIGURATION,
            RETIREMENT_BLOCKERS.IGNORED_RESEARCH_ARTIFACTS,
          ].includes(b),
        ),
    )
    .sort((a, b) => (b.recoverableBytesEstimate || 0) - (a.recoverableBytesEstimate || 0));

  // Historical cleanup queue: candidates first, then in-main trees with ≤3 dirty report/tmp-only paths
  const lightDirtyInMain = preservation
    .filter((r) => {
      if (!inDealalityRoot(r.path) || !r.headInOriginMain || r.aheadMain > 0) return false;
      if ((r.locks || []).length || (r.nestedWorktrees || []).length) return false;
      if (r.dirtyCount === 0 || r.dirtyCount > 3) return false;
      const sample = r.dirtySample || [];
      return sample.every(
        (p) =>
          /^(reports|tmp|data)\//i.test(String(p).replace(/\\/g, "/")) ||
          /\.log(\.|$)/i.test(String(p)),
      );
    })
    .sort((a, b) => a.dirtyCount - b.dirtyCount);

  // Default next batch: top N clean candidates by recoverable estimate (cap 8).
  // If none, propose near-candidates as PRESERVATION_THEN_RETIRE (still no delete without approval).
  const batchLimit = opts.batchLimit ?? 8;
  const sourceRows =
    sortedCandidates.length > 0 ? sortedCandidates.slice(0, batchLimit) : nearCandidates.slice(0, batchLimit);
  const proposedBatch = sourceRows.map((r, i) => ({
    order: i + 1,
    path: r.path,
    registeredId: r.registeredId,
    stream: r.stream,
    branch: r.branch,
    head: r.head,
    junctions: r.junctions,
    recoverableBytesEstimate: r.recoverableBytesEstimate,
    risk: sortedCandidates.length > 0 ? "LOW" : "LOW_AFTER_PRESERVATION_ACK",
    blockers: r.blockers || [],
    unprotectedConfig: r.unprotectedConfig || [],
    ignoredResearchArtifacts: (r.ignoredResearchArtifacts || []).slice(0, 10),
  }));

  const approvalPhrase = `AUTHORIZE_WORKTREE_LIFECYCLE_RETIRE_${batchId.toUpperCase().replace(/[^A-Z0-9]+/g, "_")}`;

  return {
    schemaVersion: 1,
    batchId,
    createdAt: now.toISOString(),
    originMain: inventory.originMain,
    worktreeCount: inventory.worktreeCount,
    summary: {
      PERMANENT: permanent.length,
      ACTIVE: active.length,
      RETIREMENT_CANDIDATE: candidates.length,
      PRESERVATION_REQUIRED: preservation.length,
      BLOCKED: blocked.length,
    },
    groups: {
      lowRiskRetirementCandidates: sortedCandidates.map((r) => ({
        path: r.path,
        stream: r.stream,
        branch: r.branch,
        registeredId: r.registeredId,
        recoverableBytesEstimate: r.recoverableBytesEstimate,
        junctions: r.junctions?.length || 0,
      })),
      lowRiskAfterPreservationAck: nearCandidates.map((r) => ({
        path: r.path,
        stream: r.stream,
        branch: r.branch,
        blockers: r.blockers,
        unprotectedConfig: r.unprotectedConfig,
        ignoredResearchArtifacts: (r.ignoredResearchArtifacts || []).slice(0, 10),
        recoverableBytesEstimate: r.recoverableBytesEstimate,
        junctions: r.junctions?.length || 0,
      })),
      preservationRequired: preservation.map((r) => ({
        path: r.path,
        blockers: r.blockers,
        dirtyCount: r.dirtyCount,
        ignoredResearchArtifacts: r.ignoredResearchArtifacts?.length || 0,
        unprotectedConfig: r.unprotectedConfig?.length || 0,
      })),
      blocked: blocked.map((r) => ({
        path: r.path,
        blockers: r.blockers,
      })),
      active: active.map((r) => ({
        path: r.path,
        branch: r.branch,
        aheadMain: r.aheadMain,
        dirtyCount: r.dirtyCount,
      })),
      permanent: permanent.map((r) => ({
        path: r.path,
        registeredId: r.registeredId,
        stream: r.stream,
      })),
    },
    proposedBatch,
    proposedBatchRecoverableBytesEstimate: proposedBatch.reduce(
      (s, r) => s + (r.recoverableBytesEstimate || 0),
      0,
    ),
    recommendedNextHistoricalBatch: [
      ...sortedCandidates.slice(0, batchLimit),
      ...nearCandidates,
      ...lightDirtyInMain,
    ]
      .filter((r, i, arr) => arr.findIndex((x) => x.path === r.path) === i)
      .slice(0, batchLimit)
      .map((r, i) => ({
        order: i + 1,
        path: r.path,
        lifecycleStatus: r.lifecycleStatus,
        risk:
          r.lifecycleStatus === LIFECYCLE_STATUS.RETIREMENT_CANDIDATE
            ? "LOW"
            : r.dirtyCount === 0
              ? "LOW_AFTER_PRESERVATION_ACK"
              : "MEDIUM_AFTER_REPORT_PRESERVE",
        dirtyCount: r.dirtyCount,
        dirtySample: (r.dirtySample || []).slice(0, 5),
        blockers: r.blockers || [],
        unprotectedConfig: r.unprotectedConfig || [],
        junctions: r.junctions?.length || 0,
        branch: r.branch,
        head: r.head,
      })),
    founderApproval: {
      required: true,
      phrase: approvalPhrase,
      instructions:
        "Reply with the exact approval phrase to authorize THIS batch only. No worktrees are deleted until retire --apply runs with that phrase.",
    },
    safety: {
      neverAutoDelete: true,
      neverForceWorktreeRemove: true,
      preserveJunctionTargets: true,
      preservePermanentWorkspaces: true,
      manageBranchesSeparately: true,
    },
    deleted: false,
  };
}

/** Junction-safe unlink: remove the reparse point only (never recursive delete of target). */
export function unlinkJunctionEntry(junctionPath) {
  const p = junctionPath;
  if (!pathExists(p)) return { ok: true, skipped: true, reason: "missing" };
  if (process.platform === "win32") {
    try {
      execFileSync("cmd.exe", ["/c", "rmdir", p], {
        stdio: ["ignore", "pipe", "pipe"],
        encoding: "utf8",
      });
      return { ok: !pathExists(p), method: "cmd-rmdir" };
    } catch (err) {
      return { ok: false, error: String(err.message || err), method: "cmd-rmdir" };
    }
  }
  try {
    fs.rmdirSync(p);
    return { ok: true, method: "rmdir" };
  } catch (err) {
    return { ok: false, error: String(err.message || err) };
  }
}

/**
 * Retire an approved batch. Never uses --force. Stops on exception; records partial completion.
 */
export function retireApprovedBatch(opts = {}) {
  const {
    repoRoot,
    proposal,
    founderApprovalPhrase,
    apply = false,
    force = false,
    now = new Date(),
  } = opts;

  if (force) {
    return {
      ok: false,
      applied: false,
      status: "BLOCKED",
      error: RETIREMENT_BLOCKERS.FORCE_FLAG_REFUSED,
      note: "git worktree remove --force is refused by lifecycle policy",
    };
  }

  if (!proposal?.batchId || !Array.isArray(proposal.proposedBatch)) {
    return { ok: false, applied: false, status: "BLOCKED", error: "missing proposal" };
  }

  const expected = proposal.founderApproval?.phrase;
  if (!founderApprovalPhrase || founderApprovalPhrase !== expected) {
    return {
      ok: false,
      applied: false,
      status: "BLOCKED",
      error: RETIREMENT_BLOCKERS.FOUNDER_APPROVAL_REQUIRED,
      expectedPhrase: expected,
      note: "Exact founder approval phrase required for this batch",
    };
  }

  const registry = loadWorktreeRegistry(repoRoot);
  const allWorktrees = parseWorktreeList(repoRoot);
  const originMainSha = runGit(repoRoot, ["rev-parse", "origin/main"]);
  const results = [];

  for (const item of proposal.proposedBatch) {
    const reassessment = assessWorktree({
      absPath: item.path,
      registry,
      allWorktrees,
      originMainSha,
      repoRoot,
      now,
    });

    if (reassessment.lifecycleStatus !== LIFECYCLE_STATUS.RETIREMENT_CANDIDATE) {
      results.push({
        path: item.path,
        ok: false,
        applied: false,
        status: "BLOCKED",
        lifecycleStatus: reassessment.lifecycleStatus,
        blockers: reassessment.blockers,
        note: "Reassessment failed — stopped batch (partial completion preserved)",
      });
      break;
    }

    if (!apply) {
      results.push({
        path: item.path,
        ok: true,
        applied: false,
        dryRun: true,
        junctions: reassessment.junctions,
        plan: ["unlink-junctions", "git worktree remove (no --force)", "update registry"],
      });
      continue;
    }

    const steps = [];
    for (const j of reassessment.junctions || []) {
      const u = unlinkJunctionEntry(j.path);
      steps.push({ stage: "unlink-junction", ...u, junction: j });
      if (!u.ok) {
        results.push({
          path: item.path,
          ok: false,
          applied: false,
          status: "BLOCKED",
          steps,
          note: "Junction unlink failed — stopped batch",
        });
        return {
          ok: false,
          applied: true,
          partial: true,
          status: "PARTIAL",
          batchId: proposal.batchId,
          founderApprovalPhrase,
          retiredAt: now.toISOString(),
          results,
        };
      }
    }

    try {
      execFileSync("git", ["worktree", "remove", item.path], {
        cwd: repoRoot,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      });
      steps.push({ stage: "worktree-remove", ok: true, force: false });
    } catch (err) {
      steps.push({
        stage: "worktree-remove",
        ok: false,
        error: String(err.stderr || err.message || err),
      });
      results.push({
        path: item.path,
        ok: false,
        applied: false,
        status: "BLOCKED",
        steps,
        note: "git worktree remove failed (no --force) — stopped batch",
      });
      return {
        ok: false,
        applied: true,
        partial: true,
        status: "PARTIAL",
        batchId: proposal.batchId,
        results,
      };
    }

    // Update registry entry if present
    const entry = (registry.temporaryWorkspaces || []).find(
      (t) => normalizePath(t.path) === normalizePath(item.path),
    );
    if (entry) {
      entry.lifecycleStatus = LIFECYCLE_STATUS.RETIRED;
      entry.status = "retired";
      entry.retirement = {
        retiredAt: now.toISOString(),
        batchId: proposal.batchId,
        pr: proposal.pr || null,
        commit: originMainSha,
        preservationEvidence: proposal.preservationEvidence || null,
        result: "RETIRED",
      };
    }

    results.push({
      path: item.path,
      ok: true,
      applied: true,
      status: "RETIRED",
      steps,
      registeredId: item.registeredId || entry?.id || null,
    });
  }

  if (apply) {
    registry.updatedAt = now.toISOString();
    registry.updatedBy = "dealality-worktree-lifecycle-v1/retire";
    fs.writeFileSync(
      path.join(repoRoot, REGISTRY_RELATIVE_PATH),
      `${JSON.stringify(registry, null, 2)}\n`,
      "utf8",
    );
  }

  const failed = results.some((r) => !r.ok);
  const anyApplied = results.some((r) => r.applied);
  return {
    ok: !failed,
    applied: apply && anyApplied,
    partial: failed && anyApplied,
    status: !apply ? "DRY_RUN" : failed ? "PARTIAL" : "COMPLETE",
    batchId: proposal.batchId,
    founderApprovalPhrase,
    retiredAt: apply ? now.toISOString() : null,
    originMain: originMainSha,
    results,
    branchesUntouched: true,
    permanentWorkspacesUntouched: true,
  };
}

export function formatInventorySummary(inventory) {
  const lines = [];
  lines.push(
    `[dealality:worktree-lifecycle] inventory worktrees=${inventory.worktreeCount} origin/main=${inventory.originMain}`,
  );
  lines.push(`  byStatus=${JSON.stringify(inventory.byStatus)}`);
  const order = [
    LIFECYCLE_STATUS.PERMANENT,
    LIFECYCLE_STATUS.ACTIVE,
    LIFECYCLE_STATUS.RETIREMENT_CANDIDATE,
    LIFECYCLE_STATUS.PRESERVATION_REQUIRED,
    LIFECYCLE_STATUS.BLOCKED,
  ];
  for (const status of order) {
    const rows = (inventory.rows || []).filter((r) => r.lifecycleStatus === status);
    if (!rows.length) continue;
    lines.push(`  --- ${status} (${rows.length}) ---`);
    for (const r of rows.slice(0, 25)) {
      lines.push(
        `    ${r.path} | ${r.branch || "-"} | dirty=${r.dirtyCount} ahead=${r.aheadMain} | blockers=${(r.blockers || []).join(",") || "-"}`,
      );
    }
    if (rows.length > 25) lines.push(`    … ${rows.length - 25} more`);
  }
  void riskRank;
  return lines.join("\n");
}
