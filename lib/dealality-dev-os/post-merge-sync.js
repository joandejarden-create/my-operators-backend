/**
 * Dealality Dev OS — post-merge product workspace synchronization.
 *
 * After Integration Controller merges a PR into main, product chats run:
 *   npm run dealality:post-merge-sync -- --stream he|adp|gdi --pr <n>
 *
 * Never resets/cleans/discards dirty files. Never edits worktree-registry.json.
 * Never force-pushes. Never checks out main in product workspaces.
 */
import { execFileSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  collectDirtyPaths,
  parseAllowDirty,
  pathsAllowed,
  resolveGitRoot,
  runGitPreflight,
} from "./git-preflight.js";
import {
  findWorkspaceByStream,
  loadWorktreeRegistry,
  normalizePath,
} from "./worktree-registry.js";

export const POST_MERGE_SYNC_VERSION = "dealality_post_merge_sync_v1";

const PRODUCT_STREAMS = new Set(["he", "adp", "gdi"]);
const TASK_BRANCH_RE = /^(feat|fix|docs|research|exec|chore)\//;

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
    const stderr = String(err?.stderr || err?.message || err);
    return { __error: true, message: stderr.slice(0, 400) };
  }
}

function runGh(args, opts = {}) {
  try {
    return execFileSync("gh", args, {
      cwd: opts.cwd || process.cwd(),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, ...(opts.env || {}) },
    }).trim();
  } catch (err) {
    const stderr = String(err?.stderr || err?.message || err);
    return { __error: true, message: stderr.slice(0, 500) };
  }
}

function sha256File(filePath) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("hex");
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

/**
 * Inventory tracked + untracked dirty paths (individual files; -uall).
 */
export function inventoryDirtyPaths(root, { gitRunner = runGit } = {}) {
  const porcelain = gitRunner(root, ["status", "--porcelain=v1", "-uall"]);
  if (!porcelain || porcelain.__error) return [];
  return String(porcelain)
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      const status = line.slice(0, 2);
      let file = line.slice(3).trim();
      if (file.includes(" -> ")) file = file.split(" -> ").pop();
      return { status, path: file.replace(/^"|"$/g, "").replace(/\\/g, "/") };
    });
}

export function isProductTaskBranch(branch) {
  return TASK_BRANCH_RE.test(String(branch || ""));
}

export function detectActiveTask({
  branch,
  homeBranch,
  prHeadRef,
  aheadMain = 0,
}) {
  const b = String(branch || "");
  if (!b || b === homeBranch || b === "main") {
    return { active: false, reason: null };
  }
  if (prHeadRef && b === prHeadRef) {
    return { active: false, reason: "merged_pr_head_eligible_for_sync" };
  }
  if (isProductTaskBranch(b) && Number(aheadMain) > 0) {
    return {
      active: true,
      reason: `active task branch '${b}' is ahead of origin/main by ${aheadMain} commit(s)`,
    };
  }
  if (isProductTaskBranch(b)) {
    return {
      active: true,
      reason: `active task branch '${b}' is not the merged PR head ('${prHeadRef || "unknown"}')`,
    };
  }
  return {
    active: true,
    reason: `unexpected branch '${b}' (expected home '${homeBranch}' or merged PR head)`,
  };
}

/**
 * Dirty tracked paths that differ between current HEAD and target tip
 * would be overwritten or conflict on switch/ff.
 */
export function findOverwriteRisks({
  dirtyPaths,
  changedBetweenTips = [],
}) {
  const changed = new Set(
    (changedBetweenTips || []).map((p) => String(p).replace(/\\/g, "/"))
  );
  return (dirtyPaths || [])
    .filter((d) => {
      const p = String(d.path || d).replace(/\\/g, "/");
      const tracked = !String(d.status || "").startsWith("??");
      return tracked && changed.has(p);
    })
    .map((d) => d.path);
}

export function createVerifiedBackup({
  root,
  dirtyPaths,
  backupRoot,
  now = new Date(),
  fsApi = fs,
}) {
  const stamp = now.toISOString().replace(/[:.]/g, "-");
  const dest = path.join(
    backupRoot,
    `dealality-post-merge-preserve-${stamp}`
  );
  ensureDir(dest);
  const entries = [];
  for (const d of dirtyPaths) {
    const rel = String(d.path || d).replace(/\\/g, "/");
    const src = path.join(root, rel);
    if (!fsApi.existsSync(src)) {
      entries.push({ path: rel, missing: true });
      continue;
    }
    const target = path.join(dest, rel);
    ensureDir(path.dirname(target));
    const stat = fsApi.statSync(src);
    if (stat.isDirectory()) {
      // inventory uses -uall file paths; directories unexpected
      continue;
    }
    fsApi.copyFileSync(src, target);
    const digest = sha256File(target);
    const srcDigest = sha256File(src);
    if (digest !== srcDigest) {
      throw new Error(`backup hash mismatch for ${rel}`);
    }
    entries.push({
      path: rel,
      bytes: stat.size,
      sha256: digest,
      verified: true,
    });
  }
  const manifestPath = path.join(dest, "MANIFEST.json");
  const manifest = {
    version: POST_MERGE_SYNC_VERSION,
    createdAt: now.toISOString(),
    sourceRoot: normalizePath(root),
    entries,
  };
  fsApi.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  return { backupDir: dest, manifestPath, manifest };
}

function parsePrNumber(value) {
  const n = Number(String(value || "").replace(/^#/, ""));
  if (!Number.isInteger(n) || n <= 0) return null;
  return n;
}

/**
 * Core synchronizer. Pass `deps` to unit-test without mutating Git.
 */
export function runPostMergeSync(input = {}) {
  const errors = [];
  const warnings = [];
  const steps = [];
  const stream = String(input.stream || "").toLowerCase();
  const prNumber = parsePrNumber(input.pr);
  const dryRun = Boolean(input.dryRun);
  const apply = input.apply !== false && !dryRun;
  const deps = input.deps || {};
  const gitRunner = deps.gitRunner || runGit;
  const ghRunner = deps.ghRunner || runGh;
  const fsApi = deps.fsApi || fs;
  const cwd = path.resolve(input.cwd || process.cwd());

  const report = {
    version: POST_MERGE_SYNC_VERSION,
    status: "BLOCKED",
    ok: false,
    stream,
    pr: prNumber,
    dryRun,
    steps,
    errors,
    warnings,
    facts: {},
  };

  if (!PRODUCT_STREAMS.has(stream)) {
    errors.push(`--stream must be he|adp|gdi (got '${stream || ""}')`);
    return report;
  }
  if (!prNumber) {
    errors.push(`--pr must be a positive PR number (got '${input.pr || ""}')`);
    return report;
  }

  let root;
  try {
    root = resolveGitRoot(cwd);
  } catch (err) {
    errors.push(String(err.message || err));
    return report;
  }

  const registry = loadWorktreeRegistry(input.registryRoot || root);
  const workspace = findWorkspaceByStream(registry, stream);
  if (!workspace) {
    errors.push(`stream '${stream}' not found in worktree registry`);
    return report;
  }
  const homeBranch = workspace.defaultBranch || `ws/${stream === "he" ? "hotel-explorer" : stream}`;
  const expectedPath = normalizePath(workspace.path);
  const rootNorm = normalizePath(root);
  report.facts.homeBranch = homeBranch;
  report.facts.expectedPath = expectedPath;
  report.facts.root = rootNorm;

  if (rootNorm !== expectedPath) {
    errors.push(
      `registered path mismatch for stream '${stream}': expected ${expectedPath}, actual ${rootNorm}`
    );
    return report;
  }

  const sim = input._simulate || {};
  const branch =
    sim.branch ||
    gitRunner(root, ["branch", "--show-current"]) ||
    "(detached)";
  const head = sim.head || gitRunner(root, ["rev-parse", "HEAD"]);
  report.facts.branch = branch;
  report.facts.head = head;
  steps.push({ step: "verify_workspace", ok: true, branch, head, root: rootNorm });

  // Fetch origin/main
  if (!sim.skipFetch) {
    const fetchResult = gitRunner(root, ["fetch", "origin", "main"]);
    if (fetchResult && fetchResult.__error) {
      errors.push(`git fetch origin main failed: ${fetchResult.message}`);
      return report;
    }
  }
  steps.push({ step: "fetch_origin_main", ok: true });

  const originMain =
    sim.originMain || gitRunner(root, ["rev-parse", "origin/main"]);
  if (!originMain || originMain.__error) {
    errors.push("unable to resolve origin/main after fetch");
    return report;
  }
  report.facts.originMain = originMain;

  // Verify PR merged via GitHub
  let prInfo = sim.prInfo;
  if (!prInfo) {
    const raw = ghRunner(
      [
        "pr",
        "view",
        String(prNumber),
        "--json",
        "state,mergedAt,mergeCommit,headRefName,headRefOid,baseRefName,url",
      ],
      { cwd: root }
    );
    if (raw && raw.__error) {
      errors.push(`gh pr view failed: ${raw.message}`);
      return report;
    }
    try {
      prInfo = JSON.parse(raw);
    } catch (err) {
      errors.push(`invalid gh pr JSON: ${String(err.message || err)}`);
      return report;
    }
  }
  report.facts.pr = {
    state: prInfo.state,
    mergedAt: prInfo.mergedAt || null,
    mergeCommit: prInfo.mergeCommit?.oid || prInfo.mergeCommit || null,
    headRefName: prInfo.headRefName || null,
    headRefOid: prInfo.headRefOid || null,
    baseRefName: prInfo.baseRefName || null,
    url: prInfo.url || null,
  };
  if (String(prInfo.state || "").toUpperCase() !== "MERGED") {
    errors.push(`PR #${prNumber} is not MERGED (state=${prInfo.state || "unknown"})`);
    steps.push({ step: "verify_pr_merged", ok: false });
    return report;
  }
  if (prInfo.baseRefName && prInfo.baseRefName !== "main") {
    warnings.push(`PR base is '${prInfo.baseRefName}' (expected main)`);
  }
  const mergeSha = prInfo.mergeCommit?.oid || prInfo.mergeCommit || null;
  if (mergeSha && originMain && String(mergeSha) !== String(originMain)) {
    // origin/main may have advanced past this merge; verify ancestry when not simulated.
    let reachable = sim.mergeReachable;
    if (reachable == null && !sim.prInfo) {
      try {
        execFileSync("git", ["merge-base", "--is-ancestor", mergeSha, "origin/main"], {
          cwd: root,
          stdio: ["ignore", "pipe", "pipe"],
        });
        reachable = true;
      } catch {
        reachable = false;
      }
    }
    if (reachable === false) {
      errors.push(
        `merge commit ${mergeSha} is not an ancestor of origin/main tip ${originMain}`
      );
      steps.push({ step: "verify_pr_merged", ok: false, mergeSha });
      return report;
    }
    if (reachable === true) {
      warnings.push(
        `origin/main tip ${originMain} is ahead of merge commit ${mergeSha} (merge still reachable)`
      );
    }
  }
  steps.push({ step: "verify_pr_merged", ok: true, mergeSha, headRefName: prInfo.headRefName });

  // Dirty inventory
  const dirty =
    sim.dirty ||
    inventoryDirtyPaths(root, { gitRunner });
  report.facts.dirtyCount = dirty.length;
  report.facts.dirtyPaths = dirty.map((d) => d.path);
  steps.push({ step: "inventory_dirty", ok: true, count: dirty.length });

  // Ahead/behind vs origin/main
  let aheadMain = 0;
  let behindMain = 0;
  if (sim.aheadMain != null || sim.behindMain != null) {
    aheadMain = Number(sim.aheadMain || 0);
    behindMain = Number(sim.behindMain || 0);
  } else {
    const ab = gitRunner(root, ["rev-list", "--left-right", "--count", "origin/main...HEAD"]);
    if (!ab.__error) {
      const [behindStr, aheadStr] = String(ab || "0\t0").split(/\s+/);
      behindMain = Number(behindStr || 0);
      aheadMain = Number(aheadStr || 0);
    }
  }
  report.facts.behindMain = behindMain;
  report.facts.aheadMain = aheadMain;

  const active = detectActiveTask({
    branch,
    homeBranch,
    prHeadRef: prInfo.headRefName,
    aheadMain,
  });
  report.facts.activeTask = active;
  if (active.active) {
    errors.push(`refusing sync: ${active.reason}`);
    steps.push({ step: "active_task_gate", ok: false, reason: active.reason });
    return report;
  }
  steps.push({ step: "active_task_gate", ok: true });

  // Overwrite risk: dirty tracked files also changed between HEAD and origin/main
  const changedOnMain = Array.isArray(sim.changedOnMain)
    ? sim.changedOnMain
    : String(gitRunner(root, ["diff", "--name-only", "HEAD...origin/main"]) || "")
        .split(/\r?\n/)
        .filter(Boolean);
  const overwriteRisks = findOverwriteRisks({
    dirtyPaths: dirty,
    changedBetweenTips: changedOnMain,
  });
  report.facts.overwriteRisks = overwriteRisks;
  if (overwriteRisks.length) {
    errors.push(
      `dirty files would be overwritten by sync: ${overwriteRisks.slice(0, 12).join(", ")}`
    );
    steps.push({ step: "overwrite_gate", ok: false, overwriteRisks });
    return report;
  }
  steps.push({ step: "overwrite_gate", ok: true });

  // Refuse destructive modes
  if (input.reset || input.clean || input.discard || input.forceClean) {
    errors.push("refusing destructive flags (reset/clean/discard are prohibited)");
    return report;
  }

  // Preserve dirty artifacts externally
  let backup = null;
  if (dirty.length) {
    const backupRoot =
      input.backupRoot ||
      path.join("C:/Backups/Dealality", `ws-${stream}-post-merge`);
    if (sim.backup) {
      backup = sim.backup;
    } else if (apply) {
      try {
        backup = createVerifiedBackup({
          root,
          dirtyPaths: dirty,
          backupRoot,
          fsApi,
        });
      } catch (err) {
        errors.push(`backup failed: ${String(err.message || err)}`);
        return report;
      }
    } else {
      backup = {
        backupDir: path.join(backupRoot, "dry-run"),
        manifestPath: null,
        manifest: { dryRun: true, entries: dirty.map((d) => ({ path: d.path })) },
      };
    }
    report.facts.backup = {
      backupDir: backup.backupDir,
      manifestPath: backup.manifestPath,
      entryCount: backup.manifest?.entries?.length || 0,
    };
    steps.push({ step: "preserve_backup", ok: true, backupDir: backup.backupDir });
  } else {
    steps.push({ step: "preserve_backup", ok: true, skipped: "clean_worktree" });
  }

  const allowDirty = parseAllowDirty([
    ...(dirty.map((d) => d.path)),
    ...(Array.isArray(input.allowDirty) ? input.allowDirty : parseAllowDirty(input.allowDirty)),
  ]);
  // Ensure directory prefixes from inventory are allowlisted
  if (!pathsAllowed(dirty, allowDirty) && dirty.length) {
    errors.push(
      `internal allow-dirty mismatch after inventory (${dirty.length} paths); refusing sync`
    );
    return report;
  }

  // Switch to home branch when needed
  if (branch !== homeBranch) {
    const switchPre = runGitPreflight({
      op: "switch",
      cwd: root,
      stream,
      expectPath: expectedPath,
      targetRef: homeBranch,
      allowDirty,
      registryRoot: input.registryRoot || root,
      _simulate: sim.preflightSwitch || {
        branch,
        head,
        dirty,
        registered: workspace,
      },
    });
    if (!switchPre.ok) {
      errors.push(...switchPre.errors.map((e) => `preflight switch: ${e}`));
      steps.push({ step: "switch_home_preflight", ok: false });
      return report;
    }
    if (apply) {
      const sw = gitRunner(root, ["switch", homeBranch], { throwOnFail: false });
      if (sw && sw.__error) {
        errors.push(`git switch ${homeBranch} failed: ${sw.message}`);
        return report;
      }
    }
    steps.push({
      step: "switch_home",
      ok: true,
      from: branch,
      to: homeBranch,
      applied: apply,
    });
  } else {
    steps.push({ step: "switch_home", ok: true, skipped: "already_on_home" });
  }

  // Fast-forward home to origin/main
  const mergePre = runGitPreflight({
    op: "merge",
    cwd: root,
    stream,
    expectPath: expectedPath,
    expectBranch: homeBranch,
    targetRef: "origin/main",
    allowDirty,
    registryRoot: input.registryRoot || root,
    _simulate: sim.preflightMerge || {
      branch: homeBranch,
      head: originMain,
      dirty,
      registered: workspace,
      behindMain: 0,
      aheadMain: 0,
    },
  });
  if (!mergePre.ok) {
    errors.push(...mergePre.errors.map((e) => `preflight merge: ${e}`));
    steps.push({ step: "ff_preflight", ok: false });
    return report;
  }
  if (apply) {
    const ff = gitRunner(root, ["merge", "--ff-only", "origin/main"], {
      throwOnFail: false,
    });
    if (ff && ff.__error) {
      errors.push(`fast-forward failed: ${ff.message}`);
      return report;
    }
  }
  steps.push({ step: "fast_forward_origin_main", ok: true, applied: apply });

  // Final preflight status-check
  const finalHead =
    sim.finalHead ||
    (apply ? gitRunner(root, ["rev-parse", "HEAD"]) : originMain);
  const statusPre = runGitPreflight({
    op: "status-check",
    cwd: root,
    stream,
    expectPath: expectedPath,
    expectBranch: homeBranch,
    allowDirty,
    registryRoot: input.registryRoot || root,
    _simulate: sim.preflightFinal || {
      branch: homeBranch,
      head: finalHead,
      dirty,
      registered: workspace,
      behindMain: 0,
      aheadMain: 0,
    },
  });
  report.facts.finalPreflight = {
    ok: statusPre.ok,
    errors: statusPre.errors,
    warnings: statusPre.warnings,
    head: statusPre.facts?.head,
  };
  if (!statusPre.ok) {
    errors.push(...statusPre.errors.map((e) => `final preflight: ${e}`));
    steps.push({ step: "final_preflight", ok: false });
    return report;
  }
  steps.push({ step: "final_preflight", ok: true });

  if (finalHead && originMain && String(finalHead) !== String(originMain) && apply) {
    warnings.push(`HEAD ${finalHead} differs from origin/main ${originMain}`);
  }

  report.ok = errors.length === 0;
  report.status = report.ok ? "READY" : "BLOCKED";
  report.facts.finalHead = finalHead;
  return report;
}

export function formatPostMergeSyncReport(result) {
  const lines = [];
  lines.push(
    `[dealality:post-merge-sync] status=${result.status} ok=${result.ok} stream=${result.stream} pr=${result.pr}`
  );
  if (result.facts?.root) lines.push(`  root=${result.facts.root}`);
  if (result.facts?.branch) lines.push(`  branch=${result.facts.branch}`);
  if (result.facts?.homeBranch) lines.push(`  home=${result.facts.homeBranch}`);
  if (result.facts?.originMain) lines.push(`  origin/main=${result.facts.originMain}`);
  if (result.facts?.finalHead) lines.push(`  finalHead=${result.facts.finalHead}`);
  if (result.facts?.dirtyCount != null) lines.push(`  dirty=${result.facts.dirtyCount}`);
  if (result.facts?.backup?.backupDir) {
    lines.push(`  backup=${result.facts.backup.backupDir}`);
  }
  for (const s of result.steps || []) {
    lines.push(`  step ${s.step}: ${s.ok ? "ok" : "FAIL"}${s.skipped ? ` (${s.skipped})` : ""}`);
  }
  for (const w of result.warnings || []) lines.push(`  WARN ${w}`);
  for (const e of result.errors || []) lines.push(`  ERROR ${e}`);
  lines.push(`  RESULT ${result.status}`);
  return lines.join("\n");
}

export function postMergeSyncFromArgv(argv = process.argv.slice(2), cwd = process.cwd()) {
  const get = (name) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : "";
  };
  const has = (name) => argv.includes(name);
  return runPostMergeSync({
    cwd,
    stream: get("--stream"),
    pr: get("--pr"),
    backupRoot: get("--backup-root") || undefined,
    allowDirty: (() => {
      const vals = [];
      for (let i = 0; i < argv.length; i += 1) {
        if (argv[i] === "--allow-dirty" && argv[i + 1]) vals.push(argv[i + 1]);
      }
      return vals;
    })(),
    dryRun: has("--dry-run"),
    apply: !has("--dry-run"),
    reset: has("--reset"),
    clean: has("--clean"),
    discard: has("--discard"),
    forceClean: has("--force-clean"),
  });
}
