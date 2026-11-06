/**
 * Dealality production release lock (filesystem exclusive-create).
 *
 * Cross-machine coordination requires DEALALITY_RELEASE_LOCK_ROOT to point at a
 * durable shared directory (recommended production path on the Railway volume:
 * /app/data/adp-durable-published/dealality-release-locks). A local-only root
 * cannot coordinate independent Cursor sessions on different machines.
 */
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";

export const RELEASE_STATES = Object.freeze([
  "READY",
  "WAITING_FOR_RAILWAY",
  "BACKING_UP",
  "DEPLOYING",
  "VERIFYING",
  "COMPLETE",
  "FAILED",
  "ROLLBACK_REQUIRED",
]);

export const DEFAULT_LEASE_MS = 45 * 60 * 1000;

export function defaultLocalLockRoot(cwd = process.cwd()) {
  return path.resolve(cwd, "data", "dealality-release-locks");
}

export function resolveLockRoot(env = process.env, cwd = process.cwd()) {
  const fromEnv = String(env.DEALALITY_RELEASE_LOCK_ROOT || "").trim();
  if (fromEnv) return path.resolve(fromEnv);
  return defaultLocalLockRoot(cwd);
}

/**
 * Fail closed unless the operator proves a shared durable lock root.
 * Local default paths are insufficient for cross-machine exclusivity.
 */
export function assertSharedLockRootConfigured(env = process.env, cwd = process.cwd()) {
  if (String(env.DEALALITY_RELEASE_LOCK_BYPASS || "").trim() === "1") {
    return { ok: true, bypass: true };
  }
  const explicit = String(env.DEALALITY_RELEASE_LOCK_ROOT || "").trim();
  if (!explicit) {
    return {
      ok: false,
      code: "LOCK_ROOT_REQUIRED",
      message:
        "DEALALITY_RELEASE_LOCK_ROOT must point at a durable shared directory (e.g. Railway volume dealality-release-locks). Local default is not cross-machine safe.",
    };
  }
  const resolved = path.resolve(explicit);
  const localDefault = defaultLocalLockRoot(cwd);
  const allowLocal = String(env.DEALALITY_RELEASE_LOCK_ALLOW_LOCAL || "").trim() === "1";
  const markedShared =
    String(env.DEALALITY_RELEASE_LOCK_SHARED || "").trim() === "1" ||
    fs.existsSync(path.join(resolved, "SHARED_LOCK_ROOT.ok"));
  if (
    path.normalize(resolved) === path.normalize(localDefault) &&
    !allowLocal
  ) {
    return {
      ok: false,
      code: "LOCK_ROOT_NOT_SHARED",
      message:
        "DEALALITY_RELEASE_LOCK_ROOT resolves to the local default. Set a shared durable path and DEALALITY_RELEASE_LOCK_SHARED=1 (or SHARED_LOCK_ROOT.ok), or DEALALITY_RELEASE_LOCK_ALLOW_LOCAL=1 for single-machine tests only.",
      root: resolved,
    };
  }
  if (!markedShared && !allowLocal) {
    return {
      ok: false,
      code: "LOCK_ROOT_SHARED_UNPROVEN",
      message:
        "Cross-machine exclusivity not proven. Set DEALALITY_RELEASE_LOCK_SHARED=1 after confirming the lock root is durable+shared, or create SHARED_LOCK_ROOT.ok in that directory.",
      root: resolved,
    };
  }
  return { ok: true, root: resolved, shared: markedShared || allowLocal };
}

export function lockPaths(root) {
  return {
    root,
    active: path.join(root, "ACTIVE_LOCK.json"),
    historyDir: path.join(root, "history"),
  };
}

function nowIso() {
  return new Date().toISOString();
}

function ensureRoot(root) {
  fs.mkdirSync(root, { recursive: true });
  fs.mkdirSync(path.join(root, "history"), { recursive: true });
}

export function readActiveLock(root = resolveLockRoot()) {
  const { active } = lockPaths(root);
  if (!fs.existsSync(active)) return null;
  try {
    return JSON.parse(fs.readFileSync(active, "utf8"));
  } catch (err) {
    return {
      corrupt: true,
      error: String(err && err.message ? err.message : err),
      path: active,
    };
  }
}

export function isLockExpired(lock, nowMs = Date.now()) {
  if (!lock || !lock.leaseExpiresAt) return true;
  const exp = Date.parse(lock.leaseExpiresAt);
  if (!Number.isFinite(exp)) return true;
  return exp <= nowMs;
}

export function acquireReleaseLock(opts = {}) {
  const root = path.resolve(opts.root || resolveLockRoot());
  ensureRoot(root);
  const { active, historyDir } = lockPaths(root);
  const product = String(opts.product || "").trim().toUpperCase();
  const approvedSha = String(opts.approvedSha || "").trim().toLowerCase();
  const owner =
    String(opts.owner || "").trim() ||
    `${os.hostname()}:${process.pid}:${os.userInfo().username || "user"}`;
  const leaseMs = Number(opts.leaseMs || DEFAULT_LEASE_MS);
  const releaseId =
    String(opts.releaseId || "").trim() ||
    `rel_${product || "UNK"}_${Date.now().toString(36)}_${crypto
      .randomBytes(3)
      .toString("hex")}`;

  if (!product || !["ADP", "AVA", "GDI"].includes(product)) {
    return { ok: false, code: "INVALID_PRODUCT", message: "product must be ADP|AVA|GDI" };
  }
  if (!/^[0-9a-f]{40}$/.test(approvedSha)) {
    return {
      ok: false,
      code: "INVALID_SHA",
      message: "approvedSha must be full 40-char lowercase hex",
    };
  }
  if (!opts.authorized) {
    return {
      ok: false,
      code: "AUTHORIZATION_REQUIRED",
      message: "explicit authorized=true required",
    };
  }

  if (opts.requireSharedRoot !== false) {
    const envForCheck = {
      ...(opts.env || process.env),
      DEALALITY_RELEASE_LOCK_ROOT: root,
    };
    const shared = assertSharedLockRootConfigured(
      envForCheck,
      opts.cwd || process.cwd()
    );
    if (!shared.ok) return shared;
  }

  const existing = readActiveLock(root);
  if (existing && !existing.corrupt && !isLockExpired(existing)) {
    if (
      existing.owner === owner &&
      existing.approvedSha === approvedSha &&
      existing.product === product
    ) {
      return renewReleaseLock({ root, owner, phase: existing.phase || "READY", leaseMs });
    }
    return {
      ok: false,
      code: "LOCK_HELD",
      message: "another production release holds the lock",
      lock: existing,
    };
  }

  if (existing && (existing.corrupt || isLockExpired(existing))) {
    const stamp = nowIso().replace(/[:.]/g, "-");
    const dest = path.join(
      historyDir,
      `stale-or-corrupt-${stamp}.json`
    );
    try {
      fs.renameSync(active, dest);
    } catch {
      try {
        fs.copyFileSync(active, dest);
        fs.unlinkSync(active);
      } catch {
        /* continue to wx create */
      }
    }
  }

  const lock = {
    contract: "DEALALITY_PRODUCTION_RELEASE_LOCK_V1",
    releaseId,
    product,
    approvedSha,
    owner,
    createdAt: nowIso(),
    updatedAt: nowIso(),
    leaseExpiresAt: new Date(Date.now() + leaseMs).toISOString(),
    phase: "READY",
    railwayDeploymentId: null,
    note: opts.note || null,
  };

  try {
    fs.writeFileSync(active, `${JSON.stringify(lock, null, 2)}\n`, { flag: "wx" });
  } catch (err) {
    const raced = readActiveLock(root);
    return {
      ok: false,
      code: "LOCK_RACE",
      message: String(err && err.message ? err.message : err),
      lock: raced,
    };
  }

  return { ok: true, lock, root, path: active };
}

export function renewReleaseLock(opts = {}) {
  const root = path.resolve(opts.root || resolveLockRoot());
  const { active } = lockPaths(root);
  const current = readActiveLock(root);
  if (!current || current.corrupt) {
    return { ok: false, code: "NO_LOCK", message: "no active lock to renew" };
  }
  if (current.owner !== opts.owner) {
    return { ok: false, code: "NOT_OWNER", message: "cannot renew another owner's lock", lock: current };
  }
  if (isLockExpired(current) && opts.allowExpiredRenew !== true) {
    return { ok: false, code: "LOCK_EXPIRED", message: "lock lease expired", lock: current };
  }
  const leaseMs = Number(opts.leaseMs || DEFAULT_LEASE_MS);
  const next = {
    ...current,
    updatedAt: nowIso(),
    leaseExpiresAt: new Date(Date.now() + leaseMs).toISOString(),
    phase: opts.phase || current.phase,
    railwayDeploymentId:
      opts.railwayDeploymentId !== undefined
        ? opts.railwayDeploymentId
        : current.railwayDeploymentId,
  };
  fs.writeFileSync(active, `${JSON.stringify(next, null, 2)}\n`);
  return { ok: true, lock: next, root, path: active };
}

export function updateReleasePhase(opts = {}) {
  return renewReleaseLock(opts);
}

export function releaseLock(opts = {}) {
  const root = path.resolve(opts.root || resolveLockRoot());
  const { active, historyDir } = lockPaths(root);
  const current = readActiveLock(root);
  if (!current || current.corrupt) {
    return { ok: true, code: "NO_LOCK", message: "nothing to release" };
  }
  if (opts.owner && current.owner !== opts.owner && opts.force !== true) {
    return { ok: false, code: "NOT_OWNER", message: "cannot release another owner's lock", lock: current };
  }
  ensureRoot(root);
  const stamp = nowIso().replace(/[:.]/g, "-");
  const dest = path.join(
    historyDir,
    `${current.releaseId || "lock"}_${current.phase || "END"}_${stamp}.json`
  );
  const finalLock = {
    ...current,
    phase: opts.finalPhase || current.phase || "COMPLETE",
    releasedAt: nowIso(),
    releaseReason: opts.reason || "released",
  };
  fs.writeFileSync(dest, `${JSON.stringify(finalLock, null, 2)}\n`);
  try {
    fs.unlinkSync(active);
  } catch (err) {
    return { ok: false, code: "RELEASE_FAILED", message: String(err.message || err), lock: current };
  }
  return { ok: true, archived: dest, lock: finalLock };
}

export function assertDeployAllowedByLock(env = process.env, opts = {}) {
  if (String(env.DEALALITY_RELEASE_LOCK_BYPASS || "").trim() === "1") {
    return {
      ok: true,
      bypass: true,
      message: "DEALALITY_RELEASE_LOCK_BYPASS=1",
    };
  }
  const liveDeploy = opts.liveDeploy === true;
  // Live production uploads fail closed on shared lock configuration.
  const requireLock =
    liveDeploy ||
    String(env.DEALALITY_REQUIRE_RELEASE_LOCK || "").trim() === "1";

  // Live deploys must come from GitHub Actions (cross-machine mutex).
  if (liveDeploy) {
    const channel = String(env.DEALALITY_RELEASE_CHANNEL || "")
      .trim()
      .toLowerCase();
    if (channel !== "github-actions") {
      return {
        ok: false,
        code: "RELEASE_CHANNEL_REQUIRED",
        message:
          "Live production deploy requires DEALALITY_RELEASE_CHANNEL=github-actions via workflow dealality-production-release. Local Cursor/CLI live uploads are blocked.",
      };
    }
  }

  if (requireLock) {
    const shared = assertSharedLockRootConfigured(env, opts.cwd || process.cwd());
    if (!shared.ok) return shared;
  }

  const root = resolveLockRoot(env, opts.cwd || process.cwd());
  const lock = readActiveLock(root);
  const owner =
    String(env.DEALALITY_RELEASE_OWNER || "").trim() ||
    `${os.hostname()}:${process.pid}`;

  if (!lock) {
    if (requireLock) {
      return {
        ok: false,
        code: "LOCK_REQUIRED",
        message:
          "Production deploy requires an active release lock — use Run-Dealality-Production-Release.ps1 / dealality:production-release",
        root,
      };
    }
    return { ok: true, warn: "NO_ACTIVE_LOCK", root };
  }
  if (lock.corrupt) {
    return { ok: false, code: "LOCK_CORRUPT", message: "active lock unreadable", lock };
  }
  if (isLockExpired(lock)) {
    if (requireLock) {
      return { ok: false, code: "LOCK_EXPIRED", message: "active lock lease expired", lock };
    }
    return { ok: true, warn: "LOCK_EXPIRED", lock, root };
  }
  if (lock.owner !== owner && String(env.DEALALITY_RELEASE_ID || "") !== lock.releaseId) {
    return {
      ok: false,
      code: "LOCK_HELD_BY_OTHER",
      message: `production release lock held by ${lock.owner} (${lock.product} ${lock.phase})`,
      lock,
    };
  }
  return { ok: true, lock, root };
}
