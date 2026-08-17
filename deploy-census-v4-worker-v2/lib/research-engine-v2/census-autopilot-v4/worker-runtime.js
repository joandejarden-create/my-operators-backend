/**
 * V4 Census worker runtime — lease lock, heartbeat, controller exit contract.
 * No research/data semantics — process persistence only.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import os from "node:os";

export const CONTROLLER_EXIT = Object.freeze({
  CHECKPOINT_CONTINUE: 0,
  BUILD_COMPLETE: 10,
  WAITING_FOR_RETRY_WINDOW: 20,
  WAITING_FOR_PAID_BUDGET: 21,
  HARD_CIRCUIT_BREAK: 30,
  FATAL_CONFIGURATION: 40,
  /** legacy / unexpected */
  UNEXPECTED_FAILURE: 1,
});

export const WORKER_STATUS = Object.freeze({
  RUNNING: "RUNNING",
  WAITING: "WAITING",
  HARD_BLOCKED: "HARD_BLOCKED",
  COMPLETE: "COMPLETE",
  STOPPED_UNEXPECTEDLY: "STOPPED_UNEXPECTEDLY",
});

export function mapStopReasonToExitCode(stopReason, circuitTripped) {
  if (circuitTripped || stopReason === "HARD_CIRCUIT_BREAKER") return CONTROLLER_EXIT.HARD_CIRCUIT_BREAK;
  if (stopReason === "NO_ACTIONABLE_WORK") return CONTROLLER_EXIT.BUILD_COMPLETE;
  if (stopReason === "WAITING_TEMPORARY_BLOCK" || stopReason === "LANES_NEED_RETRY_OR_ENGINEERING") {
    return CONTROLLER_EXIT.WAITING_FOR_RETRY_WINDOW;
  }
  if (stopReason === "WAITING_FOR_PAID_BUDGET_RESET") return CONTROLLER_EXIT.WAITING_FOR_PAID_BUDGET;
  if (stopReason === "FATAL_CONFIGURATION") return CONTROLLER_EXIT.FATAL_CONFIGURATION;
  // INFRASTRUCTURE_RUNTIME_BOUNDARY and default → continue
  return CONTROLLER_EXIT.CHECKPOINT_CONTINUE;
}

export function supervisorActionForExitCode(code) {
  switch (code) {
    case CONTROLLER_EXIT.CHECKPOINT_CONTINUE:
      return { action: "restart_controller_soon", sleep_ms: 2_000 };
    case CONTROLLER_EXIT.BUILD_COMPLETE:
      return { action: "supervisor_exit_complete", sleep_ms: 0 };
    case CONTROLLER_EXIT.WAITING_FOR_RETRY_WINDOW:
      return { action: "sleep_then_retry", sleep_ms: 60_000 };
    case CONTROLLER_EXIT.WAITING_FOR_PAID_BUDGET:
      return { action: "sleep_then_retry", sleep_ms: 15 * 60_000 };
    case CONTROLLER_EXIT.HARD_CIRCUIT_BREAK:
      return { action: "supervisor_hard_block", sleep_ms: 0 };
    case CONTROLLER_EXIT.FATAL_CONFIGURATION:
      return { action: "supervisor_fatal", sleep_ms: 0 };
    default:
      return { action: "backoff_retry", sleep_ms: 30_000 };
  }
}

function ensureDir(fp) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
}

export function createWorkerId() {
  return `v4w_${os.hostname().replace(/[^a-zA-Z0-9]/g, "").slice(0, 12)}_${process.pid}_${crypto
    .randomBytes(4)
    .toString("hex")}`;
}

function writeJsonAtomic(lockPath, obj) {
  ensureDir(lockPath);
  const body = JSON.stringify(obj, null, 2);
  // Windows: rename over existing file often EPERM when AV/indexer holds handle.
  // Prefer direct overwrite; retry briefly on transient locks.
  let lastErr;
  for (let i = 0; i < 8; i++) {
    try {
      fs.writeFileSync(lockPath, body);
      return;
    } catch (err) {
      lastErr = err;
      const code = err?.code;
      if (code === "EPERM" || code === "EBUSY" || code === "EACCES") {
        const waitUntil = Date.now() + 30 + i * 25;
        while (Date.now() < waitUntil) {
          /* brief backoff */
        }
        continue;
      }
      throw err;
    }
  }
  // Fallback temp+copy
  const tmp = `${lockPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, body);
  try {
    fs.copyFileSync(tmp, lockPath);
  } finally {
    try {
      fs.unlinkSync(tmp);
    } catch {
      /* ignore */
    }
  }
  if (!fs.existsSync(lockPath)) throw lastErr;
}

/**
 * File-based exclusive lease (single active writer).
 * @param {string} lockPath
 * @param {{ worker_id: string, ttl_ms?: number, stale_ms?: number }} opts
 */
export function tryAcquireWorkerLock(lockPath, opts) {
  const now = Date.now();
  const ttl = opts.ttl_ms ?? 120_000;
  const stale = opts.stale_ms ?? 180_000;
  ensureDir(lockPath);

  let existing = null;
  if (fs.existsSync(lockPath)) {
    try {
      existing = JSON.parse(fs.readFileSync(lockPath, "utf8"));
    } catch {
      existing = null;
    }
  }

  if (existing?.worker_id && existing.worker_id !== opts.worker_id) {
    const hb = Date.parse(existing.heartbeat_at || existing.acquired_at || 0);
    const expires = Date.parse(existing.expires_at || 0);
    const fresh = Number.isFinite(hb) && now - hb < stale;
    const notExpired = Number.isFinite(expires) && now < expires;
    if (fresh && notExpired) {
      return {
        acquired: false,
        reason: "lease_held_by_other_healthy_worker",
        holder: existing,
      };
    }
  }

  const lease = {
    lock_name: "CENSUS_V4_WORKER_LOCK",
    worker_id: opts.worker_id,
    acquired_at: existing?.worker_id === opts.worker_id ? existing.acquired_at : new Date().toISOString(),
    heartbeat_at: new Date().toISOString(),
    expires_at: new Date(now + ttl).toISOString(),
    host: os.hostname(),
    pid: process.pid,
  };
  writeJsonAtomic(lockPath, lease);
  return { acquired: true, lease };
}

export function renewWorkerLock(lockPath, workerId, ttlMs = 120_000) {
  if (!fs.existsSync(lockPath)) return tryAcquireWorkerLock(lockPath, { worker_id: workerId, ttl_ms: ttlMs });
  let existing;
  try {
    existing = JSON.parse(fs.readFileSync(lockPath, "utf8"));
  } catch {
    return tryAcquireWorkerLock(lockPath, { worker_id: workerId, ttl_ms: ttlMs });
  }
  if (existing.worker_id !== workerId) {
    return { acquired: false, reason: "lost_lease", holder: existing };
  }
  existing.heartbeat_at = new Date().toISOString();
  existing.expires_at = new Date(Date.now() + ttlMs).toISOString();
  writeJsonAtomic(lockPath, existing);
  return { acquired: true, lease: existing };
}

export function releaseWorkerLock(lockPath, workerId) {
  if (!fs.existsSync(lockPath)) return;
  try {
    const existing = JSON.parse(fs.readFileSync(lockPath, "utf8"));
    if (existing.worker_id === workerId) fs.unlinkSync(lockPath);
  } catch {
    /* ignore */
  }
}

export function writeHeartbeat(heartbeatPath, payload) {
  ensureDir(heartbeatPath);
  const row = {
    ...payload,
    last_heartbeat_at: new Date().toISOString(),
  };
  fs.writeFileSync(heartbeatPath, JSON.stringify(row, null, 2));
  return row;
}

export function readJsonSafe(fp) {
  if (!fs.existsSync(fp)) return null;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
