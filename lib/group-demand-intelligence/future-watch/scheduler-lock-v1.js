/**
 * File-based concurrency lock for scheduled watch research.
 * CLAIM → PROCESS → RELEASE with stale-claim recovery.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { getGdiDataRoot } from "../repository.js";
import { SCHEDULER_DEFAULTS } from "./constants.js";

export function schedulerLockDir() {
  return path.join(getGdiDataRoot(), "_scheduler_locks");
}

export function watchClaimPath(watchId) {
  const safe = String(watchId || "unknown").replace(/[^\w.-]+/g, "_");
  return path.join(schedulerLockDir(), `${safe}.claim.json`);
}

export function globalSchedulerLockPath() {
  return path.join(schedulerLockDir(), "gdi-future-watch-scheduler.lock.json");
}

/**
 * Acquire a per-watch claim. Returns { ok, claimToken, staleRecovered }.
 */
export function claimWatch(watchId, { runId, staleClaimMs = SCHEDULER_DEFAULTS.staleClaimMs } = {}) {
  fs.mkdirSync(schedulerLockDir(), { recursive: true });
  const p = watchClaimPath(watchId);
  const now = Date.now();
  const claimToken = crypto.randomBytes(12).toString("hex");

  if (fs.existsSync(p)) {
    try {
      const existing = JSON.parse(fs.readFileSync(p, "utf8"));
      const age = now - new Date(existing.claimedAt || 0).getTime();
      if (existing.status === "CLAIMED" || existing.status === "PROCESSING") {
        if (age < staleClaimMs) {
          return {
            ok: false,
            reason: "ALREADY_CLAIMED",
            claimToken: null,
            holderRunId: existing.runId,
            staleRecovered: false,
          };
        }
        // Stale — recover
        const claim = {
          watchId,
          runId,
          claimToken,
          status: "CLAIMED",
          claimedAt: new Date().toISOString(),
          recoveredFrom: existing.runId,
          staleRecovered: true,
        };
        fs.writeFileSync(p, JSON.stringify(claim, null, 2), "utf8");
        return { ok: true, claimToken, staleRecovered: true, claim };
      }
    } catch {
      // corrupt lock — overwrite
    }
  }

  const claim = {
    watchId,
    runId,
    claimToken,
    status: "CLAIMED",
    claimedAt: new Date().toISOString(),
    staleRecovered: false,
  };
  fs.writeFileSync(p, JSON.stringify(claim, null, 2), "utf8");
  return { ok: true, claimToken, staleRecovered: false, claim };
}

export function markWatchProcessing(watchId, claimToken) {
  const p = watchClaimPath(watchId);
  if (!fs.existsSync(p)) return { ok: false, reason: "NO_CLAIM" };
  const existing = JSON.parse(fs.readFileSync(p, "utf8"));
  if (existing.claimToken !== claimToken) {
    return { ok: false, reason: "TOKEN_MISMATCH" };
  }
  existing.status = "PROCESSING";
  existing.processingAt = new Date().toISOString();
  fs.writeFileSync(p, JSON.stringify(existing, null, 2), "utf8");
  return { ok: true };
}

export function releaseWatchClaim(watchId, claimToken) {
  const p = watchClaimPath(watchId);
  if (!fs.existsSync(p)) return { ok: true, reason: "ALREADY_RELEASED" };
  try {
    const existing = JSON.parse(fs.readFileSync(p, "utf8"));
    if (claimToken && existing.claimToken !== claimToken) {
      return { ok: false, reason: "TOKEN_MISMATCH" };
    }
  } catch {
    /* remove anyway */
  }
  fs.unlinkSync(p);
  return { ok: true };
}

/**
 * Global scheduler process lock — prevent overlapping cron invocations.
 */
export function acquireGlobalSchedulerLock(runId, { staleClaimMs = SCHEDULER_DEFAULTS.staleClaimMs } = {}) {
  fs.mkdirSync(schedulerLockDir(), { recursive: true });
  const p = globalSchedulerLockPath();
  const now = Date.now();
  if (fs.existsSync(p)) {
    try {
      const existing = JSON.parse(fs.readFileSync(p, "utf8"));
      const age = now - new Date(existing.startedAt || 0).getTime();
      if (age < staleClaimMs) {
        return { ok: false, reason: "SCHEDULER_ALREADY_RUNNING", holderRunId: existing.runId };
      }
    } catch {
      /* overwrite */
    }
  }
  const lock = {
    runId,
    startedAt: new Date().toISOString(),
    pid: process.pid,
  };
  fs.writeFileSync(p, JSON.stringify(lock, null, 2), "utf8");
  return { ok: true, lock };
}

export function releaseGlobalSchedulerLock(runId) {
  const p = globalSchedulerLockPath();
  if (!fs.existsSync(p)) return { ok: true };
  try {
    const existing = JSON.parse(fs.readFileSync(p, "utf8"));
    if (existing.runId !== runId) {
      return { ok: false, reason: "NOT_HOLDER" };
    }
  } catch {
    /* remove */
  }
  fs.unlinkSync(p);
  return { ok: true };
}
