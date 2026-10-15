/**
 * Durable per-hotel weekly refresh state.
 *
 * Storage: GDI_DATA_ROOT/hotels/{hotelId}/weekly-refresh/
 * Production must mount a persistent volume (or swap adapter later).
 * Do not rely on ephemeral /tmp for locks or freshness.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { hotelDir } from "../repository.js";
import {
  WEEKLY_REFRESH_CONTRACT_VERSION,
  RUN_STATUS,
} from "./weekly-refresh-contract-v1.js";

export const WEEKLY_REFRESH_STATE_VERSION = "gdi-weekly-refresh-state-v1";

export function weeklyRefreshDir(hotelId, env = process.env) {
  return path.join(hotelDir(hotelId, env), "weekly-refresh");
}

function statePath(hotelId, env) {
  return path.join(weeklyRefreshDir(hotelId, env), "state.json");
}

function lockPath(hotelId, env) {
  return path.join(weeklyRefreshDir(hotelId, env), "run.lock.json");
}

function runsDir(hotelId, env) {
  return path.join(weeklyRefreshDir(hotelId, env), "runs");
}

function emptyState(hotelId) {
  return {
    version: WEEKLY_REFRESH_STATE_VERSION,
    contractVersion: WEEKLY_REFRESH_CONTRACT_VERSION,
    hotelId,
    enabled: false,
    autoPublishEnabled: false,
    lastSuccessfulRunId: null,
    lastSuccessfulRunAt: null,
    lastSuccessfulPublicationAt: null,
    nextEligibleAt: null,
    schedulingRationale: null,
    pendingGaps: [],
    pendingRetries: [],
    segmentFreshness: {},
    accountFreshness: {},
    sourceFreshness: {},
    providerJobIds: [],
    budget: {
      reservations: [],
      estimatedSpendUsd: 0,
      actualBilledSpendUsd: null,
      perRunCapUsd: 0,
      perHotelWeeklyCapUsd: 0,
    },
    killSwitch: false,
    updatedAt: null,
  };
}

export function loadWeeklyRefreshState(hotelId, env = process.env) {
  const p = statePath(hotelId, env);
  if (!fs.existsSync(p)) return emptyState(hotelId);
  try {
    return { ...emptyState(hotelId), ...JSON.parse(fs.readFileSync(p, "utf8")) };
  } catch {
    return emptyState(hotelId);
  }
}

export function saveWeeklyRefreshState(hotelId, state, env = process.env) {
  const dir = weeklyRefreshDir(hotelId, env);
  fs.mkdirSync(dir, { recursive: true });
  const next = {
    ...emptyState(hotelId),
    ...state,
    hotelId,
    version: WEEKLY_REFRESH_STATE_VERSION,
    contractVersion: WEEKLY_REFRESH_CONTRACT_VERSION,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(statePath(hotelId, env), JSON.stringify(next, null, 2) + "\n");
  return next;
}

/**
 * Acquire per-hotel run lock. Prevents overlapping weekly runs.
 */
export function acquireWeeklyRefreshLock(
  hotelId,
  { runId, staleClaimMs = 2 * 60 * 60 * 1000, env = process.env } = {}
) {
  const dir = weeklyRefreshDir(hotelId, env);
  fs.mkdirSync(dir, { recursive: true });
  const p = lockPath(hotelId, env);
  const now = Date.now();
  const claimToken = crypto.randomBytes(12).toString("hex");

  if (fs.existsSync(p)) {
    try {
      const existing = JSON.parse(fs.readFileSync(p, "utf8"));
      const age = now - new Date(existing.claimedAt || 0).getTime();
      if (
        (existing.status === "CLAIMED" || existing.status === "RUNNING") &&
        age < staleClaimMs
      ) {
        return {
          ok: false,
          reason: "ALREADY_LOCKED",
          holderRunId: existing.runId,
          claimToken: null,
        };
      }
    } catch {
      // corrupt — overwrite
    }
  }

  const claim = {
    hotelId,
    runId,
    claimToken,
    status: "CLAIMED",
    claimedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, JSON.stringify(claim, null, 2) + "\n");
  return { ok: true, claimToken, claim };
}

export function releaseWeeklyRefreshLock(
  hotelId,
  claimToken,
  env = process.env
) {
  const p = lockPath(hotelId, env);
  if (!fs.existsSync(p)) return { ok: true, reason: "NO_LOCK" };
  try {
    const existing = JSON.parse(fs.readFileSync(p, "utf8"));
    if (claimToken && existing.claimToken !== claimToken) {
      return { ok: false, reason: "TOKEN_MISMATCH" };
    }
  } catch {
    // ignore
  }
  fs.unlinkSync(p);
  return { ok: true };
}

export function persistRunJournal(hotelId, runRecord, env = process.env) {
  const dir = runsDir(hotelId, env);
  fs.mkdirSync(dir, { recursive: true });
  const id = runRecord.runId || `run_${Date.now()}`;
  const p = path.join(dir, `${id}.json`);
  fs.writeFileSync(p, JSON.stringify(runRecord, null, 2) + "\n");
  return p;
}

/**
 * Advance freshness only for successful checks.
 */
export function advanceFreshness(state, { segmentId, accountKey, sourceKey, at } = {}) {
  const next = { ...state };
  const ts = at || new Date().toISOString();
  if (segmentId) {
    next.segmentFreshness = {
      ...(next.segmentFreshness || {}),
      [segmentId]: {
        ...(next.segmentFreshness?.[segmentId] || {}),
        lastSuccessfulCheckAt: ts,
      },
    };
  }
  if (accountKey) {
    next.accountFreshness = {
      ...(next.accountFreshness || {}),
      [accountKey]: {
        ...(next.accountFreshness?.[accountKey] || {}),
        lastSuccessfulCheckAt: ts,
      },
    };
  }
  if (sourceKey) {
    next.sourceFreshness = {
      ...(next.sourceFreshness || {}),
      [sourceKey]: {
        ...(next.sourceFreshness?.[sourceKey] || {}),
        lastSuccessfulCheckAt: ts,
      },
    };
  }
  return next;
}

/**
 * Do NOT advance freshness on failure — record retry instead.
 */
export function recordFailedCheck(state, { segmentId, accountKey, reason, at } = {}) {
  const next = { ...state };
  const entry = {
    segmentId: segmentId || null,
    accountKey: accountKey || null,
    reason: reason || "check_failed",
    failedAt: at || new Date().toISOString(),
  };
  next.pendingRetries = [...(next.pendingRetries || []), entry].slice(-100);
  return next;
}

export function markRunStatus(runRecord, status, extra = {}) {
  return {
    ...runRecord,
    status,
    ...extra,
    endedAt:
      status === RUN_STATUS.RUNNING
        ? runRecord.endedAt || null
        : extra.endedAt || new Date().toISOString(),
  };
}
