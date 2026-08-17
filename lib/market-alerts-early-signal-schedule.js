/**
 * Weekly Early Signal production guard (V1.2.1 / V1.3).
 * Standard RSS cadence is also weekly in V1.3 — see market-alerts-rss-schedule.js.
 */
import fs from "fs";
import path from "path";
import {
  isEarlySignalProductionEnabled,
} from "./market-alerts-early-signal-config.js";
import { runEarlySignalProductionSync } from "./market-alerts-early-signal-production.js";

const DEFAULT_INTERVAL_MINUTES = 10080; // 7 days

export function getEarlySignalIntervalMinutes() {
  const raw = parseInt(process.env.MARKET_ALERTS_EARLY_SIGNALS_INTERVAL_MINUTES || "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_INTERVAL_MINUTES;
}

export function getEarlySignalIntervalMs() {
  return getEarlySignalIntervalMinutes() * 60 * 1000;
}

export function getEarlySignalStatePath() {
  const configured = process.env.MARKET_ALERTS_EARLY_SIGNALS_STATE_PATH;
  if (configured) {
    return path.isAbsolute(configured)
      ? configured
      : path.join(process.cwd(), configured);
  }
  return path.join(process.cwd(), "data", "market-alerts-early-signals-schedule-state.json");
}

export function readEarlySignalScheduleState() {
  try {
    const raw = fs.readFileSync(getEarlySignalStatePath(), "utf8");
    const parsed = JSON.parse(raw);
    return {
      lastSuccessfulRunAt: parsed.lastSuccessfulRunAt || null,
      lastInserted: parsed.lastInserted ?? null,
      lastQualifiedNotInserted: parsed.lastQualifiedNotInserted ?? null,
    };
  } catch {
    return {
      lastSuccessfulRunAt: null,
      lastInserted: null,
      lastQualifiedNotInserted: null,
    };
  }
}

export function writeEarlySignalScheduleState(patch = {}) {
  const current = readEarlySignalScheduleState();
  const next = {
    ...current,
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  const filePath = getEarlySignalStatePath();
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(next, null, 2));
  return next;
}

/**
 * @param {number} [nowMs]
 * @returns {{ run: boolean, reason: string, lastSuccessfulRunAt?: string|null, nextEligibleAt?: string|null, intervalMinutes?: number }}
 */
export function evaluateEarlySignalSchedule(nowMs = Date.now()) {
  if (!isEarlySignalProductionEnabled()) {
    return { run: false, reason: "disabled" };
  }

  const intervalMs = getEarlySignalIntervalMs();
  const state = readEarlySignalScheduleState();
  const lastMs = state.lastSuccessfulRunAt
    ? new Date(state.lastSuccessfulRunAt).getTime()
    : 0;

  if (lastMs && Number.isFinite(lastMs) && nowMs - lastMs < intervalMs) {
    return {
      run: false,
      reason: "interval_not_elapsed",
      lastSuccessfulRunAt: state.lastSuccessfulRunAt,
      nextEligibleAt: new Date(lastMs + intervalMs).toISOString(),
      intervalMinutes: getEarlySignalIntervalMinutes(),
    };
  }

  return {
    run: true,
    reason: lastMs ? "interval_elapsed" : "never_run",
    lastSuccessfulRunAt: state.lastSuccessfulRunAt,
    intervalMinutes: getEarlySignalIntervalMinutes(),
  };
}

/**
 * Run Early Signal production only when weekly guard allows.
 * @param {{ dryRun?: boolean, force?: boolean, limit?: number }} [opts]
 */
export async function maybeRunEarlySignalProductionSync(opts = {}) {
  const dryRun = opts.dryRun === true || process.env.DRY_RUN === "true";

  if (dryRun) {
    return runEarlySignalProductionSync({ ...opts, dryRun: true });
  }

  if (!isEarlySignalProductionEnabled()) {
    return {
      ok: true,
      skipped: true,
      reason: "MARKET_ALERTS_EARLY_SIGNALS_ENABLED is not true",
    };
  }

  if (opts.force) {
    const forced = await runEarlySignalProductionSync({ ...opts, dryRun: false });
    if (forced.ok && !forced.skipped) {
      recordEarlySignalProductionRun(forced);
    }
    return forced;
  }

  const schedule = evaluateEarlySignalSchedule();
  if (!schedule.run) {
    return {
      ok: true,
      skipped: true,
      reason: schedule.reason,
      schedule,
    };
  }

  const result = await runEarlySignalProductionSync({ ...opts, dryRun: false });
  if (result.ok && !result.skipped) {
    recordEarlySignalProductionRun(result);
  }
  return { ...result, schedule };
}

/**
 * @param {{ ok?: boolean, skipped?: boolean, dryRun?: boolean, inserted?: number, audit?: { productionReady?: number } }} result
 */
export function recordEarlySignalProductionRun(result) {
  if (!result?.ok || result.skipped || result.dryRun) return null;

  const productionReady = result.audit?.productionReady ?? result.inserted ?? 0;
  const inserted = result.inserted ?? 0;

  return writeEarlySignalScheduleState({
    lastSuccessfulRunAt: new Date().toISOString(),
    lastInserted: inserted,
    lastQualifiedNotInserted: Math.max(0, productionReady - inserted),
  });
}
