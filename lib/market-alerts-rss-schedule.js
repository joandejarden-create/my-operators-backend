/**
 * Weekly Standard RSS production guard (Market Alerts V1.3).
 * Early Signals use market-alerts-early-signal-schedule.js separately.
 */
import fs from "fs";
import path from "path";
import { runMarketAlertsRssSync } from "../api/run-market-alerts-rss-sync.js";

const DEFAULT_INTERVAL_MINUTES = 10080; // 7 days

export function getRssSyncIntervalMinutes() {
  const raw = parseInt(process.env.MARKET_ALERTS_RSS_SYNC_INTERVAL_MINUTES || "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_INTERVAL_MINUTES;
}

export function getRssSyncIntervalMs() {
  return getRssSyncIntervalMinutes() * 60 * 1000;
}

export function getRssScheduleStatePath() {
  const configured = process.env.MARKET_ALERTS_RSS_SCHEDULE_STATE_PATH;
  if (configured) {
    return path.isAbsolute(configured)
      ? configured
      : path.join(process.cwd(), configured);
  }
  return path.join(process.cwd(), "data", "market-alerts-rss-schedule-state.json");
}

export function readRssScheduleState() {
  try {
    const raw = fs.readFileSync(getRssScheduleStatePath(), "utf8");
    const parsed = JSON.parse(raw);
    return {
      lastSuccessfulRunAt: parsed.lastSuccessfulRunAt || null,
      lastCreated: parsed.lastCreated ?? null,
    };
  } catch {
    return { lastSuccessfulRunAt: null, lastCreated: null };
  }
}

export function writeRssScheduleState(patch = {}) {
  const current = readRssScheduleState();
  const next = {
    ...current,
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  const filePath = getRssScheduleStatePath();
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(next, null, 2));
  return next;
}

/**
 * @param {number} [nowMs]
 */
export function evaluateRssSchedule(nowMs = Date.now()) {
  if (process.env.MARKET_ALERTS_RSS_SYNC_ENABLED !== "true") {
    return { run: false, reason: "disabled" };
  }

  const intervalMs = getRssSyncIntervalMs();
  const state = readRssScheduleState();
  const lastMs = state.lastSuccessfulRunAt
    ? new Date(state.lastSuccessfulRunAt).getTime()
    : 0;

  if (lastMs && Number.isFinite(lastMs) && nowMs - lastMs < intervalMs) {
    return {
      run: false,
      reason: "interval_not_elapsed",
      lastSuccessfulRunAt: state.lastSuccessfulRunAt,
      nextEligibleAt: new Date(lastMs + intervalMs).toISOString(),
      intervalMinutes: getRssSyncIntervalMinutes(),
    };
  }

  return {
    run: true,
    reason: lastMs ? "interval_elapsed" : "never_run",
    lastSuccessfulRunAt: state.lastSuccessfulRunAt,
    intervalMinutes: getRssSyncIntervalMinutes(),
  };
}

/**
 * @param {{ dryRun?: boolean, force?: boolean, limit?: number }} [opts]
 */
export async function maybeRunMarketAlertsRssSync(opts = {}) {
  const dryRun = opts.dryRun === true || process.env.DRY_RUN === "true";

  if (dryRun) {
    return runMarketAlertsRssSync({ ...opts, dryRun: true });
  }

  if (process.env.MARKET_ALERTS_RSS_SYNC_ENABLED !== "true") {
    return {
      ok: true,
      skipped: true,
      reason: "MARKET_ALERTS_RSS_SYNC_ENABLED is not true",
    };
  }

  if (opts.force) {
    const forced = await runMarketAlertsRssSync({ ...opts, dryRun: false });
    if (forced && !forced.dryRun) {
      recordRssSyncRun(forced);
    }
    return forced;
  }

  const schedule = evaluateRssSchedule();
  if (!schedule.run) {
    return {
      ok: true,
      skipped: true,
      reason: schedule.reason,
      schedule,
    };
  }

  const result = await runMarketAlertsRssSync({ ...opts, dryRun: false });
  recordRssSyncRun(result);
  return { ...result, schedule };
}

/**
 * @param {{ created?: number, dryRun?: boolean }} result
 */
export function recordRssSyncRun(result) {
  if (!result || result.dryRun) return null;
  return writeRssScheduleState({
    lastSuccessfulRunAt: new Date().toISOString(),
    lastCreated: result.created ?? 0,
  });
}
