/**
 * Recurring period read service (Phase 3B.6).
 * Supports latest/prior/specific period queries without UI changes.
 */

import { readBaselineFreezeMarker } from "./baseline-freeze.js";
import {
  buildBaselinePeriodReference,
  listPeriodManifests,
  readPeriodManifest,
  PERIOD_STATUS,
  PERIOD_TYPE,
} from "./recurring-period-model.js";
import { FULL_PERIOD_COMPARABILITY_RULE } from "./recurring-comparability.js";
import { buildTrendFoundation } from "./period-trend-foundation.js";
import { buildSourceChangeFoundation } from "./period-source-change-foundation.js";

export const RECURRING_PERIOD_READ_SERVICE_VERSION =
  "ai_visibility_recurring_period_read_v1";

function isFullComparablePeriod(period) {
  if (!period) return false;
  const waves = Object.values(period.providerWaves || {});
  return waves.every((w) => w.successful >= FULL_PERIOD_COMPARABILITY_RULE.METRIC_DENOMINATOR);
}

/**
 * List all known periods including baseline reference.
 */
export function listMonitoringPeriods(opts = {}) {
  const freeze = readBaselineFreezeMarker(opts.rootDir);
  const baseline = freeze ? buildBaselinePeriodReference(freeze) : null;
  const recurring = listPeriodManifests(opts.rootDir);
  const all = baseline ? [baseline, ...recurring] : recurring;
  return {
    periods: all,
    count: all.length,
    baselinePeriodId: baseline?.periodId || null,
    recurringCount: recurring.length,
  };
}

/**
 * Get latest completed comparable period.
 */
export function getLatestCompletedPeriod(opts = {}) {
  const { periods } = listMonitoringPeriods(opts);
  const completed = periods.filter(
    (p) => p.status === PERIOD_STATUS.COMPLETED && isFullComparablePeriod(p)
  );
  if (!completed.length) return null;
  return completed.sort((a, b) =>
    String(b.completedAt || b.periodId).localeCompare(String(a.completedAt || a.periodId))
  )[0];
}

/**
 * Get prior comparable period relative to a given period.
 */
export function getPriorComparablePeriod(periodId, opts = {}) {
  const { periods } = listMonitoringPeriods(opts);
  const comparable = periods.filter(isFullComparablePeriod);
  const idx = comparable.findIndex((p) => p.periodId === periodId);
  if (idx <= 0) return null;
  return comparable[idx - 1];
}

/**
 * Get specific period by ID.
 */
export function getMonitoringPeriod(periodId, opts = {}) {
  if (periodId === "aiv_monitoring_period_baseline_four_provider_v1") {
    const freeze = readBaselineFreezeMarker(opts.rootDir);
    return freeze ? buildBaselinePeriodReference(freeze) : null;
  }
  return readPeriodManifest(periodId, opts.rootDir);
}

/**
 * Build read context for brand/detail modules.
 */
export function buildPeriodReadContext(opts = {}) {
  const periodId = opts.periodId || null;
  const current = periodId
    ? getMonitoringPeriod(periodId, opts)
    : getLatestCompletedPeriod(opts);
  const prior = current ? getPriorComparablePeriod(current.periodId, opts) : null;
  const completedCount = listMonitoringPeriods(opts).periods.filter(isFullComparablePeriod)
    .length;

  return {
    version: RECURRING_PERIOD_READ_SERVICE_VERSION,
    currentPeriod: current,
    priorPeriod: prior,
    priorComparable: prior != null,
    completedComparablePeriods: completedCount,
    trend: buildTrendFoundation({ completedComparablePeriods: completedCount }),
    sourceChange: buildSourceChangeFoundation({ completedComparablePeriods: completedCount }),
    TREND_AVAILABLE: completedCount >= 2,
    SOURCE_CHANGE_AVAILABLE: completedCount >= 2,
    VISIBLE_PERIOD_SELECTOR_RECOMMENDED: false,
    VISIBLE_PERIOD_SELECTOR_WHY:
      "Default to latest completed comparable period; historical drill-down via API when needed; avoid filter clutter",
  };
}

/**
 * Filter observation rows by periodId.
 */
export function filterObservationsByPeriod(rows = [], periodId) {
  if (!periodId) return rows;
  return rows.filter(
    (r) =>
      r.periodId === periodId ||
      r.monitoringPeriodId === periodId ||
      r.batchId === periodId
  );
}
