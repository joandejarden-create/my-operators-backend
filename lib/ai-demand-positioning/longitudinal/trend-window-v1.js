/**
 * Trend window metrics integrity — each plotted point must equal certified period metrics.
 * Does not change formulas; only selects which certified periods appear.
 */

import {
  COMPARISON_MODES,
  TREND_WINDOW_HISTORY_INTEGRITY,
  toCalendarDate,
} from "./comparison-window-v1.js";

export const TREND_METRICS = Object.freeze([
  "realityCoverage",
  "scenarioPresence",
  "considerationRate",
]);

/**
 * Build trend series from certified metric history + comparison resolution.
 *
 * @param {object} args
 * @param {object} args.comparisonResolution
 * @param {Record<string, { realityCoverage:number, scenarioPresence:number, considerationRate:number }>} args.metricsByPeriodId
 */
export function buildTrendSeriesFromResolution({ comparisonResolution, metricsByPeriodId = {} }) {
  const periods = comparisonResolution?.periodsInTrendWindow || [];
  const points = [];
  const defects = [];

  for (const p of periods) {
    const m = metricsByPeriodId[p.periodId];
    if (!m) {
      defects.push({
        code: TREND_WINDOW_HISTORY_INTEGRITY,
        detail: `missing certified metrics for ${p.periodId}`,
      });
      continue;
    }
    points.push({
      periodId: p.periodId,
      calendarDate: toCalendarDate(p.executionDate || p.calendarDate),
      realityCoverage: m.realityCoverage,
      scenarioPresence: m.scenarioPresence,
      considerationRate: m.considerationRate,
    });
  }

  points.sort((a, b) => String(a.calendarDate).localeCompare(String(b.calendarDate)));

  return {
    comparisonMode: comparisonResolution?.comparisonMode || null,
    points,
    defects,
    xAxis: "ACTUAL_MONITORING_DATES",
    interpolate: false,
    fabricateMissingWeeks: false,
  };
}

/**
 * Audit trend window population vs resolution.
 */
export function auditTrendWindowIntegrity({
  comparisonResolution,
  series,
  metricsByPeriodId,
  allEligiblePeriodIds,
}) {
  const defects = [...(series?.defects || [])];
  const plotted = (series?.points || []).map((p) => p.periodId);
  const expected = (comparisonResolution?.periodsInTrendWindow || []).map((p) => p.periodId);

  if (plotted.join(",") !== expected.join(",")) {
    defects.push({
      code: TREND_WINDOW_HISTORY_INTEGRITY,
      detail: `plotted !== trend window: ${plotted.join(",")} vs ${expected.join(",")}`,
    });
  }

  // Chronological
  const dates = (series?.points || []).map((p) => p.calendarDate);
  const sorted = [...dates].sort();
  if (dates.join(",") !== sorted.join(",")) {
    defects.push({ code: TREND_WINDOW_HISTORY_INTEGRITY, detail: "points not chronological" });
  }

  // Each point equals certified metrics
  for (const pt of series?.points || []) {
    const cert = metricsByPeriodId[pt.periodId];
    if (!cert) continue;
    for (const key of TREND_METRICS) {
      if (pt[key] !== cert[key]) {
        defects.push({
          code: TREND_WINDOW_HISTORY_INTEGRITY,
          detail: `trendPoint.${key} !== certified for ${pt.periodId}`,
        });
      }
    }
  }

  // No period outside window
  const windowIds = new Set(expected);
  for (const id of plotted) {
    if (!windowIds.has(id)) {
      defects.push({
        code: TREND_WINDOW_HISTORY_INTEGRITY,
        detail: `period outside window: ${id}`,
      });
    }
  }

  // Current latest point equals current customer KPI
  const last = series?.points?.[series.points.length - 1];
  if (last && last.periodId !== comparisonResolution.currentPeriodId) {
    defects.push({
      code: TREND_WINDOW_HISTORY_INTEGRITY,
      detail: "latest trend point is not current period",
    });
  }

  // Comparison baseline must be in trend window for LAST_30 / MTD; for PRIOR_RUN it must be the first of two
  const baselineId = comparisonResolution.comparisonPeriodId;
  if (baselineId && comparisonResolution.comparability?.comparable) {
    if (!windowIds.has(baselineId) && comparisonResolution.comparisonMode !== COMPARISON_MODES.PRIOR_RUN) {
      // For 30d, baseline is nearest to anchor — must still fall inside trailing window when eligible
      // If baseline is inside candidates but outside window, that is a resolver bug
      defects.push({
        code: TREND_WINDOW_HISTORY_INTEGRITY,
        detail: `comparison baseline ${baselineId} not in trend window`,
      });
    }
    if (
      comparisonResolution.comparisonMode === COMPARISON_MODES.PRIOR_RUN &&
      plotted.length === 2 &&
      plotted[0] !== baselineId
    ) {
      defects.push({
        code: TREND_WINDOW_HISTORY_INTEGRITY,
        detail: "PRIOR_RUN trend must start with comparison baseline",
      });
    }
  }

  void allEligiblePeriodIds;
  return { pass: defects.length === 0, defects };
}
