/**
 * Trend display rules + client-safe copy states.
 */

export const BRAND_LONGITUDINAL_TREND_DISPLAY_VERSION = "brand_longitudinal_trend_display_v1";

export const TREND_CLIENT_STATES = Object.freeze({
  BASELINE_ONLY: "BASELINE_ONLY",
  CURRENT_VS_PRIOR: "CURRENT_VS_PRIOR",
  EARLY_TREND: "EARLY_TREND",
  TREND: "TREND",
});

export const TREND_COPY_RULES = Object.freeze({
  BASELINE_ONLY: "Baseline measurement",
  CURRENT_VS_PRIOR: "Change since prior measurement",
  EARLY_TREND: "Early trend",
  TREND: "Trend",
  FORBIDDEN_TWO_POINT: "Trend improving",
  FORBIDDEN_TWO_POINT_DECLINING: "Trend declining",
});

/**
 * Resolve client trend state from distinct valid measurement period count.
 */
export function resolveTrendClientState(distinctValidPeriodCount) {
  const n = Number(distinctValidPeriodCount) || 0;
  if (n <= 1) return TREND_CLIENT_STATES.BASELINE_ONLY;
  if (n === 2) return TREND_CLIENT_STATES.CURRENT_VS_PRIOR;
  if (n === 3) return TREND_CLIENT_STATES.EARLY_TREND;
  if (n >= 4 && n <= 6) return TREND_CLIENT_STATES.TREND;
  if (n > 6) return TREND_CLIENT_STATES.TREND;
  return TREND_CLIENT_STATES.BASELINE_ONLY;
}

/**
 * Client-safe headline for trend section.
 */
export function trendClientCopy(distinctValidPeriodCount) {
  const state = resolveTrendClientState(distinctValidPeriodCount);
  return {
    state,
    headline: TREND_COPY_RULES[state] || TREND_COPY_RULES.BASELINE_ONLY,
    mayShowTrendLine: distinctValidPeriodCount >= 3,
    mayLabelAsTrend: distinctValidPeriodCount >= 4,
    twoPointsIsNotTrend: distinctValidPeriodCount === 2,
  };
}

/**
 * Audit existing trend UI data contract (read-only metadata).
 */
export function auditExistingTrendUiContract() {
  return {
    executiveMarketMovement: {
      dataSource: "getBrandExecutiveSummaryPayload → marketMovement",
      apiRoute: "/api/ai-visibility/brand/executive-summary",
      chartElement: "#aivMarketTrendChart",
    },
    detailPresenceOverTime: {
      dataSource: "getBrandTrendPayload → points",
      apiRoute: "/api/ai-visibility/brand/:brandId/trend",
      chartElement: "#aivDetailTrendChart",
      minPointsForChart: 2,
      renderStateOnePoint: "ONE_VALID_POINT — insufficient history message",
    },
    PLACEHOLDER_OR_SYNTHETIC: "NO",
    FAKE_INTERMEDIATE_POINTS: "NONE",
    CHANGES_REQUIRED_NOW: "None — UI already suppresses trend line below 2 points; longitudinal foundation prepares period storage only.",
  };
}
