/**
 * Trend calculation foundation (Phase 3B.6).
 * Prepares deterministic period-to-period metric deltas — no calculation until Period 2 exists.
 */

export const PERIOD_TREND_FOUNDATION_VERSION = "ai_visibility_period_trend_foundation_v1";

export const TREND_METRICS = Object.freeze([
  "AI_PRESENCE",
  "RECOMMENDATION_RATE",
  "RECOMMENDATION_SHARE",
  "TOP3_RECOMMENDATION_RATE",
  "FIRST_RECOMMENDATION_RATE",
  "COMPETITIVE_POSITION",
  "QUESTIONS_WON",
  "QUESTIONS_MISSING",
  "OWNER_DECISION_VISIBILITY",
]);

/**
 * Allowed factual change labels — no significance thresholds.
 */
export const TREND_CHANGE_LABELS = Object.freeze({
  INCREASED: "increased",
  DECREASED: "decreased",
  UNCHANGED: "unchanged",
  APPEARED: "appeared",
  NO_LONGER_APPEARED: "no_longer_appeared",
});

export const SIGNIFICANCE_LOGIC = Object.freeze({
  IMPLEMENTED: false,
  RULE: "No arbitrary significance thresholds in Phase 3B.6; factual labels only when deterministic comparison supports them",
});

/**
 * Compute factual change label between two numeric values.
 * @param {number|null} current
 * @param {number|null} prior
 */
export function labelNumericChange(current, prior) {
  if (current == null && prior == null) return TREND_CHANGE_LABELS.UNCHANGED;
  if (current != null && prior == null) return TREND_CHANGE_LABELS.APPEARED;
  if (current == null && prior != null) return TREND_CHANGE_LABELS.NO_LONGER_APPEARED;
  if (current > prior) return TREND_CHANGE_LABELS.INCREASED;
  if (current < prior) return TREND_CHANGE_LABELS.DECREASED;
  return TREND_CHANGE_LABELS.UNCHANGED;
}

/**
 * Check if trend calculation is available given period count.
 * @param {number} completedComparablePeriods
 */
export function isTrendAvailable(completedComparablePeriods) {
  return completedComparablePeriods >= 2;
}

/**
 * Build trend foundation metadata (no actual trend values).
 */
export function buildTrendFoundation(opts = {}) {
  const completedPeriods = opts.completedComparablePeriods ?? 1;
  const available = isTrendAvailable(completedPeriods);

  return {
    version: PERIOD_TREND_FOUNDATION_VERSION,
    TREND_CALCULATION_FOUNDATION_READY: true,
    TREND_AVAILABLE: available,
    AVAILABLE_NOW: available,
    completedComparablePeriods: completedPeriods,
    metrics: TREND_METRICS,
    changeLabels: { ...TREND_CHANGE_LABELS },
    significance: { ...SIGNIFICANCE_LOGIC },
    requiresFullPeriod: "84/84 successful governed observations per provider-period",
    note: available
      ? "Trend calculation may proceed when prior and current periods are comparable"
      : "Second comparable period required before trend calculation",
  };
}
