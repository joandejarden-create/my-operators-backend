/**
 * Source change comparison foundation (Phase 3B.6).
 * Prepares deterministic source movement keys — no calculation until Period 2 exists.
 */

export const PERIOD_SOURCE_CHANGE_FOUNDATION_VERSION =
  "ai_visibility_period_source_change_foundation_v1";

export const SOURCE_CHANGE_TYPES = Object.freeze({
  NEWLY_APPEARING_SOURCE: "Newly Appearing Source",
  NO_LONGER_APPEARING: "No Longer Appearing",
  APPEARING_ACROSS_BOTH_PERIODS: "Appearing Across Both Periods",
  RESPONSE_FREQUENCY_INCREASED: "Response Frequency Increased",
  RESPONSE_FREQUENCY_DECREASED: "Response Frequency Decreased",
});

export const SOURCE_CHANGE_TERMINOLOGY = Object.freeze({
  PREFERRED: Object.values(SOURCE_CHANGE_TYPES),
  AVOID: [
    "lost authority",
    "gained trust",
    "influence increased",
    "ranking power",
  ],
  RULE: "No causal claims; factual source presence/frequency changes only",
});

/**
 * Build source comparison key for period-to-period matching.
 */
export function buildSourceComparisonKey(parts = {}) {
  return {
    version: PERIOD_SOURCE_CHANGE_FOUNDATION_VERSION,
    provider: String(parts.provider || "").toLowerCase(),
    sourceDomain: String(parts.sourceDomain || parts.domain || "").toLowerCase(),
    sourceUrl: parts.sourceUrl || null,
    geography: parts.geography || parts.geographyKey || null,
    language: parts.language || null,
    intent: parts.intent || parts.intentTerritory || null,
    periodId: parts.periodId || null,
  };
}

export function sourceComparisonKeyString(key) {
  const k = buildSourceComparisonKey(key);
  return [k.provider, k.sourceDomain, k.geography, k.language, k.intent].join("|");
}

/**
 * Check if source change analysis is available.
 */
export function isSourceChangeAvailable(completedComparablePeriods) {
  return completedComparablePeriods >= 2;
}

/**
 * Build source change foundation metadata (no actual source movement).
 */
export function buildSourceChangeFoundation(opts = {}) {
  const completedPeriods = opts.completedComparablePeriods ?? 1;
  const available = isSourceChangeAvailable(completedPeriods);

  return {
    version: PERIOD_SOURCE_CHANGE_FOUNDATION_VERSION,
    SOURCE_CHANGE_FOUNDATION_READY: true,
    AVAILABLE_NOW: available,
    SOURCE_CHANGE_AVAILABLE: available,
    completedComparablePeriods: completedPeriods,
    changeTypes: { ...SOURCE_CHANGE_TYPES },
    terminology: { ...SOURCE_CHANGE_TERMINOLOGY },
    comparisonKeyFields: [
      "provider",
      "sourceDomain",
      "geography",
      "language",
      "intent",
      "period",
    ],
    note: available
      ? "Source comparison may proceed when prior and current periods are comparable"
      : "Second comparable period required before source change analysis",
  };
}
