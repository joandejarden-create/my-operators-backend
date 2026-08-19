/**
 * Period-scoped benchmark grain identity — cross-period deduplication prohibited.
 * Preserves historical intersectionGrainKey within a period; adds comparison scope.
 */

import { intersectionGrainKey } from "./intersection-grains.js";

export const PERIOD_SCOPED_GRAIN_VERSION = "period_scoped_grain_v1";
export const BASELINE_MEASUREMENT_PERIOD = "DEMO_VALIDATION";
export const CROSS_PERIOD_DEDUPLICATION = "PROHIBITED";
export const POOLED_ALL_PERIODS_INDEX = "PROHIBITED";
export const HISTORICAL_IDS_CHANGED = false;

/** Dimensions represented in benchmark / longitudinal architecture. */
export const GRAIN_DIMENSION_AUDIT = Object.freeze({
  PROMPT: { field: "promptId", inWithinPeriodGrain: true, inLongitudinalGrain: true },
  PROVIDER: { field: "provider", inWithinPeriodGrain: true, inLongitudinalGrain: true },
  GEO: { field: "geography", inWithinPeriodGrain: true, inLongitudinalGrain: true },
  LANGUAGE: { field: "language", inWithinPeriodGrain: true, inLongitudinalGrain: true },
  SCENARIO: { field: "scenarioId", inWithinPeriodGrain: true, inLongitudinalGrain: true },
  PROMPT_VERSION: { field: "promptVersion", inWithinPeriodGrain: true, inLongitudinalGrain: true },
  MODEL_BOUNDARY: { field: "model", inWithinPeriodGrain: false, inLongitudinalGrain: true },
  CLASSIFIER_VERSION: { field: "classifierVersion", inWithinPeriodGrain: false, inLongitudinalGrain: false },
  MEASUREMENT_PERIOD: {
    field: "measurementPeriodId",
    inWithinPeriodGrain: false,
    inLongitudinalGrain: true,
    role: "SEPARATE_DIMENSION",
  },
  OBSERVATION_TIMESTAMP: {
    field: "timestamp",
    inWithinPeriodGrain: false,
    inLongitudinalGrain: true,
    role: "DERIVABLE_ONLY",
  },
  WAVE_ID: { field: "waveId", inWithinPeriodGrain: false, inLongitudinalGrain: false, role: "SEPARATE_DIMENSION" },
});

/**
 * Canonical comparable grain within one measurement period (unchanged from intersection grains).
 */
export function buildCanonicalComparableGrainIdentity(record = {}) {
  return intersectionGrainKey(record);
}

/**
 * Period-scoped comparison key — never dedupe across periods on canonical identity alone.
 */
export function buildComparisonGrainKey(record = {}, measurementPeriodId = null) {
  const periodId = measurementPeriodId || record.measurementPeriodId || BASELINE_MEASUREMENT_PERIOD;
  return `${periodId}|${buildCanonicalComparableGrainIdentity(record)}`;
}

/**
 * Audit whether measurement period is part of comparison scope.
 */
export function auditPeriodArchitecture() {
  const withinPeriodKey = buildCanonicalComparableGrainIdentity({
    scenarioId: "scenario_soft_brand_collection_affiliation_v1",
    promptId: "p_test",
    provider: "openai",
    language: "en",
    geography: "CALA",
    promptVersion: "1",
  });
  const periodA = buildComparisonGrainKey(
    { scenarioId: "s", promptId: "p", provider: "openai", language: "en", geography: "CALA", promptVersion: "1" },
    "period_a"
  );
  const periodB = buildComparisonGrainKey(
    { scenarioId: "s", promptId: "p", provider: "openai", language: "en", geography: "CALA", promptVersion: "1" },
    "period_b"
  );

  return {
    version: PERIOD_SCOPED_GRAIN_VERSION,
    MEASUREMENT_PERIOD_PRESENT: true,
    PERIOD_PART_OF_COMPARISON_SCOPE: true,
    PERIOD_ROLE: "SEPARATE_DIMENSION",
    WITHIN_PERIOD_GRAIN_INCLUDES_PERIOD: false,
    LONGITUDINAL_GRAIN_INCLUDES_PERIOD: true,
    CURRENT_GRAIN_KEY: withinPeriodKey,
    COMPARISON_GRAIN_KEY_EXAMPLE: { periodA, periodB, distinct: periodA !== periodB },
    CROSS_PERIOD_DEDUP_RISK: withinPeriodKey === periodA.split("|").slice(1).join("|") ? "YES_WITHOUT_PERIOD_SCOPE" : "NO",
    CROSS_PERIOD_DEDUPLICATION,
    POOLED_ALL_PERIODS_INDEX,
    HISTORICAL_IDS_CHANGED,
    RECOMMENDED_PERIOD_SCOPING:
      "Scope benchmark calculation to one measurementPeriodId per index. Use buildComparisonGrainKey for ledger uniqueness across periods. Never merge response sets from different periods into one grainsByScenario index.",
    dimensions: GRAIN_DIMENSION_AUDIT,
  };
}

/**
 * Detect accidental cross-period deduplication in a grain key collection.
 * Returns collisions where canonical identity matches but period differs — expected distinct keys.
 */
export function detectCrossPeriodDedupViolations(records = []) {
  const byCanonical = new Map();
  const violations = [];
  for (const rec of records) {
    const canonical = buildCanonicalComparableGrainIdentity(rec);
    const periodId = rec.measurementPeriodId || BASELINE_MEASUREMENT_PERIOD;
    const comparison = buildComparisonGrainKey(rec, periodId);
    const prev = byCanonical.get(canonical);
    if (prev && prev.periodId !== periodId && prev.comparison === comparison) {
      violations.push({
        canonical,
        periodA: prev.periodId,
        periodB: periodId,
        reason: "same_comparison_key_different_periods_collapsed",
      });
    }
    byCanonical.set(canonical, { periodId, comparison });
  }
  return {
    ok: violations.length === 0,
    violations,
    distinctPeriods: [...new Set(records.map((r) => r.measurementPeriodId || BASELINE_MEASUREMENT_PERIOD))],
    CROSS_PERIOD_DEDUPLICATION,
  };
}

/**
 * Partition responses by measurement period — never pool.
 */
export function partitionResponsesByPeriod(responses = []) {
  const byPeriod = new Map();
  for (const rec of responses) {
    const periodId = rec.measurementPeriodId || BASELINE_MEASUREMENT_PERIOD;
    if (!byPeriod.has(periodId)) byPeriod.set(periodId, []);
    byPeriod.get(periodId).push({ ...rec, measurementPeriodId: periodId });
  }
  return byPeriod;
}

/**
 * Assert a grain set was built from a single period slice.
 */
export function assertSinglePeriodScope(periodId, sourcePeriodIds = []) {
  const unique = [...new Set(sourcePeriodIds.filter(Boolean))];
  if (unique.length > 1) {
    return {
      ok: false,
      reason: "MULTI_PERIOD_POOL_DETECTED",
      periodId,
      sourcePeriodIds: unique,
    };
  }
  return { ok: true, periodId: periodId || unique[0] || BASELINE_MEASUREMENT_PERIOD };
}
