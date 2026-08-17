/**
 * AI Visibility — shared config / versions / opportunity thresholds.
 * Thresholds are versioned; do not hard-code them in call sites.
 */

export const METRIC_VERSION = "ai_visibility_metrics_v1";
export const PARSER_VERSION = "ai_visibility_parser_v1";
/** @deprecated Prefer RESOLVER_VERSION from normalize-entities.js (v2.1). Kept for import compat. */
export const RESOLVER_VERSION = "ai_visibility_entity_resolver_v2_1_contextual";
export const RECOMMENDATION_CLASSIFIER_VERSION =
  "ai_visibility_recommendation_classifier_v4_1";
export const CITATION_ASSOC_VERSION = "ai_visibility_citation_assoc_v1";
export const UNRESOLVED_FILTER_VERSION = "ai_visibility_unresolved_filter_v1";
export const GEOGRAPHY_MODEL_VERSION = "ai_visibility_geography_v1";
export const RULE_VERSION = "ai_visibility_opportunity_rules_v1";

export const EVIDENCE_DESCRIPTORS = Object.freeze({
  REPEATED_ACROSS_ENGINES: "Repeated across engines",
  REPEATED_ACROSS_RUNS: "Repeated across runs",
  EMERGING_PATTERN: "Emerging pattern",
  SINGLE_ENGINE: "Single-engine observation",
});

/** Founder-approved initial working thresholds (configurable). */
export const OPPORTUNITY_THRESHOLDS_V1 = Object.freeze({
  ruleVersion: RULE_VERSION,
  persistentAbsence: Object.freeze({
    minEngines: 2,
    minPeriods: 2,
    requirePeerPresence: true,
  }),
  competitorDominance: Object.freeze({
    presenceRateGapPp: 15,
    presenceRateRatio: 2,
    minObservations: 4,
  }),
});

export function isAiVisibilityEnabled() {
  return String(process.env.AI_VISIBILITY_ENABLED || "").toLowerCase() === "true";
}

export function isAiVisibilityLiveTestAllowed() {
  return String(process.env.AI_VISIBILITY_LIVE_TEST || "").toLowerCase() === "true";
}

export function resolveDefaultProvider() {
  return String(process.env.AI_VISIBILITY_PROVIDER || "openai").trim().toLowerCase();
}

export function resolveDefaultModel() {
  return (
    String(process.env.AI_VISIBILITY_MODEL || "").trim() ||
    "gpt-4.1"
  );
}

export function resolveMaxTestRuns() {
  const n = parseInt(process.env.AI_VISIBILITY_MAX_TEST_RUNS || "1", 10);
  return Number.isFinite(n) && n > 0 ? n : 1;
}

export function resolveMaxDailyCostUsd() {
  const raw = process.env.AI_VISIBILITY_MAX_DAILY_COST_USD;
  if (raw == null || String(raw).trim() === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

export function resolveMaxBatchCostUsd() {
  const raw = process.env.AI_VISIBILITY_MAX_BATCH_COST_USD;
  if (raw == null || String(raw).trim() === "") return 5;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : 5;
}

export function resolvePhase2EModel() {
  return String(process.env.AI_VISIBILITY_MODEL || "gpt-5.6").trim() || "gpt-5.6";
}
