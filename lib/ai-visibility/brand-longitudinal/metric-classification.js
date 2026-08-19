/**
 * Trendable vs snapshot-only metric classification.
 * Recommendation metrics explicitly excluded.
 */

export const BRAND_LONGITUDINAL_METRIC_CLASS_VERSION = "brand_longitudinal_metric_classification_v1";

export const TRENDABLE_METRICS = Object.freeze([
  "AI_PRESENCE",
  "QUESTIONS_MISSING",
  "CITATION_RATE",
  "PROVIDER_PRESENCE",
  "COMPETITIVE_GAP_PERSISTENCE",
  "SOURCE_MIX_OWNED_EXTERNAL",
  "OWNED_CITATION_RATE",
  "EXTERNAL_CITATION_RATE",
]);

export const TRENDABLE_WITH_CAUTION = Object.freeze([
  "PRODUCTION_NARRATIVE_OCCURRENCE",
  "PRODUCTION_ASSOCIATION_OCCURRENCE",
  "TRUTH_PERCEPTION_GAP_OCCURRENCE",
  "SOURCE_RECURRENCE",
]);

export const SNAPSHOT_EVENT_METRICS = Object.freeze([
  "NEW_TRUTH_ISSUE",
  "NEW_NARRATIVE_TENSION",
  "SPECIFIC_EVIDENCE_EXAMPLE",
  "NEW_RECURRING_CITED_DOMAIN",
  "DOMAIN_NO_LONGER_CITED",
]);

/** Explicitly forbidden — RECOMMENDATION_BUILD = 0 */
export const FORBIDDEN_METRICS = Object.freeze([
  "RECOMMENDATION_RATE",
  "RECOMMENDATION_SHARE",
  "QUESTIONS_WON",
  "WIN_RATE",
  "AVERAGE_RECOMMENDATION_POSITION",
  "OWNER_DECISION_SHARE_OF_VOICE",
  "TOP3_RECOMMENDATION_RATE",
  "FIRST_RECOMMENDATION_RATE",
]);

export function classifyMetric(metricKey) {
  const k = String(metricKey || "").toUpperCase();
  if (FORBIDDEN_METRICS.includes(k)) {
    return { metric: k, classification: "FORBIDDEN", trendEligible: false };
  }
  if (TRENDABLE_METRICS.includes(k)) {
    return { metric: k, classification: "TRENDABLE", trendEligible: true };
  }
  if (TRENDABLE_WITH_CAUTION.includes(k)) {
    return {
      metric: k,
      classification: "TRENDABLE_WITH_CAUTION",
      trendEligible: true,
      caution: "Requires multiple genuine periods; short-term stability ≠ longitudinal persistence",
    };
  }
  if (SNAPSHOT_EVENT_METRICS.includes(k)) {
    return { metric: k, classification: "SNAPSHOT_EVENT", trendEligible: false };
  }
  return { metric: k, classification: "UNCLASSIFIED", trendEligible: false };
}

export function assertNoForbiddenMetrics(metricKeys = []) {
  const violations = metricKeys.filter((k) => classifyMetric(k).classification === "FORBIDDEN");
  return { ok: violations.length === 0, violations };
}
