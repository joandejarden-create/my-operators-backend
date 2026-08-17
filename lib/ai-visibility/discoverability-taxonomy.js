/**
 * Discoverability → Referral → Business Impact product taxonomy (Phase 3C.1).
 * Provider-neutral. No composite scores.
 */

export const DISCOVERABILITY_TAXONOMY_VERSION = "ai_visibility_discoverability_taxonomy_v1";

export const PRODUCT_CHAIN = Object.freeze([
  "AI Visibility",
  "Discoverability",
  "Referral",
  "Business Impact",
]);

export const PRODUCT_DEFINITIONS = Object.freeze({
  AI_VISIBILITY: {
    id: "ai_visibility",
    label: "AI Visibility",
    question:
      "How often and how strongly does this Brand appear in monitored AI owner/developer decisions?",
    IMPLEMENTED: true,
    DUPLICATION_RULE: "Do not duplicate Visibility metrics in Discoverability",
  },
  DISCOVERABILITY: {
    id: "discoverability",
    label: "Discoverability",
    question:
      "Can AI/search systems technically discover, access, crawl, and understand the Brand's relevant development content?",
    IMPLEMENTED: "foundation",
  },
  REFERRAL: {
    id: "referral",
    label: "Referral",
    question:
      "Is measurable traffic reaching the Brand's relevant pages from AI-originated or AI-adjacent sources?",
    IMPLEMENTED: "contract_only",
  },
  BUSINESS_IMPACT: {
    id: "business_impact",
    label: "Business Impact",
    question: "Do those visits generate qualified owner/development actions?",
    IMPLEMENTED: "contract_only",
  },
});

export const COMPOSITE_SCORE = Object.freeze({
  ALLOWED: false,
  RULE: "No Discoverability Score, GEO Score, Business Impact Score, or AI Optimization Score in v1",
});

/**
 * Lock distinction between product layers.
 */
export const VISIBILITY_VS_DISCOVERABILITY_GUARD = Object.freeze({
  AI_PRESENCE:
    "Observed behavior inside monitored AI responses — not crawl readiness",
  DISCOVERABILITY:
    "Technical ability of relevant Brand content to be found/accessed — not AI Presence",
  REFERRAL: "Observed visits from identifiable sources — not citations in AI responses",
  BUSINESS_IMPACT: "Observed qualified actions — not inferred from AI Presence or citations",
  INFERRED_BUSINESS_IMPACT_ALLOWED: false,
  BLURRING_PROHIBITED: true,
});

export function assertNotVisibilityMetric(metricId) {
  const blocked = [
    "aiPresenceRate",
    "recommendationRate",
    "recommendationShare",
    "top3RecommendationRate",
    "citationRate",
  ];
  return !blocked.includes(metricId);
}
