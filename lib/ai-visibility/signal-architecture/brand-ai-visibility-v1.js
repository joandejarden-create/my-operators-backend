/**
 * Brand AI Visibility v1 — Presence-led product contract.
 * Recommended is NON-BLOCKING for v1.
 */

import { PRODUCTION_SIGNALS, SIGNAL_KEYS } from "./production-signals.js";
import {
  AI_SIGNAL_RECOMMENDED_STATUS,
  RECOMMENDED_DEPENDENT_BLOCKS,
  REOPEN_RESEARCH_IF,
  buildRecommendedResearchClosure,
} from "./recommended-research-closure.js";
import { getSignalReadiness, SIGNAL_READINESS } from "./readiness.js";

export const BRAND_AI_VISIBILITY_V1_VERSION =
  "brand_ai_visibility_v1_presence_led_v1";

export const BRAND_AI_VISIBILITY_V1_STATUS = "PRESENCE_LED_PRODUCTION_BUILD";

/** Controlled client release — not full automated production. */
export const BRAND_AI_VISIBILITY_V1_RELEASE_MODE =
  "CONTROLLED_CLIENT_RELEASE_READY";

export const CONTROLLED_RELEASE_MONITORING_MODE = "MANUAL_GOVERNED";

export const BRAND_V1_CORE_SIGNAL = PRODUCTION_SIGNALS.AI_SIGNAL_PRESENCE;

/** Product questions Brand v1 answers (Presence-led). */
export const BRAND_V1_PRODUCT_QUESTIONS = Object.freeze([
  "WHERE DOES THE BRAND APPEAR?",
  "WHERE IS IT ABSENT?",
  "HOW DOES IT COMPARE WITH PEERS?",
  "HOW DOES THIS DIFFER BY PROVIDER?",
  "HOW DOES THIS DIFFER BY REGION?",
  "WHAT QUESTIONS ARE WE MISSING?",
  "HOW IS PRESENCE CHANGING?",
  "WHAT EVIDENCE / SOURCES ARE ASSOCIATED WITH THESE OBSERVATIONS?",
]);

export const BRAND_V1_ENABLED_SURFACES = Object.freeze([
  "AI_PRESENCE",
  "REGIONAL_PRESENCE",
  "COMPETITIVE_POSITION_PRESENCE",
  "QUESTIONS_MISSING",
  "COMPARABLE_PRESENCE_TRENDS",
  "CROSS_PROVIDER_PRESENCE_INTELLIGENCE",
  "EXECUTIVE_INSIGHT_LAYER",
  "EVIDENCE_DRILLDOWN",
  "SOURCE_INTELLIGENCE",
]);

export const BRAND_V1_BLOCKED_SURFACES = Object.freeze({
  RECOMMENDATION_SHARE: RECOMMENDED_DEPENDENT_BLOCKS.RECOMMENDATION_SHARE,
  QUESTIONS_WON: RECOMMENDED_DEPENDENT_BLOCKS.QUESTIONS_WON,
  FIRST_RECOMMENDATION: RECOMMENDED_DEPENDENT_BLOCKS.FIRST_RECOMMENDATION,
  NEGATIVE_OR_QUALIFIED: RECOMMENDED_DEPENDENT_BLOCKS.NEGATIVE_OR_QUALIFIED,
  COMPARATOR: RECOMMENDED_DEPENDENT_BLOCKS.COMPARATOR,
  AI_VISIBILITY_SCORE: "FORBIDDEN",
  GEO_SCORE: "FORBIDDEN",
  PROVIDER_CONFIDENCE_SCORE: "FORBIDDEN",
  CONSENSUS_SCORE: "FORBIDDEN",
});

export const BRAND_V1_ROADMAP = Object.freeze({
  CURRENT: "CONTROLLED_CLIENT_RELEASE_READY",
  NEXT: "READY_FOR_CONTROLLED_BRAND_AI_VISIBILITY_CLIENT_RELEASE",
  THEN: "OPERATOR_AI_VISIBILITY_FOUNDATION_GAP_FILL",
  FULL_AUTOMATED_PRODUCTION: "BLOCKED_UNTIL_SCHEDULER_CERTIFIED",
  QUERY_ORIGIN: "RESEARCH_COMPLETE_NO_PRODUCTIZATION",
  RECOMMENDED: AI_SIGNAL_RECOMMENDED_STATUS,
});

export const BRAND_V1_LANGUAGE_RULES = Object.freeze({
  allowed: Object.freeze([
    "Observed Presence is higher...",
    "Brand appears in...",
    "Brand is absent from...",
    "Three monitored providers surface...",
    "Associated sources include...",
    "Presence increased between comparable monitoring periods...",
    "Brand was not observed in these monitored responses.",
  ]),
  forbidden: Object.freeze([
    "AI prefers...",
    "AI recommends...",
    "This source influenced the AI...",
  ]),
  note: "Causal / preference language forbidden unless separately validated evidence supports the claim.",
});

/**
 * @returns {object} Brand v1 advancement contract snapshot
 */
export function buildBrandAiVisibilityV1Contract() {
  const presence = getSignalReadiness(SIGNAL_KEYS.PRESENCE);
  const recommended = getSignalReadiness(SIGNAL_KEYS.RECOMMENDED);
  const presenceValidated =
    presence.productionCertificationStatus === "PRODUCTION_VALIDATED" ||
    presence.productionReadinessAfterHoldout === SIGNAL_READINESS.VALIDATED;

  return {
    version: BRAND_AI_VISIBILITY_V1_VERSION,
    BRAND_AI_VISIBILITY_V1: BRAND_AI_VISIBILITY_V1_STATUS,
    BRAND_AI_VISIBILITY_V1_RELEASE_MODE,
    CONTROLLED_RELEASE_MONITORING_MODE,
    CORE_SIGNAL: BRAND_V1_CORE_SIGNAL,
    RECOMMENDED_REQUIRED_FOR_V1: false,
    BRAND_AI_VISIBILITY_CAN_ADVANCE_WITHOUT_RECOMMENDED: true,
    PRESENCE: presenceValidated
      ? "PRODUCTION_VALIDATED"
      : presence.readiness || "NOT_READY",
    RECOMMENDED:
      recommended.productionCertificationStatus ||
      recommended.readiness ||
      AI_SIGNAL_RECOMMENDED_STATUS,
    productQuestions: BRAND_V1_PRODUCT_QUESTIONS,
    ENABLED: BRAND_V1_ENABLED_SURFACES,
    BLOCKED: BRAND_V1_BLOCKED_SURFACES,
    ROADMAP: BRAND_V1_ROADMAP,
    LANGUAGE: BRAND_V1_LANGUAGE_RULES,
    recommendedClosure: buildRecommendedResearchClosure(),
    REOPEN_RESEARCH_IF,
    hardGuards: Object.freeze({
      RECOMMENDED_RESEARCH_CALLS: 0,
      RECOMMENDED_CLASSIFIER_CHANGES: 0,
      RECOMMENDATION_SHARE_ENABLE: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      PRESENCE_DEFINITION_CHANGES: 0,
      PRESENCE_RESCORE: 0,
      ARBITRARY_SCORE_CREATION: 0,
      UNSUPPORTED_CAUSAL_CLAIMS: 0,
      RAW_RESPONSES_TO_AIRTABLE: 0,
    }),
    FINAL_STATUS: presenceValidated
      ? "BRAND_AI_VISIBILITY_V1_CONTROLLED_RELEASE_READY"
      : "PRESENCE_NOT_VALIDATED",
  };
}
