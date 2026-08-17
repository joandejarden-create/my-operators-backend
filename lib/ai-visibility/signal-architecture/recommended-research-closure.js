/**
 * Formal closure of the Recommended research cycle (2026-08-15).
 * Not abandoned — reopen only under governed conditions. No recurring research.
 */

export const RECOMMENDED_RESEARCH_CLOSURE_VERSION =
  "ai_intelligence_recommended_research_closure_v1";

export const AI_SIGNAL_RECOMMENDED_STATUS =
  "RESEARCH_BLOCKED_NOT_PRODUCTION_READY";

export const RECOMMENDED_DEPENDENT_BLOCKS = Object.freeze({
  RECOMMENDATION_SHARE: "BLOCKED",
  FIRST_RECOMMENDATION: "NOT_READY",
  QUESTIONS_WON: "BLOCKED",
  NEGATIVE_OR_QUALIFIED: "NOT_READY",
  COMPARATOR: "NOT_READY",
});

/** Production gate unchanged — do not lower. */
export const RECOMMENDED_PRODUCTION_GATE = Object.freeze({
  PRECISION_MIN: 0.98,
  RECALL_MIN: 0.98,
});

export const RECOMMENDED_RESEARCH_CYCLE_SUMMARY = Object.freeze({
  deterministicBinaryRemediation: Object.freeze({
    version: "ai_visibility_recommended_binary_v1",
    ruleVersion: "ai_visibility_recommended_binary_rules_v1_3",
    unambiguousDevN: 273,
    precision: 0.8182,
    recall: 0.6,
    gate: "FAIL",
  }),
  semanticAdjudicationFeasibility: Object.freeze({
    model: "gpt-4.1-mini",
    studyN: 114,
    bestConsensus: Object.freeze({
      rule: "majority_2_of_3",
      precision: 0.8478,
      recall: 0.5342,
    }),
    repeatability: Object.freeze({
      unanimousDecisionRate: 0.9649,
      twoOfThreeAgreementRate: 1,
    }),
    gate: "FAIL",
    fullDevExecuted: false,
  }),
  conclusion: "RECOMMENDED_SIGNAL_NOT_YET_RELIABLY_AUTOMATABLE",
  abandoned: false,
  recurringResearchScheduled: false,
});

/**
 * Reopen Recommended research only if one of these is true.
 * Do not schedule recurring Recommended research.
 */
export const REOPEN_RESEARCH_IF = Object.freeze([
  "materially stronger semantic model becomes available",
  "evidence-contract architecture materially improves",
  "substantial new real-world monitoring dataset exists",
  "customer use case justifies human-assisted adjudication",
  "new research hypothesis has a plausible path to >=98% precision and recall",
]);

export function buildRecommendedResearchClosure() {
  return {
    version: RECOMMENDED_RESEARCH_CLOSURE_VERSION,
    AI_SIGNAL_RECOMMENDED: AI_SIGNAL_RECOMMENDED_STATUS,
    ...RECOMMENDED_DEPENDENT_BLOCKS,
    PRODUCTION_GATE: RECOMMENDED_PRODUCTION_GATE,
    cycle: RECOMMENDED_RESEARCH_CYCLE_SUMMARY,
    REOPEN_RESEARCH_IF,
    RECOMMENDED_REQUIRED_FOR_BRAND_AI_VISIBILITY_V1: false,
    note:
      "Recommended remains a governed future signal. Brand AI Visibility v1 advances on Presence only.",
  };
}
