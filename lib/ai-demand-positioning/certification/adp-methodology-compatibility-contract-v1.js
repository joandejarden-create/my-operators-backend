/**
 * Methodology compatibility for trends / MoM.
 * Incompatible methodologies must not produce performance gain/decline framing.
 *
 * Contract: ADP_METHODOLOGY_COMPATIBILITY_CONTRACT_V1
 */

import {
  ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1,
  isIndependentUnpromptedMethodologyId,
  isLegacyMentionAllCoreMethodologyId,
} from "./adp-independent-unprompted-methodology-contract-v1.js";

export const ADP_METHODOLOGY_COMPATIBILITY_CONTRACT_V1 =
  "ADP_METHODOLOGY_COMPATIBILITY_CONTRACT_V1";

export const METHODOLOGY_COMPARISON = Object.freeze({
  COMPATIBLE: "COMPATIBLE",
  INCOMPATIBLE: "INCOMPATIBLE",
  UNKNOWN: "UNKNOWN",
});

export function normalizeMethodologyId(id) {
  const s = String(id || "").trim();
  if (!s) return null;
  return s;
}

export function classifyMethodologyFamily(id) {
  if (isIndependentUnpromptedMethodologyId(id)) return "INDEPENDENT_UNPROMPTED_V1";
  if (isLegacyMentionAllCoreMethodologyId(id)) return "LEGACY_MENTION_ALL_CORE";
  if (!id) return "UNSTAMPED_TREATED_AS_LEGACY";
  return "OTHER";
}

/**
 * Decide whether two periods may form a customer-facing MoM delta.
 */
export function assessMethodologyCompatibility({
  priorMethodologyId = null,
  currentMethodologyId = null,
  priorCoverage = null,
  currentCoverage = null,
} = {}) {
  const prior = normalizeMethodologyId(priorMethodologyId);
  const current = normalizeMethodologyId(currentMethodologyId);
  const priorFamily = classifyMethodologyFamily(prior);
  const currentFamily = classifyMethodologyFamily(current);

  if (!prior && !current) {
    return {
      status: METHODOLOGY_COMPARISON.UNKNOWN,
      allowMomDelta: false,
      reason: "both_methodology_unstamped",
      priorFamily,
      currentFamily,
      customerFraming: "SUPPRESS_PERFORMANCE_DELTA",
    };
  }

  if (priorFamily !== currentFamily) {
    return {
      status: METHODOLOGY_COMPARISON.INCOMPATIBLE,
      allowMomDelta: false,
      reason: "methodology_family_differs",
      priorMethodologyId: prior || ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1,
      currentMethodologyId: current || null,
      priorFamily,
      currentFamily,
      customerFraming: "METHODOLOGY_TRANSITION_NOT_PERFORMANCE",
      establishV1Baseline:
        currentFamily === "INDEPENDENT_UNPROMPTED_V1" &&
        priorFamily !== "INDEPENDENT_UNPROMPTED_V1",
    };
  }

  // Same family — optional coverage gate
  if (
    priorCoverage?.eligibleScenarioCount != null &&
    currentCoverage?.eligibleScenarioCount != null &&
    Number(priorCoverage.eligibleScenarioCount) !==
      Number(currentCoverage.eligibleScenarioCount)
  ) {
    return {
      status: METHODOLOGY_COMPARISON.COMPATIBLE,
      allowMomDelta: true,
      reason: "same_methodology_coverage_differs_disclose",
      priorFamily,
      currentFamily,
      coverageDisclosureRequired: true,
      customerFraming: "ALLOW_DELTA_WITH_COVERAGE_NOTE",
    };
  }

  return {
    status: METHODOLOGY_COMPARISON.COMPATIBLE,
    allowMomDelta: true,
    reason: "same_methodology_family",
    priorFamily,
    currentFamily,
    customerFraming: "ALLOW_PERFORMANCE_DELTA",
  };
}

/**
 * Build transition record when cutting over legacy → V1.
 */
export function buildMethodologyTransitionRecord({
  propertyId,
  legacyPeriodId,
  legacyMethodologyId = ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1,
  legacyConsiderationRate = null,
  v1PeriodId,
  v1MethodologyId = ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  v1ConsiderationRate = null,
}) {
  const compatibility = assessMethodologyCompatibility({
    priorMethodologyId: legacyMethodologyId,
    currentMethodologyId: v1MethodologyId,
  });
  return {
    contract: ADP_METHODOLOGY_COMPATIBILITY_CONTRACT_V1,
    propertyId,
    legacy: {
      periodId: legacyPeriodId,
      methodologyId: legacyMethodologyId,
      considerationRate: legacyConsiderationRate,
      preservedHistorically: true,
    },
    v1Baseline: {
      periodId: v1PeriodId,
      methodologyId: v1MethodologyId,
      considerationRate: v1ConsiderationRate,
      firstCertifiedV1Baseline: true,
    },
    compatibility,
    forbiddenCustomerClaims: [
      "performance_decline",
      "performance_gain",
      "mom_delta_across_methodology",
    ],
  };
}
