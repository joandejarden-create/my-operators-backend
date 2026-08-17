/**
 * P0C Class D integration — production gaps only when truth + executive gates pass (P0D-A.1).
 */

import { buildGapId, GAP_ENGINE_RULE_VERSION } from "../gaps/gap-identity.js";
import { TRUTH_DIMENSION_READINESS } from "./truth-eligibility.js";
import { assessExecutiveEligibility } from "./executive-eligibility.js";

function dimensionForClaimType(claimType) {
  const map = {
    PARENT_COMPANY: "PARENT_COMPANY",
    BRAND_FAMILY: "BRAND_FAMILY",
    CHAIN_SCALE: "CHAIN_SCALE",
    BRAND_MODEL: "BRAND_MODEL",
    SOFT_BRAND_COLLECTION: "SOFT_BRAND_COLLECTION",
    POSITIONING: "STRUCTURED_POSITIONING",
    CONVERSION_ORIENTATION: "CONVERSION_ORIENTATION",
  };
  return map[claimType] || claimType;
}

/**
 * Whether a truth comparison can promote a P0C Class D gap.
 */
export function isProductionTruthGapEligible(comparison, claim = {}) {
  if (!comparison) return { eligible: false, reason: "no_comparison" };
  if (comparison.comparisonStatus !== "POTENTIAL_PERCEPTION_GAP") {
    return { eligible: false, reason: "not_perception_gap" };
  }
  if (comparison.executiveEligible !== true) {
    const exec = assessExecutiveEligibility(comparison, {
      subjectBrandName: comparison.subjectBrandName,
    });
    if (!exec.executiveEligible) {
      return { eligible: false, reason: exec.reason || "not_executive_eligible", detailOnly: true };
    }
  }
  const dimension = dimensionForClaimType(comparison.aiClaimType || claim.claimType);
  const readiness = TRUTH_DIMENSION_READINESS[dimension];
  if (readiness !== "PRODUCTION_VALIDATED" && dimension !== "CHAIN_SCALE") {
    return { eligible: false, reason: `dimension_${readiness || "UNKNOWN"}` };
  }
  if (comparison.eligibilityStatus !== "ELIGIBLE") {
    return { eligible: false, reason: "dealality_not_eligible" };
  }
  if (!comparison.aiSupportingSpan || comparison.aiSupportingSpan.length < 8) {
    return { eligible: false, reason: "invalid_span" };
  }
  if (!comparison.subjectBrandId || !comparison.aiClaimType || !comparison.aiClaimValue) {
    return { eligible: false, reason: "incomplete_claim" };
  }
  if (comparison.CENSUS_USED) {
    return { eligible: false, reason: "census_blocked" };
  }
  return { eligible: true, reason: "production_truth_gap", dimension, readiness, executiveEligible: true };
}

/**
 * Build production Class D gap from validated truth comparison.
 */
export function buildProductionTruthGap(comparison) {
  const gate = isProductionTruthGapEligible(comparison);
  if (!gate.eligible) return null;

  return {
    gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP",
    subjectBrandId: comparison.subjectBrandId,
    scenarioId: comparison.scenarioId,
    promptId: comparison.promptId,
    provider: comparison.provider,
    language: comparison.language,
    geography: comparison.geography,
    aiClaimType: comparison.aiClaimType,
    aiClaimValue: comparison.aiClaimValue,
    dealalityFactType: comparison.dealalityFactType,
    dealalityFactValue: comparison.dealalityFactValue,
    comparisonStatus: comparison.comparisonStatus,
    comparisonReason: comparison.comparisonReason,
    productionEligible: true,
    executiveEligible: true,
    lifecycleStatus: "ACTIVE",
    classification: "REVIEW",
    ruleVersion: GAP_ENGINE_RULE_VERSION,
    truthRuleVersion: comparison.truthRuleVersion,
    truthComparisonId: comparison.truthComparisonId,
    clientSafeExplanation: comparison.clientSafeExplanation || null,
    evidenceIds: comparison.evidenceId ? [comparison.evidenceId] : [],
    createdAt: comparison.createdAt || new Date().toISOString(),
    gapId: buildGapId({
      gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP",
      subjectBrandId: comparison.subjectBrandId,
      scenarioId: comparison.scenarioId,
      geography: comparison.geography,
      language: comparison.language,
      attributeId: comparison.aiClaimType,
      comparisonWindow: comparison.truthComparisonId,
    }),
    truthLayer: {
      dealalityTruthRef: comparison.truthComparisonId,
      truthComparisonStatus: comparison.comparisonStatus,
      dealalitySource: comparison.dealalitySource,
      dealalityGovernanceState: comparison.dealalityGovernanceState,
      executiveEligible: true,
    },
  };
}

/**
 * Partition comparisons into production D gaps vs blocked placeholders.
 */
export function integrateP0cClassDGaps(comparisons = [], placeholderGaps = []) {
  const production = [];
  const blocked = [];
  const detailOnly = [];

  for (const cmp of comparisons) {
    const gap = buildProductionTruthGap(cmp);
    if (gap) {
      production.push(gap);
    } else if (cmp.comparisonStatus === "POTENTIAL_PERCEPTION_GAP") {
      const gate = isProductionTruthGapEligible(cmp);
      const entry = {
        comparisonId: cmp.truthComparisonId,
        reason: gate.reason,
        comparison: cmp,
      };
      if (gate.detailOnly) detailOnly.push(entry);
      else blocked.push(entry);
    }
  }

  for (const ph of placeholderGaps) {
    blocked.push({
      gapId: ph.gapId,
      reason: "p0c_placeholder_no_truth_gate",
      placeholder: true,
    });
  }

  return {
    productionDGaps: production,
    blockedDGaps: blocked,
    detailOnlyDGaps: detailOnly,
    PRODUCTION_D_GAPS: production.length,
    BLOCKED_D_GAPS: blocked.length,
    DETAIL_ONLY_D_GAPS: detailOnly.length,
    EXECUTIVE_ELIGIBLE: production.filter((g) => g.executiveEligible).length,
  };
}

export { dimensionForClaimType };
