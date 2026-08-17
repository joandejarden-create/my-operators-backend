/**
 * Executive eligibility gate for Truth Layer perception gaps (P0D-A.1).
 * Boolean only — no score layer.
 */

import {
  areDimensionsComparable,
  inferAiSemanticDimension,
  inferDealalitySemanticDimension,
  isContrastiveMention,
  isListEnumerationClaim,
  isPortfolioRangeChainScale,
  isNonParentParentClaim,
  normalizeArchitectureBucket,
} from "./truth-comparability.js";
import { parentCompanyMatches, parentKeys } from "./brand-basics-truth.js";

/**
 * Deterministic executive eligibility for a truth comparison record.
 * @param {object} comparison
 * @param {object} [context]
 */
export function assessExecutiveEligibility(comparison, context = {}) {
  const subjectBrandName = context.subjectBrandName || comparison.subjectBrandName || "";
  const span = comparison.aiSupportingSpan || "";

  if (comparison.comparisonStatus !== "POTENTIAL_PERCEPTION_GAP") {
    return { executiveEligible: false, reason: "not_perception_gap" };
  }
  if (comparison.eligibilityStatus !== "ELIGIBLE") {
    return { executiveEligible: false, reason: "dealality_not_eligible" };
  }
  if (!span || span.length < 12) {
    return { executiveEligible: false, reason: "invalid_span" };
  }
  if (comparison.CENSUS_USED) {
    return { executiveEligible: false, reason: "census_blocked" };
  }

  const aiDim = inferAiSemanticDimension(comparison.aiClaimType, comparison.aiClaimValue, span);
  const dealDim = inferDealalitySemanticDimension(comparison.dealalityFactType, comparison.dealalityFactValue);
  const comparability = areDimensionsComparable(aiDim, dealDim);

  if (comparability === "NO") {
    return { executiveEligible: false, reason: "cross_dimension_non_comparable", aiDim, dealDim };
  }
  if (isContrastiveMention(span, comparison.aiClaimValue, subjectBrandName)) {
    return { executiveEligible: false, reason: "contrastive_context_not_subject_claim" };
  }
  if (isListEnumerationClaim(span, comparison.aiClaimValue, subjectBrandName)) {
    return { executiveEligible: false, reason: "list_enumeration_not_subject_attribute" };
  }
  if (isPortfolioRangeChainScale(span)) {
    return { executiveEligible: false, reason: "portfolio_range_not_subject_scale" };
  }
  if (comparison.aiClaimType === "PARENT_COMPANY" && isNonParentParentClaim(span, comparison.aiClaimValue)) {
    return { executiveEligible: false, reason: "positioning_language_not_parent" };
  }

  if (comparison.aiClaimType === "PARENT_COMPANY") {
    const match = parentCompanyMatches(comparison.aiClaimValue, {
      factValue: comparison.dealalityFactValue,
      parentNormalizedKeys: context.parentNormalizedKeys || parentKeys(comparison.dealalityFactValue),
    });
    if (match.match) {
      return { executiveEligible: false, reason: "normalization_variation_not_gap" };
    }
  }

  if (aiDim === "BRAND_ARCHITECTURE" || aiDim === "SOFT_BRAND_COLLECTION_STATUS") {
    const aiBucket = normalizeArchitectureBucket(comparison.aiClaimValue);
    const dealBucket = normalizeArchitectureBucket(comparison.dealalityFactValue);
    if (aiBucket === dealBucket) {
      return { executiveEligible: false, reason: "terminology_variation" };
    }
    const mutuallyExclusive = {
      COLLECTION: ["HARD_BRAND"],
      HARD_BRAND: ["COLLECTION", "SOFT_BRAND"],
      SOFT_BRAND: ["HARD_BRAND"],
    };
    const exclusions = mutuallyExclusive[dealBucket] || [];
    if (!exclusions.includes(aiBucket)) {
      return { executiveEligible: false, reason: "non_mutually_exclusive_values" };
    }
  }

  if (comparability === "CONDITIONAL" && comparison.ambiguous) {
    return { executiveEligible: false, reason: "conditional_dimension_ambiguous" };
  }

  return {
    executiveEligible: true,
    reason: "explicit_same_dimension_conflict",
    aiDim,
    dealDim,
    clientSafeExplanation: buildClientSafeExplanation(comparison, subjectBrandName, aiDim),
  };
}

function buildClientSafeExplanation(comparison, brandName, aiDim) {
  const brand = brandName || "the brand";
  const ai = comparison.aiClaimValue;
  const deal = comparison.dealalityFactValue;
  if (aiDim === "BRAND_ARCHITECTURE") {
    return `AI responses occasionally classify ${brand} as a ${ai.toLowerCase()}, while Dealality's governed Brand Architecture classifies it as ${deal.toLowerCase()}.`;
  }
  if (aiDim === "CHAIN_SCALE") {
    return `AI responses occasionally describe ${brand} at ${ai} chain scale, while Dealality's governed Hotel Chain Scale is ${deal}.`;
  }
  if (aiDim === "PARENT_COMPANY") {
    return `AI responses occasionally associate ${brand} with ${ai}, while Dealality's governed Parent Company is ${deal}.`;
  }
  return `AI perception differs from Dealality governed fact on ${aiDim}.`;
}
