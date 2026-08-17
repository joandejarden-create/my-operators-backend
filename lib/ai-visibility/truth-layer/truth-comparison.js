/**
 * Truth comparison engine (P0D-A / P0D-A.1) — conservative, auditable, semantic-safe.
 */

import { createHash } from "crypto";
import { normalizeMatchKey } from "../normalize-entities.js";
import {
  COMPARISON_STATUSES,
  isTruthDimensionProductionReady,
  isGovernanceEligibleForTruth,
} from "./truth-eligibility.js";
import {
  getBrandBasicsTruthFact,
  parentCompanyMatches,
} from "./brand-basics-truth.js";
import { isExplicitParentClaimValue } from "./truth-claim-extractor.js";
import {
  inferAiSemanticDimension,
  inferDealalitySemanticDimension,
  areDimensionsComparable,
  isContrastiveMention,
  isListEnumerationClaim,
  isPortfolioRangeChainScale,
  isNonParentParentClaim,
  normalizeArchitectureBucket,
  isChainScaleListNoise,
  TRUTH_RULE_VERSION_SEMANTIC,
} from "./truth-comparability.js";
import { assessExecutiveEligibility } from "./executive-eligibility.js";

export const TRUTH_RULE_VERSION = TRUTH_RULE_VERSION_SEMANTIC;

export function buildTruthComparisonId(seed) {
  return `tcmp_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Semantic pre-check before dimension-specific comparison.
 */
export function applySemanticPreCheck(claim, dealalityFact, spanText, subjectBrandName) {
  const aiDim = inferAiSemanticDimension(claim.claimType, claim.claimValue, spanText);
  const dealDim = inferDealalitySemanticDimension(dealalityFact.factType, dealalityFact.factValue);
  const comparability = areDimensionsComparable(aiDim, dealDim);

  if (comparability === "NO") {
    return { status: "NOT_EVALUATED", reason: "cross_dimension_non_comparable", aiDim, dealDim };
  }
  if (isContrastiveMention(spanText, claim.claimValue, subjectBrandName)) {
    return { status: "NOT_EVALUATED", reason: "contrastive_context_not_subject_claim" };
  }
  if (isListEnumerationClaim(spanText, claim.claimValue, subjectBrandName)) {
    return { status: "NOT_EVALUATED", reason: "list_enumeration_not_subject_attribute" };
  }
  if (claim.claimType === "CHAIN_SCALE" && isPortfolioRangeChainScale(spanText)) {
    return { status: "NOT_EVALUATED", reason: "portfolio_range_not_subject_scale" };
  }
  if (claim.claimType === "CHAIN_SCALE" && isChainScaleListNoise(spanText, claim.claimValue, subjectBrandName)) {
    return { status: "NOT_EVALUATED", reason: "list_context_not_subject_chain_scale" };
  }
  if (
    (claim.claimType === "PARENT_COMPANY" || claim.claimType === "BRAND_FAMILY") &&
    isNonParentParentClaim(spanText, claim.claimValue)
  ) {
    return { status: "NOT_EVALUATED", reason: "positioning_language_not_parent" };
  }
  if (claim.claimType === "CHAIN_SCALE" && aiDim === "POSITIONING") {
    return { status: "NOT_EVALUATED", reason: "positioning_language_not_chain_scale" };
  }
  if (claim.claimType === "BRAND_MODEL" && aiDim === "POSITIONING") {
    return { status: "NOT_EVALUATED", reason: "positioning_language_not_architecture" };
  }
  if (claim.claimType === "BRAND_MODEL" && aiDim === "OPERATING_MODEL") {
    return { status: "NOT_EVALUATED", reason: "operating_model_not_architecture" };
  }

  return null;
}

/**
 * Compare chain scale with ambiguity tolerance.
 */
export function compareChainScale(aiClaimValue, dealalityFact, spanText = "") {
  if (!dealalityFact || dealalityFact.eligibility !== "ELIGIBLE") {
    return { status: "INSUFFICIENT_DEALALITY_EVIDENCE", reason: "no_governed_chain_scale" };
  }
  const ai = normalizeMatchKey(aiClaimValue);
  const deal = normalizeMatchKey(dealalityFact.factValue);
  if (ai === deal) return { status: "ALIGNED", reason: "exact_taxonomy_match" };

  const compatible = [
    ["upper upscale", "upscale"],
    ["luxury", "upper upscale"],
    ["lifestyle", "upper upscale"],
    ["lifestyle boutique", "lifestyle brand"],
    ["midscale", "upper upscale"],
  ];
  for (const [a, b] of compatible) {
    if ((ai.includes(a) && deal.includes(b)) || (ai.includes(b) && deal.includes(a))) {
      if (a === "midscale" && /\bmidscale to upper/i.test(String(spanText))) {
        return { status: "NOT_EVALUATED", reason: "scale_range_not_point_claim", ambiguous: true };
      }
      return { status: "ALIGNED", reason: "compatible_terminology_overlap", ambiguous: true };
    }
  }

  return { status: "POTENTIAL_PERCEPTION_GAP", reason: "chain_scale_mismatch" };
}

/**
 * Compare brand architecture (Brand Model field — architecture values only).
 */
export function compareBrandModel(aiClaimValue, dealalityFact) {
  if (!dealalityFact || dealalityFact.eligibility !== "ELIGIBLE") {
    return { status: "INSUFFICIENT_DEALALITY_EVIDENCE", reason: "no_governed_brand_model" };
  }
  const ai = normalizeMatchKey(aiClaimValue);
  const deal = normalizeMatchKey(dealalityFact.factValue);
  if (ai === deal || ai.includes(deal) || deal.includes(ai)) {
    return { status: "ALIGNED", reason: "brand_model_match" };
  }

  const aiBucket = normalizeArchitectureBucket(aiClaimValue);
  const dealBucket = normalizeArchitectureBucket(dealalityFact.factValue);
  if (aiBucket === dealBucket) {
    return { status: "ALIGNED", reason: "architecture_bucket_match", ambiguous: true };
  }

  const softCollection = ["soft brand", "collection brand", "soft collection brand"];
  if (softCollection.some((s) => ai.includes(s.replace(/\s/g, "")) || ai.includes(s))) {
    if (deal.includes("collection") || deal.includes("soft")) {
      return { status: "ALIGNED", reason: "soft_collection_compatible", ambiguous: true };
    }
  }

  const mutuallyExclusive = {
    COLLECTION: ["HARD_BRAND"],
    HARD_BRAND: ["COLLECTION", "SOFT_BRAND"],
    SOFT_BRAND: ["HARD_BRAND"],
  };
  const exclusions = mutuallyExclusive[dealBucket] || [];
  if (!exclusions.includes(aiBucket)) {
    return { status: "NOT_EVALUATED", reason: "non_mutually_exclusive_architecture_values" };
  }

  return { status: "POTENTIAL_PERCEPTION_GAP", reason: "brand_architecture_mismatch" };
}

/**
 * Compare soft brand / collection status.
 */
export function compareSoftBrandCollection(aiClaimValue, dealalityFact, spanText = "") {
  if (!dealalityFact) {
    return { status: "INSUFFICIENT_DEALALITY_EVIDENCE", reason: "no_governed_architecture" };
  }
  if (isListEnumerationClaim(spanText, aiClaimValue, "") || isContrastiveMention(spanText, aiClaimValue, "")) {
    return { status: "NOT_EVALUATED", reason: "collection_context_not_subject_classification" };
  }
  const ai = normalizeMatchKey(aiClaimValue);
  const deal = normalizeMatchKey(dealalityFact.factValue);
  if (ai.includes("soft") && deal.includes("soft")) return { status: "ALIGNED", reason: "soft_brand_aligned" };
  if (ai.includes("collection") && deal.includes("collection")) return { status: "ALIGNED", reason: "collection_aligned" };
  if (ai.includes("hard") && deal.includes("hard")) return { status: "ALIGNED", reason: "hard_brand_aligned" };

  const aiBucket = normalizeArchitectureBucket(aiClaimValue);
  const dealBucket = normalizeArchitectureBucket(dealalityFact.factValue);
  if (aiBucket === dealBucket) return { status: "ALIGNED", reason: "architecture_bucket_match" };

  return { status: "POTENTIAL_PERCEPTION_GAP", reason: "collection_status_mismatch" };
}

/**
 * Main comparison for one AI claim.
 */
export function compareTruthClaim(claim, basicsIndex, options = {}) {
  const subjectRow = basicsIndex.get(claim.subjectBrandId);
  const subjectBrandName = subjectRow?.brandName || claim.subjectBrandName || "";

  if (!claim?.supportingSpan?.exactText && !claim?.supportingSpan?.text) {
    return finalizeComparison(claim, {
      comparisonStatus: "NOT_EVALUATED",
      comparisonReason: "missing_supporting_span",
      eligibilityStatus: "NOT_ELIGIBLE",
    }, subjectBrandName, subjectRow);
  }

  const spanText = claim.supportingSpan.exactText || claim.supportingSpan.text;
  if (!spanText || spanText.length < 8) {
    return finalizeComparison(claim, {
      comparisonStatus: "NOT_EVALUATED",
      comparisonReason: "span_too_short",
      eligibilityStatus: "NOT_ELIGIBLE",
    }, subjectBrandName, subjectRow);
  }

  const dimension = mapClaimTypeToDimension(claim.claimType);
  if (!isTruthDimensionProductionReady(dimension) && dimension !== "CHAIN_SCALE") {
    return finalizeComparison(claim, {
      comparisonStatus: "INSUFFICIENT_DEALALITY_EVIDENCE",
      comparisonReason: `dimension_${dimension}_deferred`,
      eligibilityStatus: "DEFERRED",
      dealalityFactType: dimension,
    }, subjectBrandName, subjectRow);
  }

  const dealalityFact = getBrandBasicsTruthFact(claim.subjectBrandId, basicsIndex, claim.claimType);
  if (!dealalityFact || !isGovernanceEligibleForTruth(dealalityFact.governanceState)) {
    return finalizeComparison(claim, {
      comparisonStatus: "INSUFFICIENT_DEALALITY_EVIDENCE",
      comparisonReason: "dealality_fact_not_eligible",
      eligibilityStatus: "NOT_ELIGIBLE",
      dealalityFactType: claim.claimType,
    }, subjectBrandName, subjectRow);
  }

  const preCheck = applySemanticPreCheck(claim, dealalityFact, spanText, subjectBrandName);
  if (preCheck) {
    return finalizeComparison(claim, {
      comparisonStatus: preCheck.status,
      comparisonReason: preCheck.reason,
      eligibilityStatus: dealalityFact.eligibility,
      dealalityFactType: dealalityFact.factType,
      dealalityFactValue: dealalityFact.factValue,
      dealalitySource: dealalityFact.source,
      dealalityGovernanceState: dealalityFact.governanceState,
      ambiguous: preCheck.ambiguous || false,
      aiSemanticDimension: preCheck.aiDim,
      dealalitySemanticDimension: preCheck.dealDim,
    }, subjectBrandName, subjectRow);
  }

  let result;
  switch (claim.claimType) {
    case "PARENT_COMPANY":
    case "BRAND_FAMILY": {
      if (!isExplicitParentClaimValue(claim.claimValue, { name: subjectBrandName })) {
        result = { status: "NOT_EVALUATED", reason: "parent_claim_not_explicit" };
        break;
      }
      const siblingParents = (options.peerParentKeys || []).filter(Boolean);
      const match = parentCompanyMatches(claim.claimValue, {
        factValue: dealalityFact.factValue,
        parentNormalizedKeys: dealalityFact.parentNormalizedKeys,
      }, siblingParents);
      if (match.siblingLeak) {
        result = { status: "NOT_EVALUATED", reason: "parent_describes_sibling_context" };
      } else if (match.match) {
        result = { status: "ALIGNED", reason: "parent_company_match" };
      } else {
        result = { status: "POTENTIAL_PERCEPTION_GAP", reason: "parent_company_mismatch" };
      }
      break;
    }
    case "CHAIN_SCALE":
      result = compareChainScale(claim.claimValue, dealalityFact, spanText);
      break;
    case "BRAND_MODEL":
      result = compareBrandModel(claim.claimValue, dealalityFact);
      break;
    case "SOFT_BRAND_COLLECTION":
      result = compareSoftBrandCollection(claim.claimValue, dealalityFact, spanText);
      break;
    case "POSITIONING":
    case "CONVERSION_ORIENTATION":
      result = { status: "INSUFFICIENT_DEALALITY_EVIDENCE", reason: "no_structured_dealality_field" };
      break;
    default:
      result = { status: "NOT_EVALUATED", reason: "unsupported_claim_type" };
  }

  return finalizeComparison(claim, {
    comparisonStatus: result.status,
    comparisonReason: result.reason,
    eligibilityStatus: dealalityFact.eligibility,
    dealalityFactType: dealalityFact.factType,
    dealalityFactValue: dealalityFact.factValue,
    dealalitySource: dealalityFact.source,
    dealalityGovernanceState: dealalityFact.governanceState,
    ambiguous: result.ambiguous || false,
    aiSemanticDimension: inferAiSemanticDimension(claim.claimType, claim.claimValue, spanText),
    dealalitySemanticDimension: inferDealalitySemanticDimension(dealalityFact.factType, dealalityFact.factValue),
  }, subjectBrandName, subjectRow);
}

function finalizeComparison(claim, fields, subjectBrandName, subjectRow) {
  const record = buildComparisonRecord(claim, fields);
  const exec = assessExecutiveEligibility(record, {
    subjectBrandName,
    parentNormalizedKeys: subjectRow?.parentNormalizedKeys,
  });
  record.executiveEligible = exec.executiveEligible;
  record.executiveEligibilityReason = exec.reason;
  if (exec.clientSafeExplanation) record.clientSafeExplanation = exec.clientSafeExplanation;
  return record;
}

export function mapClaimTypeToDimension(claimType) {
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

function buildComparisonRecord(claim, fields) {
  return {
    truthComparisonId: buildTruthComparisonId(
      `${claim.claimId}:${fields.comparisonStatus}:${fields.comparisonReason}`
    ),
    subjectBrandId: claim.subjectBrandId,
    scenarioId: claim.scenarioId,
    promptId: claim.promptId,
    provider: claim.provider,
    language: claim.language,
    geography: claim.geography,
    aiClaimType: claim.claimType,
    aiClaimValue: claim.claimValue,
    aiSupportingSpan: claim.supportingSpan?.exactText || claim.supportingSpan?.text,
    evidenceId: claim.evidenceId,
    responseId: claim.responseId,
    dealalityFactType: fields.dealalityFactType || null,
    dealalityFactValue: fields.dealalityFactValue || null,
    dealalitySource: fields.dealalitySource || null,
    dealalityGovernanceState: fields.dealalityGovernanceState || null,
    eligibilityStatus: fields.eligibilityStatus || null,
    comparisonStatus: fields.comparisonStatus,
    comparisonReason: fields.comparisonReason,
    ambiguous: fields.ambiguous || false,
    aiSemanticDimension: fields.aiSemanticDimension || null,
    dealalitySemanticDimension: fields.dealalitySemanticDimension || null,
    truthRuleVersion: TRUTH_RULE_VERSION,
    createdAt: new Date().toISOString(),
    CENSUS_USED: false,
  };
}

export { COMPARISON_STATUSES };
