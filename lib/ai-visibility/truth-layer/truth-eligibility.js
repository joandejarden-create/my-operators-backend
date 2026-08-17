/**
 * Truth Layer source governance + dimension eligibility (P0D-A).
 * Census-backed dimensions explicitly deferred.
 */

export const TRUTH_GOVERNANCE_STATES = Object.freeze([
  "COMPANY_VALIDATED",
  "COMPANY_PUBLISHED",
  "STRUCTURED_GOVERNED_FACT",
  "SOURCE_INFORMED",
  "CURATED_INTERPRETATION",
  "AI_ASSISTED",
]);

export const TRUTH_ELIGIBILITY = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  CONDITIONAL: "CONDITIONAL",
  NOT_ELIGIBLE: "NOT_ELIGIBLE",
  DEFERRED: "DEFERRED",
});

export const COMPARISON_STATUSES = Object.freeze([
  "ALIGNED",
  "POTENTIAL_PERCEPTION_GAP",
  "INSUFFICIENT_DEALALITY_EVIDENCE",
  "NOT_EVALUATED",
]);

export const TRUTH_RULE_VERSION = "ai_visibility_truth_layer_v1_1_semantic";

/** Dimensions implemented in P0D-A (non-Census). */
export const P0DA_TRUTH_DIMENSIONS = Object.freeze([
  "PARENT_COMPANY",
  "CHAIN_SCALE",
  "BRAND_MODEL",
  "STRUCTURED_POSITIONING",
  "CONVERSION_ORIENTATION",
  "SOFT_BRAND_COLLECTION",
  "BRAND_FAMILY",
]);

/** Explicitly deferred until P0D-B Census certification. */
export const CENSUS_DEFERRED_DIMENSIONS = Object.freeze([
  "COUNTRY_PRESENCE",
  "GEOGRAPHIC_FOOTPRINT",
  "OPEN_HOTEL_COUNT",
  "PIPELINE",
  "MARKET_PRESENCE",
  "MARKET_STRENGTH",
]);

export const BRAND_BASICS_FIELD_GOVERNANCE = Object.freeze({
  "Brand Name": { governance: "STRUCTURED_GOVERNED_FACT", eligibility: "ELIGIBLE" },
  "Parent Company": { governance: "COMPANY_PUBLISHED", eligibility: "ELIGIBLE" },
  "Hotel Chain Scale": { governance: "STRUCTURED_GOVERNED_FACT", eligibility: "ELIGIBLE" },
  "Brand Model": { governance: "STRUCTURED_GOVERNED_FACT", eligibility: "ELIGIBLE" },
  "Brand Architecture": { governance: "STRUCTURED_GOVERNED_FACT", eligibility: "ELIGIBLE" },
  "Brand Positioning": { governance: "SOURCE_INFORMED", eligibility: "CONDITIONAL" },
  "Brand Status": { governance: "STRUCTURED_GOVERNED_FACT", eligibility: "NOT_ELIGIBLE" },
});

/**
 * @param {string} fieldName
 */
export function brandBasicsFieldEligibility(fieldName) {
  return BRAND_BASICS_FIELD_GOVERNANCE[fieldName] || {
    governance: "SOURCE_INFORMED",
    eligibility: "CONDITIONAL",
  };
}

/**
 * Dimension readiness for P0D-A publication.
 */
export const TRUTH_DIMENSION_READINESS = Object.freeze({
  PARENT_COMPANY: "PRODUCTION_VALIDATED",
  CHAIN_SCALE: "CONDITIONAL",
  BRAND_MODEL: "PRODUCTION_VALIDATED",
  STRUCTURED_POSITIONING: "DEFERRED",
  CONVERSION_ORIENTATION: "DEFERRED",
  SOFT_BRAND_COLLECTION: "PRODUCTION_VALIDATED",
  BRAND_FAMILY: "PRODUCTION_VALIDATED",
  COUNTRY_PRESENCE: "DEFERRED",
  GEOGRAPHIC_FOOTPRINT: "DEFERRED",
  OPEN_HOTEL_COUNT: "DEFERRED",
  PIPELINE: "DEFERRED",
});

/**
 * @param {string} dimension
 */
export function isTruthDimensionProductionReady(dimension) {
  const status = TRUTH_DIMENSION_READINESS[dimension];
  return status === "PRODUCTION_VALIDATED";
}

/**
 * @param {string} governanceState
 */
export function isGovernanceEligibleForTruth(governanceState) {
  return ["COMPANY_VALIDATED", "COMPANY_PUBLISHED", "STRUCTURED_GOVERNED_FACT"].includes(
    governanceState
  );
}
