/**
 * Non-narrative operator truth dimensions from governed Operator Master / Explorer.
 * Do not use website marketing copy as canonical truth.
 */

export const OPERATOR_TRUTH_VERSION = "operator_truth_layer_v1";

export const OPERATOR_TRUTH_DIMENSIONS = Object.freeze({
  PRODUCTION: Object.freeze([
    "OPERATOR_IDENTITY",
    "OPERATOR_LENS",
    "MONITORED_SCOPE",
    "CANONICAL_NAME",
    "CANONICAL_ID",
    "FIRST_PARTY_DOMAIN",
    "OPERATOR_MODEL",
    "MANAGED_BRAND_AFFILIATED",
    "THIRD_PARTY_MANAGEMENT",
    "GEOGRAPHIC_OPERATING_SCOPE",
    "BRAND_AGNOSTIC_CAPABILITY",
  ]),
  DETAIL_ONLY: Object.freeze([
    "PARENT_PLATFORM",
    "FULL_SERVICE_CAPABILITY",
    "LUXURY_CAPABILITY",
    "LIFESTYLE_BOUTIQUE_CAPABILITY",
    "RESORT_CAPABILITY",
    "INDEPENDENT_HOTEL_CAPABILITY",
    "CONVERSION_REPOSITIONING_CAPABILITY",
  ]),
  NOT_USABLE: Object.freeze([
    "WEBSITE_MARKETING_COPY",
    "VAGUE_GLOBAL_OPERATOR_LABEL",
    "CENSUS_FOOTPRINT_COUNTS",
    "AI_INFERRED_CAPABILITY",
    "CHAIN_SCALE_HOTEL_SEGMENT",
  ]),
});

export const OPERATOR_TRUTH_SEMANTIC_RULES = Object.freeze([
  "Do not compare global operator with third-party manager as if contradictory.",
  "Do not compare brand-company identity with operating-capability lens as a perception gap.",
  "Potential outputs: ALIGNED | POTENTIAL_OPERATOR_PERCEPTION_GAP | NOT_EVALUATED.",
  "Client label: Potential AI Perception Gap — never AI is wrong.",
]);

export function operatorTruthAudit() {
  return {
    version: OPERATOR_TRUTH_VERSION,
    governedSource: "Operator Setup Master + Operator Explorer factory/baseline registries",
    productionDimensions: [...OPERATOR_TRUTH_DIMENSIONS.PRODUCTION],
    detailOnlyDimensions: [...OPERATOR_TRUTH_DIMENSIONS.DETAIL_ONLY],
    notUsable: [...OPERATOR_TRUTH_DIMENSIONS.NOT_USABLE],
    censusReads: 0,
  };
}
