/**
 * Operator association taxonomy — research first. Nothing production-eligible yet.
 */

export const OPERATOR_ASSOCIATION_TAXONOMY_VERSION = "operator_association_taxonomy_v1";

export const OPERATOR_ASSOCIATION_FAMILIES = Object.freeze([
  "COMMERCIAL_REVENUE_CAPABILITY",
  "OWNER_ALIGNMENT",
  "OPERATIONAL_SCALE",
  "LOCAL_MARKET_EXPERTISE",
  "BRAND_AGNOSTIC_FLEXIBILITY",
  "LUXURY_CAPABILITY",
  "RESORT_CAPABILITY",
  "INDEPENDENT_HOTEL_CAPABILITY",
  "TECHNOLOGY_SYSTEMS",
  "LABOR_OPERATIONS",
  "FB_OPERATIONS",
  "TURNAROUND_REPOSITIONING",
  "DEVELOPMENT_SUPPORT",
]);

export function operatorAssociationStatus() {
  return {
    version: OPERATOR_ASSOCIATION_TAXONOMY_VERSION,
    taxonomy: [...OPERATOR_ASSOCIATION_FAMILIES],
    productionEligible: [],
    detailOnly: [],
    researchOnly: [...OPERATOR_ASSOCIATION_FAMILIES],
    blocked: [],
  };
}
