/**
 * Central association attribute production eligibility gate (P0C).
 * Single source of truth — do not scatter across UI modules.
 */

/** Attributes validated on sealed holdout (P0B.1) for client publication. */
export const PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES = Object.freeze(["DISTRIBUTION"]);

/** All other production-eligible taxonomy attrs remain research-only until holdout passes. */
export const RESEARCH_ONLY_ASSOCIATION_ATTRIBUTES = Object.freeze([
  "INDEPENDENT_IDENTITY",
  "DESIGN_INDIVIDUALITY",
  "LOYALTY",
  "OWNER_FLEXIBILITY",
  "OWNER_CONTROL",
  "CONVERSION_SUITABILITY",
  "GEOGRAPHIC_PRESENCE",
  "LIFESTYLE_POSITIONING",
  "LUXURY_POSITIONING",
  "BRANDED_RESIDENCES",
  "OPERATING_MODEL",
  "MARKET_FIT",
]);

export const DEFERRED_ASSOCIATION_ATTRIBUTES = Object.freeze(["ECONOMICS", "DEVELOPMENT_SUPPORT"]);

/**
 * @param {string|null|undefined} attributeId
 * @returns {boolean}
 */
export function isAssociationAttributeProductionEligible(attributeId) {
  if (!attributeId) return false;
  return PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES.includes(String(attributeId));
}

/**
 * @param {string|null|undefined} attributeId
 * @returns {"PRODUCTION"|"RESEARCH_ONLY"|"DEFERRED"|"UNKNOWN"}
 */
export function associationAttributePublicationTier(attributeId) {
  if (!attributeId) return "UNKNOWN";
  const id = String(attributeId);
  if (PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES.includes(id)) return "PRODUCTION";
  if (DEFERRED_ASSOCIATION_ATTRIBUTES.includes(id)) return "DEFERRED";
  if (RESEARCH_ONLY_ASSOCIATION_ATTRIBUTES.includes(id)) return "RESEARCH_ONLY";
  return "UNKNOWN";
}
