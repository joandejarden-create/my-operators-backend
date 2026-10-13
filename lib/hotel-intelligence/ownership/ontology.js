/**
 * Ownership Intelligence ontology — relationship roles ≠ entity identity.
 * EntityType describes what an entity IS; RelationshipRole/Type describe context.
 */

export const OWNERSHIP_ONTOLOGY_VERSION = "ownership-ontology-v1";

/**
 * Durable entity classification (not a hotel-relationship role).
 * Keep small — do not encode propco/sponsor/operator here.
 */
export const ENTITY_TYPES = Object.freeze([
  "company",
  "fund",
  "family_office",
  "reit",
  "individual",
  "government_entity",
  "trust",
  "joint_venture",
  "institution",
  "other",
  "unknown",
]);

/** Graph predicates (edges). */
export const RELATIONSHIP_TYPES = Object.freeze([
  "OWNED_BY",
  "CONTROLLED_BY",
  "SPONSORED_BY",
  "JV_WITH",
  "DEVELOPED_BY",
  "OPERATED_BY",
  "ASSET_MANAGED_BY",
  "BRANDED_BY",
  // Reserved — no P0 research obligation
  "FINANCED_BY",
  "DECISION_MAKER_AT",
  // Reserved — lease/lessor (P1.6 smallest ontology extension; not auto-produced as OWNED_BY)
  "LEASED_FROM",
  // Reserved — portfolio "investment" without control (P1.7; do not overload OWNED_BY / SPONSORED_BY)
  "INVESTED_IN_BY",
]);

/**
 * Presentation / query role derived from relationship context.
 * Stored on the relationship; never as EntityType.
 */
export const RELATIONSHIP_ROLES = Object.freeze([
  "PROPERTY_OWNER",
  "PARENT_OWNER",
  "ULTIMATE_SPONSOR",
  "JV_PARTNER",
  "DEVELOPER",
  "OPERATOR",
  "ASSET_MANAGER",
  "BRAND_FRANCHISOR",
  "CAPITAL_PROVIDER",
  "DECISION_MAKER",
  "LESSOR",
]);

export const RELATIONSHIP_TYPE_TO_ROLE = Object.freeze({
  OWNED_BY: "PROPERTY_OWNER",
  CONTROLLED_BY: "PARENT_OWNER",
  SPONSORED_BY: "ULTIMATE_SPONSOR",
  JV_WITH: "JV_PARTNER",
  DEVELOPED_BY: "DEVELOPER",
  OPERATED_BY: "OPERATOR",
  ASSET_MANAGED_BY: "ASSET_MANAGER",
  BRANDED_BY: "BRAND_FRANCHISOR",
  FINANCED_BY: "CAPITAL_PROVIDER",
  DECISION_MAKER_AT: "DECISION_MAKER",
  LEASED_FROM: "LESSOR",
  INVESTED_IN_BY: "CAPITAL_PROVIDER",
});

/** P0 research may produce these predicates. */
export const P0_RELATIONSHIP_TYPES = Object.freeze([
  "OWNED_BY",
  "CONTROLLED_BY",
  "SPONSORED_BY",
  "JV_WITH",
  "DEVELOPED_BY",
  "OPERATED_BY",
  "ASSET_MANAGED_BY",
  "BRANDED_BY",
]);

export const VERIFICATION_STATUSES = Object.freeze([
  "verified",
  "high",
  "probable",
  "needs_review",
  "conflict",
  "unknown",
  "historical",
]);

export const ENTITY_STATUSES = Object.freeze(["active", "inactive", "unknown"]);

export const SOURCE_TYPES = Object.freeze([
  "government",
  "first_party",
  "trade_press",
  "corporate_registry",
  "securities_filing",
  "dealality_census",
  "dealality_existing",
  "web_search",
  "manual",
  "other",
]);

/** Never allowed as product graph evidence. */
export const FORBIDDEN_EVIDENCE_SOURCES = Object.freeze([
  "costar",
  "costar_true_owner",
  "gtm_costar",
  "gtm_internal_hint",
]);

export const RESEARCH_STAGES = Object.freeze([
  "A",
  "country",
  "brazil_corporate",
  "franchise_disclosure",
  "gleif",
  "economic",
  "B",
  "C",
]);

export const RESEARCH_RUN_STATUSES = Object.freeze([
  "pending",
  "running",
  "completed",
  "stopped",
  "failed",
]);

export const EXTRACTION_METHODS = Object.freeze([
  "stage_a",
  "stage_country",
  "branch_to_matrix",
  "qsa_partner_climb",
  "franchisor_financial_disclosure",
  "gleif",
  "stage_economic",
  "stage_b",
  "stage_c",
  "owner_portfolio",
  "manual",
]);

export const IDENTIFIER_KINDS = Object.freeze([
  "lei",
  "ruc",
  "nit",
  "rfc",
  "cnpj",
  "tax_id",
  "registration_number",
  "other",
]);

/**
 * @param {string} relationshipType
 * @returns {string | null}
 */
export function roleForRelationshipType(relationshipType) {
  return RELATIONSHIP_TYPE_TO_ROLE[relationshipType] || null;
}

/**
 * @param {string} value
 * @param {readonly string[]} allowed
 */
export function isAllowedEnum(value, allowed) {
  return allowed.includes(String(value || "").trim());
}
