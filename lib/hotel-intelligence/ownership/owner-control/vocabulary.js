/**
 * Packet 2.8B-2 — Owner Control Graph vocabulary.
 * Entity type = what entity IS. Relationship = what it DOES.
 */

export const OWNER_CONTROL_VOCAB_VERSION = "owner-control-vocab-v1";

export const ENTITY_TYPES = Object.freeze([
  "HOTEL",
  "COMPANY",
  "HOLDING_COMPANY",
  "OPERATING_COMPANY",
  "PROPCO",
  "SPV",
  "FUND",
  "FAMILY_OFFICE",
  "REIT",
  "TRUST",
  "JV",
  "INDIVIDUAL",
  "GOVERNMENT_ENTITY",
  "UNKNOWN",
]);

/** Canonical relationship types (reuse Focus Graph / HI ontology). */
export const RELATIONSHIP_TYPES = Object.freeze([
  "OWNED_BY",
  "CONTROLLED_BY",
  "SPONSORED_BY",
  "JV_WITH",
  "PARENT_OF",
  "SUBSIDIARY_OF",
  "DEVELOPED_BY",
  "ASSET_MANAGED_BY",
  "LEASED_FROM",
  "OPERATED_BY",
  "BRANDED_BY",
  "RELATED",
  "UNKNOWN",
]);

export const PORTFOLIO_BUCKETS = Object.freeze([
  "OWNED_CONTROLLED",
  "JV_PARTIAL",
  "SPONSORED_ECONOMIC_INTEREST",
  "OPERATED_MANAGED",
  "DEVELOPED",
  "HISTORICAL_DISPOSED",
  "PIPELINE_ANNOUNCED",
  "UNKNOWN_RELATIONSHIP",
]);

export const ASSOCIATION_LEVELS = Object.freeze([
  "CORE_CONTROL",
  "CONTROLLED_ENTITY",
  "PROPERTY_VEHICLE",
  "PARENT_ENTITY",
  "SPONSORED_ENTITY",
  "JV_ENTITY",
  "JV_PARTNER",
  "AFFILIATE",
  "OPERATING_ENTITY",
  "HISTORICAL_ASSOCIATION",
  "SHAREHOLDER_INFLUENCE",
]);

export const OWNER_ROLES = Object.freeze([
  "ECONOMIC_OWNER",
  "CONTROL_OWNER",
  "PROPERTY_OWNING_ORGANIZATION",
  "PROPCO",
  "SPONSOR",
  "UNKNOWN",
]);

export const COMPLETENESS_STATUSES = Object.freeze([
  "COMPLETE",
  "STRONG",
  "PARTIAL",
  "UNKNOWN",
  "CONFLICTED",
  "STALE",
]);

export const CONTROL_PERCENTAGE_CLASSES = Object.freeze([
  "FULL_100",
  "MAJORITY",
  "MINORITY",
  "JV",
  "CONTROL_UNDISCLOSED_PCT",
  "UNKNOWN",
]);

export const TEMPORAL_STATUSES = Object.freeze([
  "CURRENT",
  "FORMER",
  "ANNOUNCED",
  "PLANNED",
  "CONTESTED",
  "HISTORICAL",
  "UNKNOWN",
]);
