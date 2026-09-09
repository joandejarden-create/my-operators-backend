/**
 * Dealality Research Method Library — playbook schema + guards.
 * Webhound is lab/benchmark/escalation only — not production truth.
 */

export const RESEARCH_METHOD_LIBRARY_VERSION = "research-method-library-v1";

export const TEMPORAL_BRAND_STATUSES = Object.freeze([
  "CURRENT",
  "FORMER",
  "ANNOUNCED",
  "PLANNED",
  "CONTESTED",
  "CANCELLED",
  "UNKNOWN",
]);

export const NEGATIVE_SCREENS = Object.freeze([
  "WRONG_PROPERTY",
  "ADJACENT_ASSET",
  "OPERATOR_NOT_OWNER",
  "HISTORICAL_NOT_CURRENT",
  "ANNOUNCED_NOT_CURRENT",
  "SIMILAR_NAME_COLLISION",
  "SIMILAR_LUXURY_PROPERTY_COLLISION",
  "INSUFFICIENT_IDENTITY_MATCH",
  "INSUFFICIENT_ENTITY_MATCH",
  "TITLE_NOT_AUTHORITY",
  "SOURCE_NOT_DECISIVE",
  "CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD",
  "DEVELOPER_NOT_OWNER",
  "DEVELOPER_NOT_CURRENT_OWNER",
  "MIXED_USE_MULTIPLE_PARTIES",
  "HISTORICAL_BUYER_NOT_CURRENT",
  "FUND_INVESTMENT_NOT_SOLE_OWNER",
  "BRANDED_RESIDENCE_DEVELOPER_NOT_HOTEL_OWNER",
  "MASTER_DEVELOPER_NOT_PROPCO",
  "BORROWER_NOT_BENEFICIAL_OWNER",
]);

export const SOURCE_AUTHORITY_WATERFALL_MX_PUBCO = Object.freeze([
  "securities_exchange_regulatory_filings",
  "company_annual_reports",
  "government_corporate_filings",
  "investor_relations_documents",
  "owner_operator_official_website",
  "brand_official_announcement",
  "reputable_transaction_development_press",
  "other_secondary",
]);

const REQUIRED_PLAYBOOK_FIELDS = [
  "playbook_id",
  "name",
  "description",
  "jurisdictions",
  "version",
];

export function validatePlaybook(playbook = {}) {
  const errors = [];
  for (const field of REQUIRED_PLAYBOOK_FIELDS) {
    if (playbook[field] == null || playbook[field] === "") {
      errors.push(`missing_${field}`);
    }
  }
  if (playbook.jurisdictions && !Array.isArray(playbook.jurisdictions)) {
    errors.push("jurisdictions_must_be_array");
  }
  if (Array.isArray(playbook.negative_screens)) {
    for (const screen of playbook.negative_screens) {
      if (!NEGATIVE_SCREENS.includes(screen)) {
        errors.push(`unknown_negative_screen:${screen}`);
      }
    }
  }
  if (Array.isArray(playbook.temporal_classifications)) {
    for (const status of playbook.temporal_classifications) {
      if (!TEMPORAL_BRAND_STATUSES.includes(status)) {
        errors.push(`unknown_temporal_status:${status}`);
      }
    }
  }
  return { ok: errors.length === 0, errors };
}
