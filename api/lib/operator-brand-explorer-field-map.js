/**
 * Brand & Relationships Explorer — Airtable field map (Profile & Positioning).
 * UI: public/js/operator-brand-relationships-sections.js
 */

export const BRAND_EXPLORER_TABLE = "Operator Setup - Profile & Positioning";

/** @type {Record<string, { airtableField: string, table: string, type: string, uiUse: string }>} */
export const MAP_BRAND_EXPLORER_FIELDS = {
  brand_portfolio_mix_json: {
    airtableField: "brand_portfolio_mix_json",
    table: BRAND_EXPLORER_TABLE,
    type: "multilineText",
    uiUse: "Portfolio Mix by Brand / Flag Type table",
  },
  brand_relationship_depth_json: {
    airtableField: "brand_relationship_depth_json",
    table: BRAND_EXPLORER_TABLE,
    type: "multilineText",
    uiUse: "Brands & Relationship Depth table",
  },
  brand_execution_capabilities_json: {
    airtableField: "brand_execution_capabilities_json",
    table: BRAND_EXPLORER_TABLE,
    type: "multilineText",
    uiUse: "Brand Execution Capabilities cards",
  },
  brand_governance_compliance_json: {
    airtableField: "brand_governance_compliance_json",
    table: BRAND_EXPLORER_TABLE,
    type: "multilineText",
    uiUse: "Brand Governance & Compliance Support cards",
  },
  brand_soft_independent_narrative: {
    airtableField: "brand_soft_independent_narrative",
    table: BRAND_EXPLORER_TABLE,
    type: "multilineText",
    uiUse: "Soft Brand / Independent Experience narrative",
  },
  brand_conversion_project_count: {
    airtableField: "brand_conversion_project_count",
    table: BRAND_EXPLORER_TABLE,
    type: "singleLineText",
    uiUse: "Brand & Relationship Snapshot — Conversion / Reflag KPI",
  },
  brandedVsIndependentMix: {
    airtableField: "brandedVsIndependentMix",
    table: BRAND_EXPLORER_TABLE,
    type: "singleLineText",
    uiUse: "Snapshot — Branded / Independent % (fallback when mix JSON empty)",
  },
  numberOfBrands: {
    airtableField: "numberOfBrands",
    table: BRAND_EXPLORER_TABLE,
    type: "number",
    uiUse: "Snapshot — Brand relationships count; Approved families KPI",
  },
};

/** Existing Profile fields used by snapshot (not created by explorer schema script). */
export const MAP_BRAND_LEGACY_PROFILE_FIELDS = {
  brand_signal_audit: { airtableField: "brand_signal_audit", table: BRAND_EXPLORER_TABLE },
  brand_signal_reflag: { airtableField: "brand_signal_reflag", table: BRAND_EXPLORER_TABLE },
  brand_signal_franchise_align: {
    airtableField: "brand_signal_franchise_align",
    table: BRAND_EXPLORER_TABLE,
  },
  brand_signal_soft_retention: {
    airtableField: "brand_signal_soft_retention",
    table: BRAND_EXPLORER_TABLE,
  },
  brand_narrative_compliance: {
    airtableField: "brand_narrative_compliance",
    table: BRAND_EXPLORER_TABLE,
  },
  brand_narrative_relationship: {
    airtableField: "brand_narrative_relationship",
    table: BRAND_EXPLORER_TABLE,
  },
  brands: { airtableField: "brands", table: BRAND_EXPLORER_TABLE },
};
