/**
 * Operator Explorer DNA — JSON subsection fields (single source of truth).
 * Sync: `node scripts/sync-operator-dna-explorer-json-fields.mjs`
 * Airtable: `node scripts/ensure-operator-dna-explorer-json-schema.mjs --apply`
 */

export const DNA_EXPLORER_JSON_TABLE = {
  PROFILE: "Operator Setup - Profile & Positioning",
  PLATFORM: "Operator Setup - Platform & Markets",
  COMMERCIAL: "Operator Setup - Commercial Fit & Terms",
};

/** @typedef {{ formKey: string, airtableTable: string, tableKey: string, setupTab: string, label: string, jsonShape: string }} DnaJsonFieldSpec */

/** @type {DnaJsonFieldSpec[]} */
export const DNA_EXPLORER_JSON_FIELD_SPECS = [
  {
    formKey: "op_commercial_engine_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "2. Operating Platform",
    label: "Commercial Engine capability tiles",
    jsonShape: '{ "intro": "...", "items": [{ "title": "...", "description": "..." }] }',
  },
  {
    formKey: "op_owner_reporting_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "2. Operating Platform",
    label: "Owner Reporting & Communication tiles",
    jsonShape: '{ "intro": "...", "items": [{ "title": "...", "description": "..." }] }',
  },
  {
    formKey: "op_preopening_transition_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "2. Operating Platform",
    label: "Pre-Opening & Transition Support tiles",
    jsonShape: '{ "intro": "...", "items": [{ "title": "...", "description": "..." }] }',
  },
  {
    formKey: "op_conversion_repositioning_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "2. Operating Platform",
    label: "Conversion & Repositioning tiles",
    jsonShape: '{ "intro": "...", "items": [{ "title": "...", "description": "..." }] }',
  },
  {
    formKey: "op_fb_lifestyle_resort_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "2. Operating Platform",
    label: "F&B, Lifestyle & Resort capability tiles",
    jsonShape: '{ "intro": "...", "items": [{ "title": "...", "description": "..." }] }',
  },
  {
    formKey: "brand_portfolio_mix_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PROFILE,
    tableKey: "PERF",
    setupTab: "3. Brand & Relationships",
    label: "Portfolio mix by brand / flag type",
    jsonShape: '[{ "brandFlagType", "portfolioMix", "assetContext", "relationshipStatus" }]',
  },
  {
    formKey: "brand_relationship_depth_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PROFILE,
    tableKey: "PERF",
    setupTab: "3. Brand & Relationships",
    label: "Brands & relationship depth",
    jsonShape: '[{ "brandSegment", "relationshipType", "depth", "ownerContext" }]',
  },
  {
    formKey: "brand_execution_capabilities_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PROFILE,
    tableKey: "PERF",
    setupTab: "3. Brand & Relationships",
    label: "Brand execution capabilities",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "brand_governance_compliance_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PROFILE,
    tableKey: "PERF",
    setupTab: "3. Brand & Relationships",
    label: "Brand governance & compliance support",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "ov_strategic_owner_value_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "5. Owner Value & Engagement",
    label: "Strategic owner value",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "ov_engagement_cadence_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "5. Owner Value & Engagement",
    label: "Owner engagement cadence",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "ov_controls_governance_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "5. Owner Value & Engagement",
    label: "Controls & governance",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "ov_reports_received_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "5. Owner Value & Engagement",
    label: "Reports owners receive",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "ov_owner_tools_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "5. Owner Value & Engagement",
    label: "Owner tools & support channels",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "ov_lifecycle_support_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "5. Owner Value & Engagement",
    label: "Owner support across asset lifecycle",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "mkt_regional_expertise_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "4. Markets & Footprint",
    label: "Local / regional expertise",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "mkt_market_fit_signals_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.PLATFORM,
    tableKey: "FOOTPRINT",
    setupTab: "4. Markets & Footprint",
    label: "Market fit signals",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "bf_fit_criteria_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "9. Best Fit & Preferences",
    label: "Operator fit criteria",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "bf_best_fit_project_types_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "9. Best Fit & Preferences",
    label: "Best-fit project types",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "bf_preferred_deal_profile_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "9. Best Fit & Preferences",
    label: "Preferred deal profile",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "bf_evaluation_path_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "9. Best Fit & Preferences",
    label: "Evaluation path",
    jsonShape: '[{ "title", "description" }]',
  },
  {
    formKey: "bf_red_flags_json",
    airtableTable: DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    tableKey: "OWNER_REL",
    setupTab: "9. Best Fit & Preferences",
    label: "Potential red flags",
    jsonShape: '[{ "title", "description" }]',
  },
];

export const DNA_EXPLORER_JSON_FORM_KEYS = DNA_EXPLORER_JSON_FIELD_SPECS.map((s) => s.formKey);

export const DNA_EXPLORER_JSON_AIRTABLE_SPECS = DNA_EXPLORER_JSON_FIELD_SPECS.map((s) => ({
  name: s.formKey,
  type: "multilineText",
  table: s.airtableTable,
}));
