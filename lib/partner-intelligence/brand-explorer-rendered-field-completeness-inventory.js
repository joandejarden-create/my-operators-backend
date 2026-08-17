/**
 * Owner-facing rendered field inventory for Brand Explorer completeness audits.
 * Each field maps to what the atelier actually renders for active_profile_ready brands.
 */

export const FIELD_STATUSES = Object.freeze([
  "pass",
  "missing",
  "blank",
  "too_thin",
  "generic",
  "duplicate",
  "misleading",
  "wrong_geography_label",
  "unsupported_metric",
  "should_suppress",
  "needs_owner_copy",
]);

export const RECOMMENDED_ACTIONS = Object.freeze([
  "fill_from_source",
  "rewrite_owner_copy",
  "suppress_component",
  "replace_with_not_publicly_disclosed",
  "reassign_existing_image",
  "remove_stub_chip",
  "add_case_summary",
  "add_body",
  "fix_label",
  "no_action",
]);

/** Snapshot KV fields (Overview → Brand Snapshot). */
export const SNAPSHOT_KV_FIELDS = Object.freeze([
  { fieldId: "snapshot.parent_company", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Parent Company", apiPath: "parentCompany", basicsField: "Parent Company", minWords: 1 },
  { fieldId: "snapshot.brand_family", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Brand Family", apiPath: "brandArchitecture", basicsField: "Brand Architecture", minWords: 1 },
  { fieldId: "snapshot.launch_year", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Launch Year", apiPath: "yearBrandLaunched", basicsField: "Year Brand Launched", minWords: 1 },
  { fieldId: "snapshot.brand_website", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Brand Website", apiPath: "brandWebsite", basicsField: "Brand Website", minWords: 1 },
  { fieldId: "snapshot.segment", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Segment", apiPath: "hotelChainScale", basicsField: "Hotel Chain Scale", minWords: 1 },
  { fieldId: "snapshot.brand_type", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Brand Type", apiPath: "brandModelFormat", basicsField: "Brand Model / Format", minWords: 1 },
  { fieldId: "snapshot.service_level", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Service Level", apiPath: "hotelServiceModel", basicsField: "Hotel Service Model", minWords: 1 },
  { fieldId: "snapshot.typical_keys", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Typical Keys Range", apiPath: "portfolioPerformance", derived: "typical_keys", minWords: 1 },
  { fieldId: "snapshot.typical_use_case", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Typical Use Case", slotKey: "overview.typical_use_case", minWords: 20 },
  { fieldId: "snapshot.geographic_focus", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Geographic Focus", apiPath: "regionOffered", basicsField: "Region Offered", minWords: 1 },
  { fieldId: "snapshot.development_model", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Development Model", slotKey: "overview.development_model", minWords: 20 },
  { fieldId: "snapshot.relative_positioning", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Relative Positioning", slotKey: "overview.relative_positioning", minWords: 20 },
  { fieldId: "snapshot.branded_residences", tabName: "Overview", sectionName: "Brand Snapshot", componentType: "kv", componentLabel: "Branded Residences", apiPath: "residences.status", minWords: 1 },
]);

export const POSITIONING_FIELDS = Object.freeze([
  { fieldId: "positioning.positioning", tabName: "Overview", sectionName: "Brand Positioning", componentType: "card", componentLabel: "Positioning", apiPath: "brandPositioning", basicsField: "Brand Positioning", minWords: 12 },
  { fieldId: "positioning.audience", tabName: "Overview", sectionName: "Brand Positioning", componentType: "card", componentLabel: "Audience", apiPath: "guestPsychographics", basicsField: "Guest Psychographics Description", minWords: 12 },
]);

export const OVERVIEW_CONTENT_SLOTS = Object.freeze([
  { fieldId: "overview.scenario.1", tabName: "Overview", sectionName: "Where This Brand Creates the Most Value", componentType: "scenario_card", componentLabel: "Scenario 1", slotKey: "overview.scenario.1", minWords: 45, requireImage: true },
  { fieldId: "overview.scenario.2", tabName: "Overview", sectionName: "Where This Brand Creates the Most Value", componentType: "scenario_card", componentLabel: "Scenario 2", slotKey: "overview.scenario.2", minWords: 45, requireImage: true },
  { fieldId: "overview.scenario.3", tabName: "Overview", sectionName: "Where This Brand Creates the Most Value", componentType: "scenario_card", componentLabel: "Scenario 3", slotKey: "overview.scenario.3", minWords: 45, requireImage: true },
  { fieldId: "overview.why_value", tabName: "Overview", sectionName: "Why Value Is Strongest", componentType: "bullet_list", componentLabel: "Why Value Is Strongest", slotKey: "overview.why_value", minWords: 40, minBullets: 3 },
  { fieldId: "overview.proof.1", tabName: "Overview", sectionName: "Proof Points", componentType: "proof_card", componentLabel: "Proof Point 1", slotKey: "overview.proof.1", minWords: 35 },
  { fieldId: "overview.proof.2", tabName: "Overview", sectionName: "Proof Points", componentType: "proof_card", componentLabel: "Proof Point 2", slotKey: "overview.proof.2", minWords: 35 },
  { fieldId: "overview.proof.3", tabName: "Overview", sectionName: "Proof Points", componentType: "proof_card", componentLabel: "Proof Point 3", slotKey: "overview.proof.3", minWords: 35 },
  { fieldId: "overview.proof.4", tabName: "Overview", sectionName: "Proof Points", componentType: "proof_card", componentLabel: "Proof Point 4", slotKey: "overview.proof.4", minWords: 35 },
  { fieldId: "overview.featured_application", tabName: "Overview", sectionName: "Featured Application / Conversion Example", componentType: "featured_card", componentLabel: "Featured Application", slotKey: "overview.featured_application", minWords: 30, requireCaseSummary: true },
  { fieldId: "overview.differentiators.identity", tabName: "Overview", sectionName: "Key Differentiators", componentType: "bullet_list", componentLabel: "Experience & Identity", slotKey: "overview.differentiators.identity", minWords: 20, minBullets: 3 },
  { fieldId: "overview.differentiators.commercial", tabName: "Overview", sectionName: "Key Differentiators", componentType: "bullet_list", componentLabel: "Commercial & Distribution", slotKey: "overview.differentiators.commercial", minWords: 20, minBullets: 3 },
  { fieldId: "overview.bestAt.1", tabName: "Overview", sectionName: "What This Brand Is Best At", componentType: "card", componentLabel: "Best At 1", slotKey: "overview.bestAt.1", minWords: 12 },
  { fieldId: "overview.bestAt.2", tabName: "Overview", sectionName: "What This Brand Is Best At", componentType: "card", componentLabel: "Best At 2", slotKey: "overview.bestAt.2", minWords: 12 },
  { fieldId: "overview.bestAt.3", tabName: "Overview", sectionName: "What This Brand Is Best At", componentType: "card", componentLabel: "Best At 3", slotKey: "overview.bestAt.3", minWords: 12 },
  { fieldId: "overview.portfolio_context", tabName: "Overview", sectionName: "Portfolio Context", componentType: "narrative", componentLabel: "Portfolio Context", slotKey: "overview.portfolio_context", minWords: 25 },
]);

export const FOOTPRINT_FIELDS = Object.freeze([
  { fieldId: "footprint.geo_intro", tabName: "Footprint & Growth", sectionName: "Geographic Footprint", componentType: "narrative", componentLabel: "Geographic Footprint Intro", slotKey: "footprint.geo_intro", minWords: 30 },
  { fieldId: "footprint.primary_regions", tabName: "Footprint & Growth", sectionName: "Presence Intelligence", componentType: "metric_card", componentLabel: "Primary Regions", derived: "primary_regions", minWords: 1 },
  { fieldId: "footprint.growth_themes", tabName: "Footprint & Growth", sectionName: "Growth Priorities", componentType: "chips", componentLabel: "Growth Themes", slotKey: "footprint.growth_themes", minWords: 4, minChips: 2 },
  { fieldId: "footprint.growth_editorial", tabName: "Footprint & Growth", sectionName: "Growth Priorities", componentType: "narrative", componentLabel: "Growth Editorial", slotKey: "footprint.growth_editorial", minWords: 30 },
  { fieldId: "footprint.growth_fit", tabName: "Footprint & Growth", sectionName: "Growth Priorities", componentType: "narrative", componentLabel: "Most Likely Growth Fit", slotKey: "footprint.growth_fit", minWords: 20 },
]);

export const LIFECYCLE_FIELDS = Object.freeze([
  { fieldId: "valueOwners.lifecycle.1", tabName: "Value to Owners", sectionName: "Support Across the Lifecycle", componentType: "phase_box", componentLabel: "Phase 1 · Evaluation", slotKey: "valueOwners.lifecycle.1", minWords: 35 },
  { fieldId: "valueOwners.lifecycle.2", tabName: "Value to Owners", sectionName: "Support Across the Lifecycle", componentType: "phase_box", componentLabel: "Phase 2 · Conversion Design", slotKey: "valueOwners.lifecycle.2", minWords: 35 },
  { fieldId: "valueOwners.lifecycle.3", tabName: "Value to Owners", sectionName: "Support Across the Lifecycle", componentType: "phase_box", componentLabel: "Phase 3 · Pre-Opening", slotKey: "valueOwners.lifecycle.3", minWords: 35 },
  { fieldId: "valueOwners.lifecycle.4", tabName: "Value to Owners", sectionName: "Support Across the Lifecycle", componentType: "phase_box", componentLabel: "Phase 4 · Opening", slotKey: "valueOwners.lifecycle.4", minWords: 35 },
  { fieldId: "valueOwners.lifecycle.5", tabName: "Value to Owners", sectionName: "Support Across the Lifecycle", componentType: "phase_box", componentLabel: "Phase 5 · Ramp-Up", slotKey: "valueOwners.lifecycle.5", minWords: 35 },
  { fieldId: "valueOwners.lifecycle.6", tabName: "Value to Owners", sectionName: "Support Across the Lifecycle", componentType: "phase_box", componentLabel: "Phase 6 · Ongoing", slotKey: "valueOwners.lifecycle.6", minWords: 35 },
]);

export const FLEXIBILITY_FIELDS = Object.freeze([
  { fieldId: "operations.flexibility.design", tabName: "Operations & Standards", sectionName: "Flexibility Indicators", componentType: "indicator_bar", componentLabel: "Design Flexibility", slotKey: "operations.flexibility.design", minWords: 1 },
  { fieldId: "operations.flexibility.conversion", tabName: "Operations & Standards", sectionName: "Flexibility Indicators", componentType: "indicator_bar", componentLabel: "Conversion Friendliness", slotKey: "operations.flexibility.conversion", minWords: 1 },
  { fieldId: "operations.flexibility.localization", tabName: "Operations & Standards", sectionName: "Flexibility Indicators", componentType: "indicator_bar", componentLabel: "Localization Flexibility", slotKey: "operations.flexibility.localization", minWords: 1 },
  { fieldId: "operations.flexibility.operational_rigidity", tabName: "Operations & Standards", sectionName: "Flexibility Indicators", componentType: "indicator_bar", componentLabel: "Operational Rigidity", slotKey: "operations.flexibility.operational_rigidity", minWords: 1 },
  { fieldId: "operations.flexibility.pip", tabName: "Operations & Standards", sectionName: "Flexibility Indicators", componentType: "indicator_bar", componentLabel: "PIP Sensitivity", slotKey: "operations.flexibility.pip", minWords: 1 },
  { fieldId: "operations.flexibility.prototype", tabName: "Operations & Standards", sectionName: "Flexibility Indicators", componentType: "indicator_bar", componentLabel: "Prototype Dependence", slotKey: "operations.flexibility.prototype", minWords: 1 },
]);

export const COMPLIANCE_FIELDS = Object.freeze([
  { fieldId: "operations.compliance.qa_cadence", tabName: "Operations & Standards", sectionName: "Compliance & Oversight", componentType: "card", componentLabel: "QA Cadence", slotKey: "operations.compliance.qa_cadence", minWords: 15 },
  { fieldId: "operations.compliance.training_rigor", tabName: "Operations & Standards", sectionName: "Compliance & Oversight", componentType: "card", componentLabel: "Training Rigor", slotKey: "operations.compliance.training_rigor", minWords: 15 },
  { fieldId: "operations.compliance.reporting", tabName: "Operations & Standards", sectionName: "Compliance & Oversight", componentType: "card", componentLabel: "Reporting Expectations", slotKey: "operations.compliance.reporting", minWords: 15 },
  { fieldId: "operations.compliance.brand_interaction", tabName: "Operations & Standards", sectionName: "Compliance & Oversight", componentType: "card", componentLabel: "Brand Interaction Frequency", slotKey: "operations.compliance.brand_interaction", minWords: 15 },
]);

export const OPENING_PATH_FIELDS = Object.freeze([
  { fieldId: "economics.opening.step.1", tabName: "Economics & Obligations", sectionName: "Opening & Conversion Path", componentType: "phase_box", componentLabel: "Application & Feasibility", slotKey: "economics.opening.step.1", minWords: 30 },
  { fieldId: "economics.opening.step.2", tabName: "Economics & Obligations", sectionName: "Opening & Conversion Path", componentType: "phase_box", componentLabel: "Design & Standards", slotKey: "economics.opening.step.2", minWords: 30 },
  { fieldId: "economics.opening.step.3", tabName: "Economics & Obligations", sectionName: "Opening & Conversion Path", componentType: "phase_box", componentLabel: "Pre-Opening Planning", slotKey: "economics.opening.step.3", minWords: 30 },
  { fieldId: "economics.opening.step.4", tabName: "Economics & Obligations", sectionName: "Opening & Conversion Path", componentType: "phase_box", componentLabel: "Opening Support", slotKey: "economics.opening.step.4", minWords: 30 },
  { fieldId: "economics.opening.step.5", tabName: "Economics & Obligations", sectionName: "Opening & Conversion Path", componentType: "phase_box", componentLabel: "Stabilization", slotKey: "economics.opening.step.5", minWords: 30 },
]);

export const MOMENTUM_AND_MIX_FIELDS = Object.freeze([
  { fieldId: "footprint.momentum", tabName: "Footprint & Growth", sectionName: "Recent Momentum", componentType: "narrative", componentLabel: "Recent Momentum", slotKey: "footprint.momentum", minWords: 20, suppressible: true },
  { fieldId: "footprint.portfolio_mix", tabName: "Footprint & Growth", sectionName: "Portfolio Mix", componentType: "chips", componentLabel: "Portfolio Mix", slotKey: "footprint.portfolio_mix", minWords: 2, minChips: 1, suppressible: true },
]);

export const SIMILAR_BRAND_FIELDS = Object.freeze([
  { fieldId: "insight.similar", tabName: "Dealality Insight", sectionName: "Similar Brands", componentType: "table_rows", componentLabel: "Similar Brand Cards", slotKey: "insight.similar", minRows: 2 },
]);

export const OPERATIONS_MODEL_FIELDS = Object.freeze([
  { fieldId: "operations.model.primary_model", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Primary Model", slotKey: "operations.model.primary_model", minWords: 8 },
  { fieldId: "operations.model.management_option", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Management Option", slotKey: "operations.model.management_option", minWords: 8 },
  { fieldId: "operations.model.typical_ownership", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Typical Ownership Structure", slotKey: "operations.model.typical_ownership", minWords: 8 },
  { fieldId: "operations.model.brand_involvement", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Brand Involvement", slotKey: "operations.model.brand_involvement", minWords: 8 },
  { fieldId: "operations.model.systems_integration", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Systems Integration", slotKey: "operations.model.systems_integration", minWords: 8 },
  { fieldId: "operations.model.pre_opening", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Pre-opening Discipline", slotKey: "operations.model.pre_opening", minWords: 8 },
  { fieldId: "operations.model.staffing_intensity", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Staffing Intensity", slotKey: "operations.model.staffing_intensity", minWords: 8 },
  { fieldId: "operations.model.fb_complexity", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "F&B Complexity", slotKey: "operations.model.fb_complexity", minWords: 8 },
  { fieldId: "operations.model.training", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Training Requirements", slotKey: "operations.model.training", minWords: 8 },
  { fieldId: "operations.model.reporting_discipline", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Reporting Discipline", slotKey: "operations.model.reporting_discipline", minWords: 8 },
  { fieldId: "operations.model.qa_rhythm", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "QA Rhythm", slotKey: "operations.model.qa_rhythm", minWords: 8 },
  { fieldId: "operations.model.technology", tabName: "Operations & Standards", sectionName: "Operating Model", componentType: "kv", componentLabel: "Technology Expectations", slotKey: "operations.model.technology", minWords: 8 },
  { fieldId: "operations.standards_philosophy", tabName: "Operations & Standards", sectionName: "Standards Philosophy", componentType: "card", componentLabel: "Philosophy", slotKey: "operations.standards_philosophy", minWords: 30 },
  { fieldId: "operations.operator_compat.summary", tabName: "Operations & Standards", sectionName: "Third-Party Operator Compatibility", componentType: "card", componentLabel: "Summary", slotKey: "operations.operator_compat.summary", minWords: 20 },
  { fieldId: "operations.operator_compat.fit", tabName: "Operations & Standards", sectionName: "Third-Party Operator Compatibility", componentType: "card", componentLabel: "Fit", slotKey: "operations.operator_compat.fit", minWords: 20 },
  { fieldId: "operations.operator_compat.tags", tabName: "Operations & Standards", sectionName: "Third-Party Operator Compatibility", componentType: "chips", componentLabel: "Compatibility Tags", slotKey: "operations.operator_compat.tags", minWords: 3, minChips: 2 },
]);

export const OWNER_CONSIDERATIONS_FIELDS = Object.freeze([
  { fieldId: "standards.requirement", tabName: "Owner Considerations", sectionName: "Standard Detail, Where Available", componentType: "table_rows", componentLabel: "Standards Checklist", slotKey: "standards.requirement", minRows: 6 },
  { fieldId: "standards.questions", tabName: "Owner Considerations", sectionName: "Confirm With Brand", componentType: "bullet_list", componentLabel: "Questions Owners Should Ask", slotKey: "standards.questions", minWords: 30, minBullets: 5 },
]);

/** Full tab-factory inventory (every owner-facing contract field). */
export const ALL_INVENTORY_FIELDS = Object.freeze([
  ...SNAPSHOT_KV_FIELDS,
  ...POSITIONING_FIELDS,
  ...OVERVIEW_CONTENT_SLOTS,
  ...LIFECYCLE_FIELDS,
  ...FOOTPRINT_FIELDS,
  ...MOMENTUM_AND_MIX_FIELDS,
  ...OPERATIONS_MODEL_FIELDS,
  ...FLEXIBILITY_FIELDS,
  ...COMPLIANCE_FIELDS,
  ...OPENING_PATH_FIELDS,
  ...OWNER_CONSIDERATIONS_FIELDS,
  ...SIMILAR_BRAND_FIELDS,
]);

export const TARGET_BRANDS = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
]);

export const PROTECTED_BRANDS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  "design-hotels",
  "tribute-portfolio",
]);

export const BENCHMARK_BRANDS = Object.freeze([
  "tribute-portfolio",
  "kimpton",
  "radisson-individuals-by-choice",
  "design-hotels",
]);
