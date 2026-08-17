/**
 * Full Operator Explorer field catalog for Partner Intelligence (10 publishable tabs).
 * Scalar + JSON blob targets aligned to prefill / explorerProfileJson keys.
 */
import { DNA_EXPLORER_JSON_FIELD_SPECS } from "../operator-dna-explorer-json-fields.js";

/** @typedef {'overview_en'|'overview_es'|'regional_deck'|'public_web'|'any'} SourceRole */

const TAB = {
  profile: "Profile & Positioning",
  operating: "Operating Platform",
  brand: "Brand & Relationships",
  markets: "Markets & Footprint",
  engagement: "Owner Engagement & Reporting",
  infrastructure: "Infrastructure & Data",
  leadership: "Leadership",
  dealFit: "Project Fit & Deal Profile",
  proof: "Proof & Track Record",
  materials: "Operator Materials",
};

/** Setup tab label → Explorer tab */
const SETUP_TAB_TO_EXPLORER = {
  "2. Operating Platform": TAB.operating,
  "3. Brand & Relationships": TAB.brand,
  "4. Markets & Footprint": TAB.markets,
  "5. Owner Value & Engagement": TAB.engagement,
  "9. Best Fit & Preferences": TAB.dealFit,
};

function scalar(fieldKey, explorerTab, explorerSection, displayLabel, prefillKey, opts = {}) {
  return {
    fieldKey,
    explorerType: "Operator Explorer",
    explorerTab,
    explorerSection,
    displayLabel,
    prefillKey,
    responsePath: opts.responsePath || `prefill.${prefillKey}`,
    publishScope: true,
    valueType: opts.valueType || "text",
    sourceRoles: opts.sourceRoles || ["overview_en", "public_web"],
    allowGapCopy: opts.allowGapCopy !== false,
  };
}

function jsonField(fieldKey, explorerTab, explorerSection, displayLabel, prefillKey, opts = {}) {
  return scalar(fieldKey, explorerTab, explorerSection, displayLabel, prefillKey, {
    ...opts,
    valueType: "json",
    sourceRoles: opts.sourceRoles || ["overview_en"],
  });
}

/** Hand-curated scalars across all 10 tabs */
const SCALAR_FIELDS = [
  // Tab 1 — Profile
  scalar("op.snapshot.companyName", TAB.profile, "Company Snapshot", "Company Name", "companyName", {
    sourceRoles: ["overview_en", "overview_es", "public_web"],
  }),
  scalar("op.snapshot.companyDescription", TAB.profile, "Company Snapshot", "Company Description", "companyDescription"),
  scalar("op.snapshot.companyHistory", TAB.profile, "Company Story", "Company History", "companyHistory"),
  scalar("op.snapshot.yearEstablished", TAB.profile, "Company Snapshot", "Year Established", "yearEstablished", {
    valueType: "number",
  }),
  scalar("op.snapshot.missionStatement", TAB.profile, "Company Story", "Mission Statement", "missionStatement"),
  scalar("op.snapshot.differentiators", TAB.profile, "Differentiators", "Differentiators", "differentiators"),
  scalar("op.snapshot.managementPhilosophy", TAB.profile, "Company Story", "Management Philosophy", "managementPhilosophy"),
  scalar("op.snapshot.parentCompany", TAB.profile, "Ownership / Corporate Structure", "Parent Company", "parentCompany"),
  scalar("op.snapshot.primaryServiceModel", TAB.profile, "Company Snapshot", "Primary Service Model", "primaryServiceModel"),
  scalar("op.snapshot.totalProperties", TAB.profile, "Portfolio Size", "Total Properties", "totalProperties", {
    valueType: "number",
  }),
  scalar("op.snapshot.totalRooms", TAB.profile, "Portfolio Size", "Total Rooms", "totalRooms", { valueType: "number" }),
  scalar("op.snapshot.website", TAB.profile, "Company Snapshot", "Website", "website", {
    sourceRoles: ["public_web", "overview_en"],
  }),
  scalar("op.snapshot.certifications", TAB.profile, "Recognition", "Certifications", "certifications"),
  scalar("op.snapshot.industryRecognition", TAB.profile, "Recognition", "Industry Recognition", "industryRecognition"),
  scalar("op.snapshot.notableAchievements", TAB.profile, "Recognition", "Notable Achievements", "notableAchievements"),
  scalar("op.snapshot.companySize", TAB.profile, "Company Snapshot", "Company Size / Employees", "companySize"),

  // Tab 2 — Operating Platform (capability singles)
  scalar("op.platform.revenueManagement", TAB.operating, "Revenue Management", "Revenue Management Capability", "revenueManagementCapability"),
  scalar("op.platform.preOpeningSupport", TAB.operating, "Pre-Opening Support", "Pre-Opening Support Capability", "preOpeningSupportCapability"),
  scalar("op.platform.conversionExperience", TAB.operating, "Conversion Support", "Conversion / Repositioning Experience", "conversionReflagExperience"),
  scalar("op.platform.ownerReporting", TAB.operating, "Owner Reporting", "Owner Reporting Level", "ownerReportingLevel"),
  scalar("op.platform.fbResortCapability", TAB.operating, "F&B & Resort", "F&B / Lifestyle / Resort Capability", "fbCapabilityLevel"),
  scalar("op.platform.offeredServices", TAB.operating, "Operating Platform", "Offered Services", "offeredServices"),

  // Tab 3 — Brand
  scalar("op.brand.familiesOperated", TAB.brand, "Brand Relationships", "Brand Families Operated", "brandFamiliesOperated"),
  scalar("op.brand.brandedVsIndependentMix", TAB.brand, "Independent / Soft Brand", "Branded vs Independent Mix", "brandedVsIndependentMix"),
  scalar("op.brand.softIndependentNarrative", TAB.brand, "Independent / Soft Brand", "Soft Brand / Independent Narrative", "brand_soft_independent_narrative"),

  // Tab 4 — Markets
  scalar("op.markets.activeCountries", TAB.markets, "Regional Presence", "Active Countries", "activeCountries", {
    sourceRoles: ["overview_en", "regional_deck", "public_web"],
  }),
  scalar("op.markets.activeMarkets", TAB.markets, "Regional Presence", "Active Markets", "activeMarkets", {
    sourceRoles: ["regional_deck", "overview_en"],
  }),
  scalar("op.markets.geographicPriorities", TAB.markets, "Geographic Priorities", "Priority / Target Markets", "priorityMarkets", {
    sourceRoles: ["overview_en", "regional_deck"],
  }),
  scalar("op.markets.targetGrowthMarkets", TAB.markets, "Geographic Priorities", "Target Growth Markets", "targetGrowthMarkets", {
    sourceRoles: ["overview_en", "regional_deck"],
  }),
  scalar("op.markets.teamExperienceMarkets", TAB.markets, "Market Experience", "Team Experience Markets", "teamExperienceMarkets", {
    sourceRoles: ["regional_deck", "overview_en"],
  }),
  scalar("op.markets.regionsSupported", TAB.markets, "Regional Presence", "Regions Supported", "regions", {
    sourceRoles: ["overview_en", "regional_deck"],
  }),
  jsonField("op.markets.regionalPortfolio", TAB.markets, "Regional Portfolio", "Regional Portfolio (structured)", "markets_regional_portfolio_json", {
    sourceRoles: ["regional_deck"],
    responsePath: "prefill.markets_regional_portfolio_json",
  }),

  // Tab 5 — Engagement
  scalar("op.engagement.ownerReportingLevel", TAB.engagement, "Owner Reporting", "Owner Reporting Level", "ownerReportingLevel"),
  scalar("op.engagement.reportingFrequency", TAB.engagement, "Owner Reporting", "Reporting Frequency / Cadence", "ownerReportingCadence"),
  scalar("op.engagement.ownerResponseTime", TAB.engagement, "Owner Engagement", "Owner Response Time", "ownerResponseTime"),
  scalar("op.engagement.reportTypes", TAB.engagement, "Reports", "Report Types", "reportTypes"),
  scalar("op.engagement.ownerPortalFeatures", TAB.engagement, "Owner Tools", "Owner Portal Features", "ownerPortalFeatures"),
  scalar("op.engagement.operatingCollaborationMode", TAB.engagement, "Owner Engagement", "Operating Collaboration Mode", "operatingCollaborationMode"),

  // Tab 6 — Infrastructure
  scalar("op.infrastructure.technologyMaturityLevel", TAB.infrastructure, "Technology Maturity", "Technology Maturity Level", "infra_technology_maturity_level"),
  jsonField("op.infrastructure.technologyStack", TAB.infrastructure, "Technology Stack", "Technology Platform Stack", "infra_technology_stack_json"),
  jsonField("op.infrastructure.servicesOffered", TAB.infrastructure, "Infrastructure Services", "Infrastructure Services Offered", "infra_services_offered_json"),
  jsonField("op.infrastructure.dataDomains", TAB.infrastructure, "Data Domains", "Data Domains Captured", "infra_data_domains_json"),
  jsonField("op.infrastructure.dataGovernance", TAB.infrastructure, "Data Governance", "Data Governance & Security", "infra_data_governance_json"),
  jsonField("op.infrastructure.analyticsSupport", TAB.infrastructure, "Analytics", "Analytics & Decision Support", "infra_analytics_support_json"),

  // Tab 7 — Leadership
  jsonField("op.leadership.orgStructure", TAB.leadership, "Organization", "Organization Structure", "lead_org_structure_json"),
  jsonField("op.leadership.teamDepth", TAB.leadership, "Team Depth", "Team Depth by Function", "lead_team_depth_json"),
  jsonField("op.leadership.languages", TAB.leadership, "Languages", "Language & Regional Capability", "lead_language_capability_json"),
  jsonField("op.leadership.governanceCadence", TAB.leadership, "Governance", "Governance & Communication Cadence", "lead_governance_cadence_json"),
  jsonField("op.leadership.teamMarkets", TAB.leadership, "Team Markets", "Team Experience Markets", "lead_team_markets_json"),
  jsonField("op.leadership.ownerRelationship", TAB.leadership, "Owner Relationship", "Owner Relationship Model", "lead_owner_relationship_json"),
  jsonField("op.leadership.executives", TAB.leadership, "Leadership Profiles", "Executive Profiles (JSON array)", "leadership_executives_json", {
    sourceRoles: ["overview_en", "overview_es"],
  }),

  // Tab 8 — Deal Fit
  scalar("op.dealFit.bestFitOwnerTypes", TAB.dealFit, "Deal Fit Profile", "Best-Fit Owner Types", "bestFitOwnerTypes"),
  scalar("op.dealFit.minPropertySize", TAB.dealFit, "Hotel Size", "Minimum Property Size", "minPropertySize", { valueType: "number" }),
  scalar("op.dealFit.maxPropertySize", TAB.dealFit, "Hotel Size", "Maximum Property Size", "maxPropertySize", { valueType: "number" }),
  scalar("op.dealFit.bestFitGeographies", TAB.dealFit, "Deal Fit Profile", "Best-Fit Geographies", "bestFitGeographies"),
  scalar("op.dealFit.lessIdealSituations", TAB.dealFit, "Deal Fit Profile", "Less Ideal Situations", "lessIdealSituations"),

  // Tab 9 — Proof
  scalar("op.proof.yearsInBusiness", TAB.proof, "Track Record", "Years in Business", "yearsInBusiness", { valueType: "number" }),
  scalar("op.proof.ownerReferences", TAB.proof, "References", "Owner References", "ownerReferences"),
  scalar("op.proof.lenderReferences", TAB.proof, "References", "Lender References", "lenderReferences"),
  jsonField("op.proof.caseStudies", TAB.proof, "Case Studies", "Case Studies (JSON array)", "case_studies_json", {
    sourceRoles: ["overview_en", "overview_es"],
  }),
  jsonField("op.proof.diligenceHighlights", TAB.proof, "Diligence", "Owner Diligence Q&A Highlights", "owner_diligence_json", {
    sourceRoles: ["overview_en"],
  }),

  // Tab 10 — Materials
  scalar("op.materials.galleryOverview", TAB.materials, "Materials", "Materials Overview", "operatorMaterialsOverview", {
    sourceRoles: ["overview_en"],
    allowGapCopy: true,
  }),
];

/** DNA JSON blobs from operator-dna-explorer-json-fields.js */
function dnaJsonFields() {
  const out = [];
  for (const spec of DNA_EXPLORER_JSON_FIELD_SPECS) {
    const explorerTab = SETUP_TAB_TO_EXPLORER[spec.setupTab] || TAB.profile;
    const section = spec.label;
    const fieldKey = `op.json.${spec.formKey}`;
    out.push(
      jsonField(fieldKey, explorerTab, section, spec.label, spec.formKey, {
        sourceRoles: spec.setupTab.includes("Markets") ? ["overview_en", "regional_deck"] : ["overview_en"],
      })
    );
  }
  return out;
}

/** Meta rollup fields (publish workflow; not LLM-extracted) */
const META_FIELDS = [
  {
    fieldKey: "op.meta.overallSourceConfidence",
    explorerType: "Operator Explorer",
    explorerTab: TAB.profile,
    explorerSection: "Overall Source Confidence",
    displayLabel: "Overall Source Confidence",
    publishScope: true,
    allowGapCopy: false,
    valueType: "text",
    sourceRoles: ["any"],
  },
  {
    fieldKey: "op.meta.lastReviewedDate",
    explorerType: "Operator Explorer",
    explorerTab: TAB.profile,
    explorerSection: "Last Reviewed Date",
    displayLabel: "Last Reviewed Date",
    publishScope: true,
    allowGapCopy: false,
    valueType: "text",
    sourceRoles: ["any"],
  },
  {
    fieldKey: "op.meta.dataGaps",
    explorerType: "Operator Explorer",
    explorerTab: TAB.profile,
    explorerSection: "Data Gaps",
    displayLabel: "Data Gaps",
    publishScope: true,
    allowGapCopy: true,
    valueType: "text",
    sourceRoles: ["any"],
  },
  {
    fieldKey: "op.meta.questionsToConfirm",
    explorerType: "Operator Explorer",
    explorerTab: TAB.profile,
    explorerSection: "Questions to Confirm",
    displayLabel: "Questions to Confirm",
    publishScope: true,
    allowGapCopy: true,
    valueType: "text",
    sourceRoles: ["any"],
  },
  {
    fieldKey: "op.meta.watchouts",
    explorerType: "Operator Explorer",
    explorerTab: TAB.profile,
    explorerSection: "Watchouts",
    displayLabel: "Watchouts",
    publishScope: true,
    allowGapCopy: true,
    valueType: "text",
    sourceRoles: ["any"],
  },
];

export function buildFullOperatorExplorerRegistry() {
  const byKey = new Map();
  for (const f of [...SCALAR_FIELDS, ...dnaJsonFields(), ...META_FIELDS]) {
    byKey.set(f.fieldKey, f);
  }
  return [...byKey.values()];
}

export function listLlmExtractableFields(registry) {
  return registry.filter((f) => f.publishScope && !f.fieldKey.startsWith("op.meta."));
}

export { TAB, SETUP_TAB_TO_EXPLORER };
