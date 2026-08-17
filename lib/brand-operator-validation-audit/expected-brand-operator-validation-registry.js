/**
 * Expected governance / validation field specs for Brand + Operator profile tables.
 * Read-only audit input — does not mutate Airtable schema.
 *
 * @see docs/data-intelligence/brand-operator-validation-fields-plan.md
 */
import {
  PARTNER_INTELLIGENCE_TABLES,
  MAP_PARTNER_SOURCE,
  MAP_PARTNER_FACT,
  MAP_PARTNER_PUBLISHED,
  MAP_PARTNER_HELENA,
} from "../../api/lib/partner-intelligence-field-map.js";
import { CENSUS_FIELDS, ALIAS_FIELDS, HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE } from "../../lib/hotel-census/fields.js";
import {
  buildP1ProfileGovernanceFieldDefs,
  P1_GOVERNANCE_FIELD_ALIASES,
  P1_EXCLUDED_FROM_SETUP_ROOTS,
} from "./p1-profile-governance-field-specs.js";

/** @param {string} name @param {{ aliases?: string[], classification?: string, notes?: string, tier?: string }} [opts] */
export function governanceFieldSpec(name, opts = {}) {
  return {
    name,
    aliases: opts.aliases || [],
    classification: opts.classification || "Recommended Governance",
    notes: opts.notes || "",
    tier: opts.tier || "core",
  };
}

/** Core profile-level governance fields (brand-operator-validation-fields-plan.md). */
export const CORE_GOVERNANCE_FIELD_SPECS = [
  governanceFieldSpec("Validation Status", {
    classification: "Recommended Governance — P1",
    notes: "Company Validated, Source-Informed, AI-Assisted, Needs Review, etc.",
  }),
  governanceFieldSpec("Usage Permission", {
    classification: "Recommended Governance — P1",
    notes: "Internal Only, Platform Display Allowed, Scoring Allowed, etc.",
  }),
  governanceFieldSpec("Source Type", {
    classification: "Recommended Governance",
    notes: "Document/channel class; partial live on Operator Master.",
  }),
  governanceFieldSpec("Source Date", { classification: "Recommended Governance" }),
  governanceFieldSpec("Source Region", {
    aliases: ["Region"],
    classification: "Recommended Governance",
    notes: "Partner Intelligence uses Region on Source Library.",
  }),
  governanceFieldSpec("Source URL / File Path", {
    aliases: ["Source URL", "Source File", "Local File Path"],
    classification: "Recommended Governance",
  }),
  governanceFieldSpec("Last Reviewed Date", {
    aliases: ["Last Reviewed", "Profile Last Reviewed"],
    classification: "Recommended Governance",
  }),
  governanceFieldSpec("Refresh Due Date", { classification: "Recommended Governance — P2" }),
  governanceFieldSpec("Confidence Level", {
    aliases: ["Data Confidence Level", "Overall Source Confidence", "Confidence Score"],
    classification: "Recommended Governance",
  }),
  governanceFieldSpec("Evidence Notes", {
    aliases: ["Evidence Text"],
    classification: "Recommended Governance",
  }),
  governanceFieldSpec("Missing Data Flags", {
    aliases: ["Data Gap?", "Missing Data"],
    classification: "Recommended Governance",
  }),
  governanceFieldSpec("Company Validated", { classification: "Recommended Governance — P1" }),
  governanceFieldSpec("Company Validation Date", { classification: "Recommended Governance — P1" }),
  governanceFieldSpec("Reviewed By", { classification: "Recommended Governance" }),
  governanceFieldSpec("External Display Status", {
    aliases: ["Public Visibility", "Public / Private / Restricted"],
    classification: "Recommended Governance — P2",
  }),
  governanceFieldSpec("Internal Notes", {
    aliases: ["Notes", "Reviewer Notes", "Confidentiality Notes"],
    classification: "Recommended Governance",
    notes: "Generic Notes exists on many tables — alias match is informational only.",
  }),
];

/** P1 approved profile governance on Setup root tables (excludes Source URL/Date — PI SSOT). */
export function buildP1ProfileGovernanceFieldSpecs() {
  return buildP1ProfileGovernanceFieldDefs().map((f) =>
    governanceFieldSpec(f.name, {
      classification: "P1 Profile Governance — approved",
      tier: "p1",
      aliases: P1_GOVERNANCE_FIELD_ALIASES[f.name]?.aliases || [],
      notes: P1_GOVERNANCE_FIELD_ALIASES[f.name]?.reason || "",
    })
  );
}

export { P1_EXCLUDED_FROM_SETUP_ROOTS };

/** Close equivalents / workflow fields to detect without treating as full governance rollout. */
export const ALIAS_EQUIVALENT_FIELD_SPECS = [
  governanceFieldSpec("Data Confidence Level", {
    tier: "alias",
    aliases: ["Data Confidence"],
    notes: "Live on Operator Setup - Master per alignment audits.",
  }),
  governanceFieldSpec("Last Updated Date", {
    tier: "alias",
    aliases: ["Last Updated", "Last Updated At"],
    notes: "Live on Operator Master; distinct from Last Reviewed Date.",
  }),
  governanceFieldSpec("Profile Last Reviewed", {
    tier: "alias",
    aliases: ["Last Reviewed Date"],
    notes: "Recommended on Operator Master.",
  }),
  governanceFieldSpec("Human Review Status", { tier: "alias", notes: "Partner Intelligence Extracted Facts." }),
  governanceFieldSpec("Publish Status", { tier: "alias", notes: "Partner Intelligence Published Explorer Fields." }),
  governanceFieldSpec("Verified Source?", { tier: "alias", notes: "Partner Intelligence Source Library." }),
  governanceFieldSpec("Source Quality", { tier: "alias", notes: "Partner Intelligence Source/Facts." }),
  governanceFieldSpec("Stale?", { tier: "alias", notes: "Partner Intelligence Published rows." }),
  governanceFieldSpec("Include in Brand Explorer", {
    tier: "alias",
    notes: "Hotel Census governance (ALT base).",
  }),
  governanceFieldSpec("Match Confidence", { tier: "alias", notes: "Brand Alias Mapping." }),
  governanceFieldSpec("Active", { tier: "alias", notes: "Brand Alias Mapping row gate." }),
  governanceFieldSpec("Explorer Hero Verification", {
    tier: "alias",
    notes: "Brand Basics — hero trust signal in brand-library.js.",
  }),
  governanceFieldSpec("Explorer Hero Data Source", {
    tier: "alias",
    notes: "Brand Basics — hero source label in brand-library.js.",
  }),
  governanceFieldSpec("readyForInvestorPublication", {
    tier: "alias",
    notes: "Operator Profile visibility gate (not validation status).",
  }),
  governanceFieldSpec("submission_status", {
    tier: "alias",
    notes: "Operator Master workflow gate (Active for Explorer).",
  }),
  governanceFieldSpec("Last Computed At", {
    tier: "alias",
    notes: "Deal Brand Cache platform cache timestamp.",
  }),
];

/**
 * @param {object} spec
 * @param {string[]} liveNames
 * @returns {{ kind: 'exact'|'alias'|'missing', live: string|null, matchedAlias?: string }}
 */
export function matchSpecToLive(spec, liveNames) {
  const liveSet = new Set(liveNames);
  if (liveSet.has(spec.name)) return { kind: "exact", live: spec.name };
  for (const alias of spec.aliases || []) {
    if (liveSet.has(alias)) return { kind: "alias", live: alias, matchedAlias: alias };
  }
  const norm = (s) => String(s).trim().toLowerCase();
  const byNorm = liveNames.find((n) => norm(n) === norm(spec.name));
  if (byNorm) return { kind: "exact", live: byNorm };
  for (const alias of spec.aliases || []) {
    const hit = liveNames.find((n) => norm(n) === norm(alias));
    if (hit) return { kind: "alias", live: hit, matchedAlias: alias };
  }
  return { kind: "missing", live: null };
}

function piFieldSpecs(mapObj, classification) {
  return [...new Set(Object.values(mapObj))].map((name) =>
    governanceFieldSpec(name, {
      classification,
      tier: "partner-intelligence",
    })
  );
}

/** @typedef {'primary'|'alt'} AuditBaseId */

/**
 * @param {string} tableName
 * @param {{
 *   tableAliases?: string[],
 *   base?: AuditBaseId,
 *   category: string,
 *   optionalTable?: boolean,
 *   legacy?: boolean,
 *   setupPriority?: number,
 *   checkCoreGovernance?: boolean,
 *   checkP1ProfileGovernance?: boolean,
 *   checkAliasEquivalents?: boolean,
 *   fieldSpecs?: ReturnType<typeof governanceFieldSpec>[],
 *   sources: string[],
 *   notes?: string,
 * }} opts
 */
function tableConfig(tableName, opts) {
  return {
    tableName,
    tableAliases: opts.tableAliases || [],
    base: opts.base || "primary",
    category: opts.category,
    optionalTable: Boolean(opts.optionalTable),
    legacy: Boolean(opts.legacy),
    setupPriority: opts.setupPriority ?? 99,
    checkCoreGovernance: Boolean(opts.checkCoreGovernance),
    checkP1ProfileGovernance: Boolean(opts.checkP1ProfileGovernance),
    checkAliasEquivalents: opts.checkAliasEquivalents !== false,
    fieldSpecs: opts.fieldSpecs || [],
    sources: opts.sources,
    notes: opts.notes || "",
  };
}

const LEGACY_OPERATOR_TABLES = [
  "3rd Party Operator - Basics",
  "3rd Party Operator - Footprint",
  "3rd Party Operator - Performance & Operations",
  "3rd Party Operator - Service Offerings",
  "3rd Party Operator - Ideal Projects & Deal Fit",
  "3rd Party Operator - Owner Relations & Communication",
  "3rd Party Operator - Case Studies",
  "3rd Party Operator - Owner Diligence QA",
  "3rd Party Operator - Deal Terms & Fees",
];

/** Ordered keys for audit iteration. */
export const BRAND_OPERATOR_AUDIT_TABLE_KEYS = [
  // Brand — primary base
  "brandBasics",
  "brandFootprint",
  "brandProjectFit",
  "brandPortfolioPerformance",
  "brandStandards",
  "brandFeeStructure",
  "brandDealTerms",
  "brandOperationalSupport",
  "brandLegalTerms",
  "brandLoyaltyCommercial",
  "brandSustainabilityEsg",
  "brandExplorerPresentation",
  "dealBrandCache",
  // Operator — primary base
  "operatorMaster",
  "operatorProfile",
  "operatorPlatform",
  "operatorCommercial",
  "operatorGovernance",
  "operatorEngagement",
  "operatorExplorerMaterials",
  "operatorLeadershipPlatform",
  "operatorLeadershipTeam",
  "operatorCaseStudies",
  "operatorDiligenceQa",
  // Partner Intelligence + shared
  "partnerSourceLibrary",
  "partnerExtractedFacts",
  "partnerPublishedFields",
  "partnerHelenaIntake",
  "companyProfile",
  // ALT base
  "brandAliasMapping",
  "hotelCensus",
  // Legacy (optional)
  ...LEGACY_OPERATOR_TABLES.map((name) => `legacy_${name.replace(/[^a-zA-Z0-9]+/g, "_")}`),
];

export function buildExpectedBrandOperatorValidationRegistry() {
  const coreOnly = () => [...CORE_GOVERNANCE_FIELD_SPECS];

  const aliasMappingSpecs = [
    governanceFieldSpec(ALIAS_FIELDS.matchConfidence, { tier: "alias-mapping" }),
    governanceFieldSpec(ALIAS_FIELDS.notes, { tier: "alias-mapping" }),
    governanceFieldSpec(ALIAS_FIELDS.active, { tier: "alias-mapping" }),
    governanceFieldSpec(ALIAS_FIELDS.canonicalBrandName, { classification: "Identity" }),
    governanceFieldSpec(ALIAS_FIELDS.aliasSourceBrandName, { classification: "Identity" }),
  ];

  const censusGovernanceSpecs = [
    governanceFieldSpec(CENSUS_FIELDS.dataConfidence, { tier: "census" }),
    governanceFieldSpec(CENSUS_FIELDS.includeInBrandExplorer, { tier: "census" }),
  ];

  const companyProfileSpecs = [
    governanceFieldSpec("Company Validated", { classification: "Recommended Governance — minimal" }),
    governanceFieldSpec("Company Validation Date", { classification: "Recommended Governance — minimal" }),
    governanceFieldSpec("Internal Notes", { aliases: ["Notes"], classification: "Recommended Governance — minimal" }),
  ];

  const dealBrandCacheSpecs = [
    governanceFieldSpec("Last Computed At", { classification: "Platform cache metadata" }),
  ];

  const brandBasicsKnown = [
    governanceFieldSpec("Explorer Hero Verification", { tier: "known-brand" }),
    governanceFieldSpec("Explorer Hero Data Source", { tier: "known-brand" }),
  ];

  const operatorMasterKnown = [
    governanceFieldSpec("Data Confidence Level", { tier: "known-operator" }),
    governanceFieldSpec("Source Type", { tier: "known-operator" }),
    governanceFieldSpec("Last Updated Date", { tier: "known-operator" }),
    governanceFieldSpec("Profile Last Reviewed", { tier: "known-operator-recommended" }),
    governanceFieldSpec("submission_status", { tier: "workflow" }),
  ];

  const tables = {
    brandBasics: tableConfig("Brand Setup - Brand Basics", {
      category: "brand",
      setupPriority: 1,
      checkP1ProfileGovernance: true,
      fieldSpecs: [...brandBasicsKnown],
      sources: ["api/brand-library.js", "scripts/setup-brand-validation-fields.mjs"],
      notes: "P1 profile root — governance via setup-brand-validation-fields.mjs.",
    }),
    brandFootprint: tableConfig("Brand Setup - Brand Footprint", {
      tableAliases: ["Brand Setup - Footprint"],
      category: "brand",
      setupPriority: 3,
      sources: ["api/brand-library.js"],
    }),
    brandProjectFit: tableConfig("Brand Setup - Project Fit", {
      category: "brand",
      setupPriority: 3,
      sources: ["api/brand-library.js"],
    }),
    brandPortfolioPerformance: tableConfig("Brand Setup - Portfolio & Performance", {
      category: "brand",
      setupPriority: 3,
      sources: ["api/brand-library.js"],
    }),
    brandStandards: tableConfig("Brand Setup - Brand Standards", {
      category: "brand",
      setupPriority: 3,
      sources: ["api/brand-library.js"],
    }),
    brandFeeStructure: tableConfig("Brand Setup - Fee Structure", {
      category: "brand",
      setupPriority: 4,
      sources: ["api/brand-library.js"],
    }),
    brandDealTerms: tableConfig("Brand Setup - Deal Terms", {
      category: "brand",
      setupPriority: 4,
      sources: ["api/brand-library.js"],
    }),
    brandOperationalSupport: tableConfig("Brand Setup - Operational Support", {
      category: "brand",
      setupPriority: 4,
      sources: ["api/brand-library.js"],
    }),
    brandLegalTerms: tableConfig("Brand Setup - Legal Terms", {
      category: "brand",
      setupPriority: 4,
      sources: ["api/brand-library.js"],
    }),
    brandLoyaltyCommercial: tableConfig("Brand Setup - Loyalty & Commercial", {
      category: "brand",
      setupPriority: 4,
      sources: ["api/brand-library.js"],
    }),
    brandSustainabilityEsg: tableConfig("Brand Setup - Sustainability & ESG", {
      category: "brand",
      setupPriority: 4,
      sources: ["api/brand-library.js"],
    }),
    brandExplorerPresentation: tableConfig("Brand Setup - Brand Explorer Presentation", {
      category: "brand",
      setupPriority: 1,
      checkP1ProfileGovernance: true,
      sources: [
        "docs/brand-explorer-presentation-slots.md",
        "scripts/setup-brand-validation-fields.mjs",
      ],
      notes: "P1 presentation root — slot copy governance.",
    }),
    dealBrandCache: tableConfig(
      process.env.AIRTABLE_TABLE_DEAL_BRAND_CACHE || "Deal Brand Cache",
      {
        category: "brand",
        optionalTable: true,
        setupPriority: 8,
        checkCoreGovernance: false,
        fieldSpecs: dealBrandCacheSpecs,
        sources: ["docs/platform-reference/airtable-deals-fields.md", "api/my-deals.js"],
        notes: "Platform cache — not profile validation SSOT.",
      }
    ),
    operatorMaster: tableConfig(
      process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master",
      {
        category: "operator",
        setupPriority: 1,
        checkP1ProfileGovernance: true,
        fieldSpecs: [...operatorMasterKnown],
        sources: [
          "api/lib/operator-setup-new-base-writer.js",
          "scripts/setup-operator-validation-fields.mjs",
        ],
        notes: "P1 operator root — partial governance live; complete via setup script.",
      }
    ),
    operatorProfile: tableConfig("Operator Setup - Profile & Positioning", {
      category: "operator",
      setupPriority: 2,
      fieldSpecs: [
        governanceFieldSpec("readyForInvestorPublication", { tier: "workflow" }),
      ],
      sources: ["api/lib/operator-setup-new-base-read.js", "docs/operator-brand-explorer-airtable-fields.md"],
    }),
    operatorPlatform: tableConfig("Operator Setup - Platform & Markets", {
      category: "operator",
      setupPriority: 2,
      sources: ["docs/operator-alignment-recommended-airtable-fields.md"],
    }),
    operatorCommercial: tableConfig("Operator Setup - Commercial Fit & Terms", {
      category: "operator",
      setupPriority: 2,
      sources: ["api/lib/operator-setup-new-base-read.js"],
    }),
    operatorGovernance: tableConfig("Operator Setup - Governance, Delivery & Diligence", {
      tableAliases: ["Operator Setup - Governance Delivery & Diligence"],
      category: "operator",
      setupPriority: 2,
      sources: ["docs/operator-infrastructure-explorer-airtable-fields.md"],
    }),
    operatorEngagement: tableConfig("Operator Setup - Engagement & Reporting", {
      category: "operator",
      setupPriority: 3,
      optionalTable: true,
      sources: ["api/lib/operator-engagement-reporting-map.js"],
    }),
    operatorExplorerMaterials: tableConfig("Operator Setup - Explorer Materials", {
      category: "operator",
      setupPriority: 1,
      checkP1ProfileGovernance: true,
      optionalTable: true,
      sources: ["docs/operator-materials-explorer-airtable-fields.md", "scripts/setup-operator-validation-fields.mjs"],
      notes: "P1 materials root — mirror Brand Explorer Presentation governance.",
    }),
    operatorLeadershipPlatform: tableConfig("Operator Setup - Leadership Platform", {
      category: "operator",
      setupPriority: 4,
      optionalTable: true,
      sources: ["api/lib/operator-leadership-platform-map.js"],
    }),
    operatorLeadershipTeam: tableConfig("Operator Setup - Leadership Team Members", {
      tableAliases: ["Operator Setup - Team Members"],
      category: "operator",
      setupPriority: 4,
      sources: ["api/lib/operator-setup-new-base-read.js"],
    }),
    operatorCaseStudies: tableConfig("Operator Setup - Case Studies", {
      category: "operator",
      setupPriority: 5,
      sources: ["api/lib/operator-setup-new-base-read.js"],
    }),
    operatorDiligenceQa: tableConfig("Operator Setup - Diligence QA", {
      category: "operator",
      setupPriority: 5,
      sources: ["api/lib/operator-setup-new-base-read.js"],
    }),
    partnerSourceLibrary: tableConfig(PARTNER_INTELLIGENCE_TABLES.sourceLibrary, {
      category: "partner-intelligence",
      optionalTable: true,
      setupPriority: 1,
      checkCoreGovernance: false,
      fieldSpecs: piFieldSpecs(MAP_PARTNER_SOURCE, "Partner Intelligence — Source Library"),
      sources: ["api/lib/partner-intelligence-field-map.js", "docs/partner-source-library-airtable-fields.md"],
    }),
    partnerExtractedFacts: tableConfig(PARTNER_INTELLIGENCE_TABLES.extractedFacts, {
      category: "partner-intelligence",
      optionalTable: true,
      setupPriority: 1,
      checkCoreGovernance: false,
      fieldSpecs: piFieldSpecs(MAP_PARTNER_FACT, "Partner Intelligence — Extracted Facts"),
      sources: ["api/lib/partner-intelligence-field-map.js", "docs/partner-extracted-facts-airtable-fields.md"],
    }),
    partnerPublishedFields: tableConfig(PARTNER_INTELLIGENCE_TABLES.publishedFields, {
      category: "partner-intelligence",
      optionalTable: true,
      setupPriority: 1,
      checkCoreGovernance: false,
      fieldSpecs: piFieldSpecs(MAP_PARTNER_PUBLISHED, "Partner Intelligence — Published"),
      sources: [
        "api/lib/partner-intelligence-field-map.js",
        "docs/partner-explorer-published-fields-airtable-fields.md",
      ],
    }),
    partnerHelenaIntake: tableConfig(PARTNER_INTELLIGENCE_TABLES.helenaIntake, {
      category: "partner-intelligence",
      optionalTable: true,
      setupPriority: 2,
      checkCoreGovernance: false,
      fieldSpecs: piFieldSpecs(MAP_PARTNER_HELENA, "Partner Intelligence — Helena Intake"),
      sources: ["api/lib/partner-intelligence-field-map.js", "docs/partner-helena-intake-airtable-fields.md"],
    }),
    companyProfile: tableConfig(
      process.env.AIRTABLE_COMPANY_PROFILE_TABLE || "Company Profile",
      {
        category: "shared",
        setupPriority: 3,
        checkCoreGovernance: false,
        fieldSpecs: companyProfileSpecs,
        sources: ["lib/pilot-provisioning/pilot-field-registry.js", "docs/platform-reference/DATA_DICTIONARY.md"],
        notes: "Permissions SSOT — minimal validation fields only.",
      }
    ),
    brandAliasMapping: tableConfig(BRAND_ALIAS_TABLE, {
      base: "alt",
      category: "shared",
      optionalTable: true,
      setupPriority: 4,
      checkCoreGovernance: false,
      fieldSpecs: aliasMappingSpecs,
      sources: ["lib/hotel-census/fields.js", "scripts/ensure-brand-alias-mapping-table.mjs"],
    }),
    hotelCensus: tableConfig(HOTEL_CENSUS_TABLE, {
      base: "alt",
      category: "shared",
      optionalTable: true,
      setupPriority: 5,
      checkCoreGovernance: false,
      fieldSpecs: censusGovernanceSpecs,
      sources: ["lib/hotel-census/fields.js", "scripts/ensure-hotel-census-governance-fields.mjs"],
      notes: "Census row QA — related to Brand Explorer rollups, not profile SSOT.",
    }),
  };

  for (const legacyName of LEGACY_OPERATOR_TABLES) {
    const key = `legacy_${legacyName.replace(/[^a-zA-Z0-9]+/g, "_")}`;
    tables[key] = tableConfig(legacyName, {
      category: "legacy",
      optionalTable: true,
      legacy: true,
      setupPriority: 99,
      checkCoreGovernance: false,
      checkAliasEquivalents: true,
      sources: ["lib/third-party-operator-airtable-fields-used.js"],
      notes: "Legacy path — report presence only; do not add governance unless still live.",
    });
  }

  return {
    tables,
    coreGovernance: coreOnly(),
    p1ProfileGovernance: buildP1ProfileGovernanceFieldSpecs(),
    aliasEquivalents: [...ALIAS_EQUIVALENT_FIELD_SPECS],
  };
}

/** Resolve live table by canonical name or alias list. */
export function resolveLiveTable(tableByName, config) {
  const candidates = [config.tableName, ...(config.tableAliases || [])];
  for (const name of candidates) {
    const hit = tableByName.get(name);
    if (hit) return { table: hit, matchedName: name };
  }
  return { table: null, matchedName: null };
}
