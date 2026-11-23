/**
 * DealSignal / HIF Track A — Global Source Expansion registry V1.
 *
 * Extends Track A source inventory + SOURCE_FAMILY + intl-readiness conventions.
 * Does NOT claim API/scrape integration unless verified.
 *
 * Integration readiness:
 *  EXISTING_INTEGRATED | ACCESSIBLE_VIA_CURRENT_TOOLING | MANUALLY_SEARCHABLE |
 *  CANDIDATE_REQUIRES_TECH_INTEGRATION | RESTRICTED_OR_UNAVAILABLE
 */
import { TRACK_A_SOURCE_FAMILIES } from "../sec-edgar/hif-v6-track-a-source-inventory.js";
import { SOURCE_FAMILY } from "../sec-edgar/contract-timing-types-v1.js";
import { FUTURE_JURISDICTION_SOURCES } from "../sec-edgar/intl-readiness-v1.js";

export const DEALSIGNAL_SOURCE_REGISTRY_VERSION = "dealsignal-source-registry-v1";

export const INTEGRATION_READINESS = Object.freeze({
  EXISTING_INTEGRATED: "EXISTING_INTEGRATED",
  ACCESSIBLE_VIA_CURRENT_TOOLING: "ACCESSIBLE_VIA_CURRENT_TOOLING",
  MANUALLY_SEARCHABLE: "MANUALLY_SEARCHABLE",
  CANDIDATE_REQUIRES_TECH_INTEGRATION: "CANDIDATE_REQUIRES_TECH_INTEGRATION",
  RESTRICTED_OR_UNAVAILABLE: "RESTRICTED_OR_UNAVAILABLE",
});

export const SOURCE_CATEGORY = Object.freeze({
  OWNER_CORPORATE_DISCLOSURE: "OWNER_CORPORATE_DISCLOSURE",
  CONTRACT_INTELLIGENCE: "CONTRACT_INTELLIGENCE",
  CAPITAL_DEBT: "CAPITAL_DEBT",
  OWNERSHIP_PROPERTY: "OWNERSHIP_PROPERTY",
  DEVELOPMENT_REPOSITIONING: "DEVELOPMENT_REPOSITIONING",
  LIVE_COMMERCIAL_SELECTION: "LIVE_COMMERCIAL_SELECTION",
  BRAND_OPERATOR_ACTIVITY: "BRAND_OPERATOR_ACTIVITY",
  LOCAL_HOSPITALITY_BI: "LOCAL_HOSPITALITY_BI",
});

/**
 * @typedef {object} DealSignalSourceEntry
 * @property {string} sourceId
 * @property {string} sourceName
 * @property {string} officialWebsite
 * @property {string[]} countryRegionCoverage
 * @property {string} sourceCategory
 * @property {string[]} commercialSignals
 * @property {string[]} languages
 * @property {string} publicAccess
 * @property {string} retrievalMethod
 * @property {string|null} apiAvailability
 * @property {string} licensingNotes
 * @property {string} dataFreshness
 * @property {string} propertyLevelUsefulness
 * @property {string} ownerLevelUsefulness
 * @property {string} contractualRelevance
 * @property {string} expectedResearchValue
 * @property {string} sourceReliability
 * @property {string} testedStatus
 * @property {string} [actualDiscoveryResults]
 * @property {string} integrationReadiness
 * @property {string[]} [extendsTrackAFamilies]
 */

/** @type {DealSignalSourceEntry[]} */
export const DEALSIGNAL_SOURCE_REGISTRY = Object.freeze([
  // —— Existing integrated / tooling ——
  {
    sourceId: "us_sec_edgar",
    sourceName: "US SEC EDGAR",
    officialWebsite: "https://www.sec.gov/edgar",
    countryRegionCoverage: ["US", "GLOBAL_ISSUERS"],
    sourceCategory: SOURCE_CATEGORY.CONTRACT_INTELLIGENCE,
    commercialSignals: ["franchise_expiration", "hma_expiration", "owner_strategy", "portfolio_schedules"],
    languages: ["en"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "SEC_API_AND_FILING_HARVEST",
    apiAvailability: "VERIFIED_PUBLIC",
    licensingNotes: "Public filings; fair use / attribution",
    dataFreshness: "CONTINUOUS",
    propertyLevelUsefulness: "HIGH_WHEN_SCHEDULES",
    ownerLevelUsefulness: "HIGH",
    contractualRelevance: "CRITICAL",
    expectedResearchValue: "HIGHEST",
    sourceReliability: "PRIMARY_OWNER",
    testedStatus: "TESTED_PRODUCTIVE",
    actualDiscoveryResults: "SHO/CLDT/APLE/RLJ/SVC schedules and HMAs across DealSignal waves",
    integrationReadiness: INTEGRATION_READINESS.EXISTING_INTEGRATED,
    extendsTrackAFamilies: ["SEC_CMBS_DISCLOSURES", "EX10_EXHIBITS"],
  },
  {
    sourceId: "us_cmbs_424b",
    sourceName: "US CMBS 424B / UW annexes",
    officialWebsite: "https://www.sec.gov/edgar",
    countryRegionCoverage: ["US"],
    sourceCategory: SOURCE_CATEGORY.CAPITAL_DEBT,
    commercialSignals: ["franchise_expiration", "combined_license_columns", "loan_maturity_context"],
    languages: ["en"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "CMBS_SCHEDULE_EXTRACTOR",
    apiAvailability: "VIA_EDGAR",
    licensingNotes: "Public SEC",
    dataFreshness: "DEAL_DRIVEN",
    propertyLevelUsefulness: "HIGH",
    ownerLevelUsefulness: "MEDIUM",
    contractualRelevance: "HIGH",
    expectedResearchValue: "HIGH",
    sourceReliability: "PRIMARY_UW",
    testedStatus: "TESTED_PRODUCTIVE",
    integrationReadiness: INTEGRATION_READINESS.EXISTING_INTEGRATED,
    extendsTrackAFamilies: ["SEC_CMBS_DISCLOSURES", "FINANCING_UNDERWRITING"],
  },
  {
    sourceId: "owner_ir_earnings",
    sourceName: "Hotel owner IR / earnings / presentations",
    officialWebsite: "VARIES_BY_ISSUER",
    countryRegionCoverage: ["GLOBAL"],
    sourceCategory: SOURCE_CATEGORY.OWNER_CORPORATE_DISCLOSURE,
    commercialSignals: ["conversion_pipeline", "manager_transfers", "repositioning", "sale_process"],
    languages: ["en", "es", "pt"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_AND_WEB_SEARCH",
    apiAvailability: null,
    licensingNotes: "Issuer IR materials",
    dataFreshness: "QUARTERLY",
    propertyLevelUsefulness: "MEDIUM_HIGH",
    ownerLevelUsefulness: "HIGH",
    contractualRelevance: "MEDIUM",
    expectedResearchValue: "HIGH",
    sourceReliability: "PRIMARY_OWNER",
    testedStatus: "TESTED_PRODUCTIVE",
    actualDiscoveryResults: "RLJ pipeline; APLE HMAs; FIHO/FINN Mexico",
    integrationReadiness: INTEGRATION_READINESS.ACCESSIBLE_VIA_CURRENT_TOOLING,
    extendsTrackAFamilies: ["OWNER_PORTFOLIOS"],
  },
  // —— Mexico Priority A ——
  {
    sourceId: "mx_bmv_eventos",
    sourceName: "Bolsa Mexicana de Valores — eventos relevantes / FIBRA disclosures",
    officialWebsite: "https://www.bmv.com.mx",
    countryRegionCoverage: ["Mexico"],
    sourceCategory: SOURCE_CATEGORY.OWNER_CORPORATE_DISCLOSURE,
    commercialSignals: ["brand_conversion", "hma_internalization", "lease_vs_manage", "franchise_renewal"],
    languages: ["es", "en"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_PDF_AND_WEB_SEARCH",
    apiAvailability: null,
    licensingNotes: "Public issuer disclosures",
    dataFreshness: "EVENT_DRIVEN",
    propertyLevelUsefulness: "HIGH",
    ownerLevelUsefulness: "HIGH",
    contractualRelevance: "HIGH",
    expectedResearchValue: "HIGHEST_CALA",
    sourceReliability: "PRIMARY_OWNER",
    testedStatus: "TESTED_PRODUCTIVE",
    actualDiscoveryResults: "FibraHotel Villahermosa/Mexicali; Fibra Inn internalization + Courtyard conversions",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
  {
    sourceId: "mx_compras_mx",
    sourceName: "Compras MX (Mexican federal procurement)",
    officialWebsite: "https://comprasmx.buengobierno.gob.mx",
    countryRegionCoverage: ["Mexico"],
    sourceCategory: SOURCE_CATEGORY.LIVE_COMMERCIAL_SELECTION,
    commercialSignals: ["hotel_ops_services", "government_hotel_management_adjacent", "pms_contracts"],
    languages: ["es"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_PORTAL_SEARCH",
    apiAvailability: "UNVERIFIED",
    licensingNotes: "Government open procurement; no scraping beyond ToS",
    dataFreshness: "CONTINUOUS",
    propertyLevelUsefulness: "MEDIUM",
    ownerLevelUsefulness: "LOW_MEDIUM",
    contractualRelevance: "MEDIUM",
    expectedResearchValue: "MEDIUM_ADJACENT_FILTER_REQUIRED",
    sourceReliability: "PRIMARY_GOVERNMENT",
    testedStatus: "TESTED_PARTIAL",
    actualDiscoveryResults: "GAFSACOMM Mundo Maya ops supervision; PMS for administered hotels — adjacent not classic BCT",
    integrationReadiness: INTEGRATION_READINESS.CANDIDATE_REQUIRES_TECH_INTEGRATION,
  },
  {
    sourceId: "mx_rnt_registro_nacional_turismo",
    sourceName: "Registro Nacional de Turismo (Mexico)",
    officialWebsite: "https://www.gob.mx",
    countryRegionCoverage: ["Mexico"],
    sourceCategory: SOURCE_CATEGORY.OWNERSHIP_PROPERTY,
    commercialSignals: ["hotel_identity", "tourism_registry"],
    languages: ["es"],
    publicAccess: "FREE_PUBLIC_PARTIAL",
    retrievalMethod: "MANUAL_DIRECTORY",
    apiAvailability: "UNVERIFIED",
    licensingNotes: "Government registry",
    dataFreshness: "UNKNOWN",
    propertyLevelUsefulness: "MEDIUM_IDENTITY",
    ownerLevelUsefulness: "LOW",
    contractualRelevance: "LOW",
    expectedResearchValue: "SUPPORTING_IDENTITY",
    sourceReliability: "PRIMARY_GOVERNMENT",
    testedStatus: "NOT_TESTED_THIS_WAVE",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
  {
    sourceId: "mx_siger_rpc",
    sourceName: "SIGER / Registro Público de Comercio",
    officialWebsite: "https://www.gob.mx",
    countryRegionCoverage: ["Mexico"],
    sourceCategory: SOURCE_CATEGORY.OWNERSHIP_PROPERTY,
    commercialSignals: ["legal_owner_entity", "corporate_changes"],
    languages: ["es"],
    publicAccess: "MIXED_FEES_POSSIBLE",
    retrievalMethod: "MANUAL_REGISTRY",
    apiAvailability: null,
    licensingNotes: "May require paid certificates; no access bypass",
    dataFreshness: "EVENT_DRIVEN",
    propertyLevelUsefulness: "MEDIUM",
    ownerLevelUsefulness: "HIGH",
    contractualRelevance: "MEDIUM",
    expectedResearchValue: "MEDIUM",
    sourceReliability: "PRIMARY_REGISTRY",
    testedStatus: "NOT_TESTED_THIS_WAVE",
    integrationReadiness: INTEGRATION_READINESS.RESTRICTED_OR_UNAVAILABLE,
  },
  {
    sourceId: "mx_fibra_issuer_ir",
    sourceName: "Mexican hotel FIBRA issuer IR sites (FIHO, FINN, etc.)",
    officialWebsite: "https://fibrainn.mx",
    countryRegionCoverage: ["Mexico"],
    sourceCategory: SOURCE_CATEGORY.OWNER_CORPORATE_DISCLOSURE,
    commercialSignals: ["hma_termination", "franchise_conversion", "internalization"],
    languages: ["es", "en"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_PDF",
    apiAvailability: null,
    licensingNotes: "Issuer IR",
    dataFreshness: "EVENT_QUARTERLY",
    propertyLevelUsefulness: "HIGH",
    ownerLevelUsefulness: "HIGH",
    contractualRelevance: "HIGH",
    expectedResearchValue: "HIGHEST_CALA",
    sourceReliability: "PRIMARY_OWNER",
    testedStatus: "TESTED_PRODUCTIVE",
    actualDiscoveryResults: "FINN Aimbridge termination; HI Puebla non-renewal; Courtyard conversions",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
  // —— Caribbean ——
  {
    sourceId: "do_dgcp_compras",
    sourceName: "DGCP / Compras Dominicana",
    officialWebsite: "https://www.dgcp.gob.do",
    countryRegionCoverage: ["Dominican Republic"],
    sourceCategory: SOURCE_CATEGORY.LIVE_COMMERCIAL_SELECTION,
    commercialSignals: ["hotel_lodging_services", "concessions_framework", "tourism_ppp_adjacent"],
    languages: ["es"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_PORTAL_SEARCH",
    apiAvailability: "UNVERIFIED",
    licensingNotes: "Government procurement",
    dataFreshness: "CONTINUOUS",
    propertyLevelUsefulness: "LOW_MEDIUM",
    ownerLevelUsefulness: "LOW",
    contractualRelevance: "MEDIUM",
    expectedResearchValue: "MEDIUM_WITH_FALSE_POSITIVE_FILTER",
    sourceReliability: "PRIMARY_GOVERNMENT",
    testedStatus: "TESTED_PARTIAL",
    actualDiscoveryResults: "Lodging service awards ≠ operator RFP; Cabo Rojo via Pro-Pedernales not DGCP hotel HMA",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
  {
    sourceId: "do_pro_pedernales",
    sourceName: "Fideicomiso Pro-Pedernales / Cabo Rojo tourism destination",
    officialWebsite: "https://propedernales.do",
    countryRegionCoverage: ["Dominican Republic"],
    sourceCategory: SOURCE_CATEGORY.DEVELOPMENT_REPOSITIONING,
    commercialSignals: ["destination_development", "strategic_partner_selection", "future_hotel_pipeline"],
    languages: ["es"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_PRESS",
    apiAvailability: null,
    licensingNotes: "Public press",
    dataFreshness: "EVENT_DRIVEN",
    propertyLevelUsefulness: "LOW_FUTURE",
    ownerLevelUsefulness: "HIGH",
    contractualRelevance: "MEDIUM",
    expectedResearchValue: "MEDIUM_EARLY_ENGAGEMENT",
    sourceReliability: "PRIMARY_PROJECT",
    testedStatus: "TESTED_PRODUCTIVE",
    actualDiscoveryResults: "Consorcio Cabo Rojo (Grupo Puntacana) awarded — COMMITTED destination partner",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
  // —— South America ——
  {
    sourceId: "co_secop2",
    sourceName: "Colombia SECOP II",
    officialWebsite: "https://www.colombiacompra.gov.co",
    countryRegionCoverage: ["Colombia"],
    sourceCategory: SOURCE_CATEGORY.LIVE_COMMERCIAL_SELECTION,
    commercialSignals: ["hotel_concession", "ops_administration_concession"],
    languages: ["es"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_PORTAL_SEARCH",
    apiAvailability: "OPEN_DATA_PARTIAL",
    licensingNotes: "Government procurement",
    dataFreshness: "CONTINUOUS",
    propertyLevelUsefulness: "MEDIUM",
    ownerLevelUsefulness: "MEDIUM",
    contractualRelevance: "HIGH_WHEN_HOTEL",
    expectedResearchValue: "MEDIUM_HIGH_WITH_FILTER",
    sourceReliability: "PRIMARY_GOVERNMENT",
    testedStatus: "TESTED_PARTIAL",
    actualDiscoveryResults: "Santander 20yr concesión bien inmueble — verify hotel identity before BCT/MCT",
    integrationReadiness: INTEGRATION_READINESS.CANDIDATE_REQUIRES_TECH_INTEGRATION,
  },
  {
    sourceId: "br_cvm",
    sourceName: "Brazil CVM disclosures / FII regulations",
    officialWebsite: "https://www.gov.br/cvm",
    countryRegionCoverage: ["Brazil"],
    sourceCategory: SOURCE_CATEGORY.OWNER_CORPORATE_DISCLOSURE,
    commercialSignals: ["hotel_fii_structures", "lease_vs_management", "operator_relationships"],
    languages: ["pt"],
    publicAccess: "FREE_PUBLIC",
    retrievalMethod: "MANUAL_AND_WEB_SEARCH",
    apiAvailability: "PARTIAL",
    licensingNotes: "Public regulator",
    dataFreshness: "CONTINUOUS",
    propertyLevelUsefulness: "LOW_MEDIUM",
    ownerLevelUsefulness: "MEDIUM",
    contractualRelevance: "MEDIUM",
    expectedResearchValue: "MEDIUM",
    sourceReliability: "PRIMARY_REGULATOR",
    testedStatus: "TESTED_CONTEXTUAL",
    actualDiscoveryResults: "BTG Hotéis FII; industry lease vs admin debate — no hotel-specific open BCT this wave",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
  {
    sourceId: "br_fohb_hotelinvest",
    sourceName: "FOHB / HotelInvest Brazilian hospitality market reports",
    officialWebsite: "https://fohb.com.br",
    countryRegionCoverage: ["Brazil"],
    sourceCategory: SOURCE_CATEGORY.LOCAL_HOSPITALITY_BI,
    commercialSignals: ["conversion_trend", "franchise_vs_admin_mix"],
    languages: ["pt"],
    publicAccess: "FREE_PARTIAL",
    retrievalMethod: "MANUAL_PDF",
    apiAvailability: null,
    licensingNotes: "Industry association; not primary hotel evidence",
    dataFreshness: "ANNUAL",
    propertyLevelUsefulness: "LOW",
    ownerLevelUsefulness: "MEDIUM_CONTEXT",
    contractualRelevance: "LOW",
    expectedResearchValue: "CONTEXT_ONLY",
    sourceReliability: "SECONDARY_INDUSTRY",
    testedStatus: "TESTED_CONTEXTUAL",
    actualDiscoveryResults: "2026 panorama: conversions + franchise share rising — not hotel-specific",
    integrationReadiness: INTEGRATION_READINESS.MANUALLY_SEARCHABLE,
  },
]);

export function listSourcesByRegion(regionHint = "") {
  const h = String(regionHint || "").toLowerCase();
  return DEALSIGNAL_SOURCE_REGISTRY.filter((s) =>
    s.countryRegionCoverage.some((c) => String(c).toLowerCase().includes(h) || h.includes(String(c).toLowerCase())),
  );
}

export function listSourcesByReadiness(readiness) {
  return DEALSIGNAL_SOURCE_REGISTRY.filter((s) => s.integrationReadiness === readiness);
}

export function listTestedProductiveSources() {
  return DEALSIGNAL_SOURCE_REGISTRY.filter((s) => s.testedStatus === "TESTED_PRODUCTIVE");
}

export function sourceRegistryAuditSummary() {
  const byReadiness = {};
  const byTested = {};
  for (const s of DEALSIGNAL_SOURCE_REGISTRY) {
    byReadiness[s.integrationReadiness] = (byReadiness[s.integrationReadiness] || 0) + 1;
    byTested[s.testedStatus] = (byTested[s.testedStatus] || 0) + 1;
  }
  return {
    version: DEALSIGNAL_SOURCE_REGISTRY_VERSION,
    totalSources: DEALSIGNAL_SOURCE_REGISTRY.length,
    byReadiness,
    byTested,
    extendsTrackAFamilyCount: TRACK_A_SOURCE_FAMILIES.length,
    legacySourceFamilyEnums: Object.keys(SOURCE_FAMILY),
    intlReadinessJurisdictions: FUTURE_JURISDICTION_SOURCES.map((j) => j.jurisdiction),
  };
}

export { TRACK_A_SOURCE_FAMILIES, SOURCE_FAMILY, FUTURE_JURISDICTION_SOURCES };
