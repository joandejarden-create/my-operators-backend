/**
 * HIF V6 Track A — Commercial, contract & property intelligence source inventory.
 * Design/inventory only until PR #503 merges. No wave execution from this module.
 */

export const HIF_V6_TRACK_A_SOURCE_INVENTORY = "hif-v6-track-a-source-inventory";

export const TRACK_A_SOURCE_FAMILIES = Object.freeze([
  {
    id: "OWNER_PORTFOLIOS",
    label: "Hotel ownership portfolios",
    priority: "HIGH",
    existingAdapters: ["owner-portfolio-operator-view-v1"],
    v6Status: "EXPAND",
    yieldHypothesis: "One verified owner → many hotels",
  },
  {
    id: "MANAGEMENT_COMPANY_PORTFOLIOS",
    label: "Hotel management-company portfolios",
    priority: "HIGH",
    existingAdapters: [],
    v6Status: "BUILD",
    yieldHypothesis: "Operator graph expansion for Management Contract list",
  },
  {
    id: "FRANCHISEE_ORGANIZATIONS",
    label: "Franchisee organizations",
    priority: "HIGH",
    existingAdapters: [],
    v6Status: "BUILD",
    yieldHypothesis: "Brand conversion portfolio clusters",
  },
  {
    id: "SEC_CMBS_DISCLOSURES",
    label: "SEC and CMBS contractual disclosures",
    priority: "CRITICAL",
    existingAdapters: [
      "cmbs-property-schedule-extractor-v1",
      "commercial-expiration-notice-extract-v1",
      "contract-terms-extractor-v1",
    ],
    v6Status: "ENHANCE_LARGE_ANNEX_TABLES",
    yieldHypothesis: "Primary US verified-expiration engine",
    requiredFieldPatterns: [
      "Franchise Expiration Date",
      "Franchise Agreement Expiration",
      "Management Agreement Expiration",
      "Expiration/Termination of Related License/Franchise/Operating Agreement",
      "Hotel Management Agreement",
      "Franchise Agreement Abstract",
      "Management Agreement Abstract",
      "Franchise Comfort Letter",
      "Renewal Agreement",
      "Amendment and Extension Agreement",
      "Agreement Termination Notice",
    ],
  },
  {
    id: "EX10_EXHIBITS",
    label: "EX-10 exhibits",
    priority: "CRITICAL",
    existingAdapters: [
      "ex10-contract-harvest-v1",
      "ex10-agreement-term-extractor-v1",
      "ex10-end-date-notice-extract-v1",
      "ex10-document-execution-class-v1",
    ],
    v6Status: "COMPLETE_62_EXHIBIT_WAVE_AFTER_MERGE",
    pendingWaveDocuments: 62,
  },
  {
    id: "HOTEL_APPRAISALS",
    label: "Hotel appraisals",
    priority: "MEDIUM",
    existingAdapters: [],
    v6Status: "INCIDENTAL_ONLY",
    priorPilot: "DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER",
    notes: "Assumption-heavy; separate appraiser assumptions from factual disclosures",
  },
  {
    id: "FINANCING_UNDERWRITING",
    label: "Financing and underwriting records",
    priority: "HIGH",
    existingAdapters: ["cmbs-property-schedule-extractor-v1"],
    v6Status: "EXPAND_WITH_SEC_CMBS",
    notes: "Mortgage maturity ≠ franchise/management expiration",
  },
  {
    id: "AGREEMENT_ABSTRACTS",
    label: "Franchise and management agreement abstracts",
    priority: "HIGH",
    existingAdapters: [],
    v6Status: "BUILD_WITH_CMBS_EX10",
  },
  {
    id: "RECORDED_MANAGEMENT_AGREEMENTS",
    label: "Recorded management agreement memoranda / land records",
    priority: "HIGH",
    existingAdapters: [],
    v6Status: "PILOT_AFTER_MERGE",
    searchInstruments: [
      "Memorandum of Management Agreement",
      "Notice of Management Agreement",
      "Recorded Hotel Management Agreement",
      "Management Agreement Assignment",
      "Management Agreement Amendment",
      "Recorded Termination",
    ],
    caveats: [
      "Memorandum is not the complete agreement",
      "Do not infer unrecorded terms",
      "Absence of recording ≠ absence of management contract",
    ],
  },
  {
    id: "COURT_BANKRUPTCY_INSOLVENCY",
    label: "Court, bankruptcy and insolvency records",
    priority: "MEDIUM",
    existingAdapters: [],
    v6Status: "PILOT_AFTER_MERGE_METERED_AUTH",
    potentialSources: [
      "US PACER / public court exhibits",
      "Public bankruptcy agreements",
      "Receivership filings",
      "European commercial court / insolvency registries",
      "Latin American public judicial filings",
    ],
    evidenceClassesRequired: true,
    sealedRecordsUnauthorized: true,
  },
  {
    id: "BRAND_CONVERSIONS_OPERATOR_CHANGES",
    label: "Brand conversions and operator changes",
    priority: "HIGH",
    existingAdapters: ["commercial-target-discovery-v1"],
    v6Status: "EXPAND_SIGNAL_CLASSIFICATION",
  },
  {
    id: "OWNERSHIP_TRANSACTIONS",
    label: "Ownership transactions",
    priority: "HIGH",
    existingAdapters: [],
    v6Status: "BUILD_WITH_PROPERTY_RECORDS",
  },
  {
    id: "RENOVATION_REPOSITIONING",
    label: "Major renovation and repositioning signals",
    priority: "MEDIUM",
    existingAdapters: [],
    v6Status: "SIGNAL_ONLY",
  },
  {
    id: "FINANCING_RESTRUCTURING",
    label: "Selected financing and restructuring events",
    priority: "MEDIUM",
    existingAdapters: [],
    v6Status: "SIGNAL_ONLY",
  },
]);

export function trackAInventorySummary() {
  return {
    version: HIF_V6_TRACK_A_SOURCE_INVENTORY,
    track: "TRACK_A_COMMERCIAL_CONTRACT_PROPERTY",
    familyCount: TRACK_A_SOURCE_FAMILIES.length,
    criticalFamilies: TRACK_A_SOURCE_FAMILIES.filter((f) => f.priority === "CRITICAL").map((f) => f.id),
    waveExecutionAuthorized: false,
    gate: "AWAITING_PR_503_MERGE",
    families: TRACK_A_SOURCE_FAMILIES,
  };
}
