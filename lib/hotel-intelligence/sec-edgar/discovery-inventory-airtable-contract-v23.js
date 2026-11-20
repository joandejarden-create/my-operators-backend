/**
 * SEC EDGAR V2.3 — permanent HI Hotel Discovery Inventory + HI Discovery Findings
 * on the canonical HPC Airtable base. Design-approved; Meta apply via ensure script.
 */

export const DISCOVERY_INVENTORY_CONTRACT_V23 = "SEC_EDGAR_DISCOVERY_INVENTORY_V23";
export const DISCOVERY_SCHEMA_VERSION = "2.3.0";

export const GRAPH_BASE_ID_CANONICAL = "appCCUsuGsE1ifoLk";
export const GRAPH_BASE_ID_FORBIDDEN = "appa2cE7FTRmIbB32";
export const HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const HPC_TABLE_NAME = "Hotel Property Census";
export const HI_ORGANIZATIONS_TABLE_ID = "tbliXEfUOWk32AN9y";
export const HI_GRAPH_EVIDENCE_TABLE_ID = "tblc9JhKT1edr4grE";

export const TABLE_HI_HOTEL_DISCOVERY_INVENTORY = "HI Hotel Discovery Inventory";
export const TABLE_HI_DISCOVERY_FINDINGS = "HI Discovery Findings";

/**
 * Why HI Graph Evidence cannot safely store unmatched hotels.
 * Decision recorded for founder / IC audit.
 */
export const GRAPH_EVIDENCE_REUSE_DECISION = Object.freeze({
  decision: "DO_NOT_REUSE_FOR_UNMATCHED",
  createInstead: TABLE_HI_DISCOVERY_FINDINGS,
  reasons: [
    "HI Graph Evidence.Hotel Property Census is a required architectural link for Hotel Explorer Sources SoT",
    "Unmatched discovery rows would either break required HPC linkage or pollute customer-facing evidence readers",
    "HE 5.0 standing authorization and he-airtable-graph-reader assume HPC-linked evidence",
    "Entity Type enum has no Discovery Property; extending it mixes research-only unmatched data into production evidence paths",
    "Hotel Property Source Evidence / Independent Hotel Source Evidence are census pipeline tables — not SEC discovery inventory",
  ],
  safeReuseAfterHpcMatch:
    "After strong HPC match, eligible verified findings may be promoted into HI Graph Evidence with read-back — never before match",
});

export const MAP_DISCOVERY_INVENTORY = Object.freeze({
  discoveryPropertyId: "Discovery Property ID",
  hotelNameNormalized: "Hotel Name (normalized)",
  hotelNameRaw: "Hotel Name (raw)",
  historicalAliases: "Historical Names / Aliases",
  city: "City",
  stateRegion: "State / Region",
  country: "Country",
  address: "Address",
  brandFamily: "Brand Family",
  brandLabel: "Brand Label",
  historicalBrand: "Historical Brand",
  currentBrand: "Current Brand",
  identityClass: "Identity Class",
  evidenceConfidence: "Evidence Confidence",
  secRegistrant: "SEC Registrant",
  registrantCik: "Registrant CIK",
  sourceFilingUrl: "Source Filing URL",
  filingDate: "Filing Date",
  sourceDocumentLedger: "Source Document Ledger",
  sourceAccessionPrimary: "Primary Accession",
  discoveryMethod: "Discovery Method",
  hpcMatchStatus: "HPC Match Status",
  hpcHotel: "HPC Hotel",
  lifecycleStatus: "Lifecycle Status",
  persistenceStatus: "Persistence Status",
  firstDiscoveredAt: "First Discovered At",
  lastResearchedAt: "Last Researched At",
  propertyKey: "Property Key",
  notesQuarantine: "Notes / Quarantine Reason",
  schemaVersion: "Schema Version",
  publicationState: "Publication State",
});

export const MAP_DISCOVERY_FINDINGS = Object.freeze({
  findingId: "Finding ID",
  discoveryProperty: "Discovery Property",
  findingType: "Finding Type",
  fieldName: "Field Name",
  valueObserved: "Value Observed",
  agreementType: "Agreement Type",
  historicalApplicability: "Historical Applicability",
  effectiveDate: "Effective / Event Date",
  reportingDate: "Reporting Date",
  sourceUrl: "Source URL",
  secAccession: "SEC Accession",
  registrantCik: "Registrant CIK",
  evidenceSnippet: "Evidence Snippet",
  confidence: "Confidence",
  verificationStatus: "Verification Status",
  relatedOrganization: "Related Organization",
  relatedOrgName: "Related Organization Name",
  supersededBy: "Superseded By",
  hpcPromotionStatus: "HPC Promotion Status",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const IDENTITY_CLASS_OPTIONS = Object.freeze([
  "VERIFIED_PROPERTY",
  "PROBABLE_PROPERTY",
  "AMBIGUOUS_PROPERTY",
  "MULTIPLE_PROPERTIES",
  "GENERIC_BRAND_OR_COMPANY",
  "EXTRACTION_ARTIFACT",
  "DUPLICATE_ALIAS",
]);

export const BRAND_FAMILY_OPTIONS = Object.freeze([
  "MARRIOTT",
  "HILTON",
  "HYATT",
  "IHG",
  "WYNDHAM",
  "CHOICE",
  "ACCOR",
  "RADISSON",
  "INDEPENDENT",
  "OTHER",
  "UNKNOWN",
]);

export const EVIDENCE_CONFIDENCE_OPTIONS = Object.freeze(["HIGH", "MEDIUM", "LOW", "UNVERIFIED"]);

export const HPC_MATCH_STATUS_OPTIONS = Object.freeze([
  "DISCOVERED_NOT_IN_HPC",
  "MATCHED",
  "AMBIGUOUS",
  "MATCH_PENDING_REVIEW",
  "NOT_APPLICABLE",
]);

/** Lifecycle: Airtable save ≠ verified intelligence. */
export const LIFECYCLE_STATUS_OPTIONS = Object.freeze([
  "DISCOVERY_PERSISTED",
  "IDENTITY_VERIFIED",
  "HPC_MATCHED",
  "EVIDENCE_PROMOTED",
  "PERSISTED_VERIFIED",
]);

export const PERSISTENCE_STATUS_OPTIONS = Object.freeze([
  "INVENTORY_ONLY",
  "DISCOVERY_PERSISTED",
  "RESEARCH_ONLY",
  "PERSISTED_VERIFIED",
  "QUARANTINED",
]);

export const PUBLICATION_STATE_OPTIONS = Object.freeze([
  "INTERNAL_ONLY",
  "DO_NOT_PUBLISH",
  "CUSTOMER_BLOCKED",
]);

export const FINDING_TYPE_OPTIONS = Object.freeze([
  "CONTRACT_MILESTONE",
  "OWNERSHIP",
  "MANAGEMENT",
  "BRAND_AFFILIATION",
  "TRANSACTION",
  "GEOGRAPHY",
  "IDENTITY_ALIAS",
  "SUBSIDIARY",
  "OTHER",
]);

export const AGREEMENT_TYPE_OPTIONS = Object.freeze([
  "FRANCHISE",
  "MANAGEMENT",
  "LEASE",
  "UNKNOWN_AGREEMENT",
  "NOT_APPLICABLE",
]);

export const HISTORICAL_APPLICABILITY_OPTIONS = Object.freeze([
  "HISTORICALLY_VERIFIED",
  "CURRENTLY_VERIFIED",
  "CURRENT_STATUS_UNCONFIRMED",
  "SUPERSEDED",
  "REQUIRES_FURTHER_RESEARCH",
]);

export const FINDING_VERIFICATION_OPTIONS = Object.freeze([
  "UNVERIFIED",
  "EVIDENCE_COLLECTED",
  "HISTORICALLY_VERIFIED",
  "CURRENTLY_VERIFIED",
  "CONFLICTED",
  "SUPERSEDED",
  "QUARANTINED",
]);

export const HPC_PROMOTION_STATUS_OPTIONS = Object.freeze([
  "NOT_PROMOTED",
  "ELIGIBLE",
  "PROMOTED_TO_GRAPH_EVIDENCE",
  "BLOCKED_NO_HPC_MATCH",
  "BLOCKED_CUSTOMER_PUBLICATION",
]);

export function assertCanonicalDiscoveryBase(baseId) {
  const id = String(baseId || "").trim();
  if (!id) return { ok: false, error: "BASE_ID_MISSING" };
  if (id === GRAPH_BASE_ID_FORBIDDEN) {
    return { ok: false, error: "FORBIDDEN_ADP_BASE", expected: GRAPH_BASE_ID_CANONICAL };
  }
  if (id !== GRAPH_BASE_ID_CANONICAL) {
    return { ok: false, error: "BASE_MISMATCH", expected: GRAPH_BASE_ID_CANONICAL, actual: id };
  }
  return { ok: true, baseId: id };
}
