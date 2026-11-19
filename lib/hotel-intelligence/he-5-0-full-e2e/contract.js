/**
 * Hotel Explorer 5.0 — Full E2E eight-hotel research pilot contract.
 * Shared $50 paid ceiling. Dry-run Airtable only until founder apply auth.
 */

export const HE50_PILOT_VERSION = "he-5-0-full-e2e-pilot-v1";
export const HE50_PILOT_RUN_ID = "he50_mullen_eight_hotel_2026_11_19";

/** Hard global paid-provider ceiling (USD). Not per hotel. */
export const HE50_GLOBAL_USD_CAP = 50;

/** Planning allocations (reallocatable within global cap). */
export const HE50_CATEGORY_CEILINGS = Object.freeze({
  property_identity_physical: 12,
  ownership_stakeholders: 18,
  portfolio_hpc_matching: 10,
  demand_access_news_gaps: 10,
});

export const HE50_CANONICAL_BASE_ID = "appCCUsuGsE1ifoLk";
export const HE50_HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
/** @deprecated Legacy Hotel Census — HE runtime must not access. Guard: legacy-hotel-census-access-guard-v1. */
export const HE50_HOTEL_CENSUS_TABLE_ID_RETIRED = "tblgj2qEwxjTcg6q0";
/** @deprecated Use HE50_HOTEL_CENSUS_TABLE_ID_RETIRED — retained name for inventory scanners. */
export const HE50_HOTEL_CENSUS_TABLE_ID = HE50_HOTEL_CENSUS_TABLE_ID_RETIRED;
export const HE50_BETHESDA_HPC_ID = "recLuxvwwxID7U2B8";
export const HE50_LEGACY_CENSUS_INDEPENDENCE = true;

export const FIELD_CLASS = Object.freeze({
  EXISTING_VERIFIED: "EXISTING_VERIFIED",
  EXISTING_NOT_CONNECTED: "EXISTING_NOT_CONNECTED",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  SOURCE_CONFLICT: "SOURCE_CONFLICT",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  SCHEMA_MAPPING_REQUIRED: "SCHEMA_MAPPING_REQUIRED",
  UNRESOLVED_AFTER_RESEARCH: "UNRESOLVED_AFTER_RESEARCH",
});

export const HOTEL_READINESS = Object.freeze({
  HPC_MINIMUM_VERIFIED: "HPC_MINIMUM_VERIFIED",
  HOTEL_EXPLORER_RESEARCH_COMPLETE: "HOTEL_EXPLORER_RESEARCH_COMPLETE",
  PERSISTENCE_VALIDATED: "PERSISTENCE_VALIDATED",
  HOTEL_EXPLORER_UI_VALIDATED: "HOTEL_EXPLORER_UI_VALIDATED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
});

export const PIPELINE_STAGES = Object.freeze([
  "AUDIT",
  "REUSE",
  "RESEARCH",
  "VERIFY",
  "DEDUPLICATE",
  "MAP",
  "DRY_RUN",
  "APPROVAL",
  "APPLY",
  "RE_READ",
  "UI_VALIDATE",
]);

export const APPLY_GATE = Object.freeze({
  applyAuthorized: false,
  airtableWritesAllowed: false,
  surfeAutomatic: false,
  productionDeploy: false,
  stopMessage:
    "STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.",
});

/** HE overlay tabs (canonical order from hotel-explorer.js). */
export const HE50_TABS = Object.freeze([
  { id: "hotel", label: "Hotel Overview" },
  { id: "rooms", label: "Guest Rooms & Meeting Space" },
  { id: "ownership", label: "Ownership Intelligence" },
  { id: "contacts", label: "Contact Intelligence" },
  { id: "organization", label: "Organization & Portfolio" },
  { id: "relationships", label: "Relationships & Structure" },
  { id: "submarket", label: "Submarket Snapshot" },
  { id: "area", label: "Area Hotels" },
  { id: "demand", label: "Demand Drivers" },
  { id: "access", label: "Access & Connectivity" },
  { id: "sentiment", label: "Guest Sentiment" },
  { id: "sources", label: "Sources & Evidence" },
]);

export const PROVIDER_ROUTING = Object.freeze({
  preferred_order: [
    "existing_dealality_airtable",
    "official_hotel_brand_websites",
    "government_property_registries",
    "public_company_filings_press",
    "market_alerts_archive",
    "exa_lite_bounded",
    "parallel_search_bounded",
  ],
  prohibited_this_pilot: ["surfe_automatic", "unbounded_agent_runs", "new_subscriptions"],
  notes:
    "Free-first. Paid only when free/existing insufficient, cost-bounded, and under shared $50 cap.",
});
