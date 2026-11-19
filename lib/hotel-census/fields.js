/**
 * Legacy Hotel Census field names (Deal Capture Platform / AIRTABLE_BASE_ID_ALT).
 *
 * Product runtime (Scout, Brand Presence, BE footprint, operators-by-brand) uses
 * Hotel Property Census via hpc-scout-record-adapter / platform-base exports.
 * This module remains for migration tooling and historical CLIs only.
 */

/** @deprecated Prefer Hotel Property Census for product runtime. */
export const HOTEL_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_CENSUS_TABLE || "Hotel Census";

export const BRAND_ALIAS_TABLE =
  process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping";

export const CENSUS_FIELDS = {
  name: "name",
  affiliation: "Affiliation",
  parentCompany: "Parent Company",
  status: "status",
  rooms: "rooms",
  country: "country",
  city: "city",
  market: "Market",
  region: "Region",
  subContinent: "Sub-Continent",
  submarket: "Submarket",
  chainScale: "Chain Scale",
  location: "Location",
  projectPhase: "project_phase",
  operationType: "Operation Type",
  managementCompany: "Management Company",
  propertyType: "Property Type",
  hotelServiceModel: "Hotel Service Model",
  /** Optional Phase 1B governance (may not exist in base yet). */
  includeInBrandExplorer:
    process.env.AIRTABLE_CENSUS_INCLUDE_IN_BRAND_EXPLORER_FIELD || "Include in Brand Explorer",
  dataConfidence:
    process.env.AIRTABLE_CENSUS_DATA_CONFIDENCE_FIELD || "Data Confidence",
};

export const ALIAS_FIELDS = {
  canonicalBrandName: "Canonical Brand Name",
  aliasSourceBrandName: "Alias / Source Brand Name",
  parentCompany: "Parent Company",
  active: "Active",
  matchConfidence: "Match Confidence",
  notes: "Notes",
};

/** Exact affiliation excluded from branded property rollups. */
export const CENSUS_INDEPENDENT_AFFILIATION = "Independent";

export const STATUS_OPEN = "Open";
export const STATUS_PIPELINE = "Pipeline";
