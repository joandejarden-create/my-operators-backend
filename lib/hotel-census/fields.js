/**
 * Hotel census field names for enrichment CLIs.
 *
 * Phase 2 (global retirement): ordinary CLI targets are Hotel Property Census.
 * Legacy Hotel Census field names are retained only as LEGACY_* exports for
 * explicit migration-read tooling (DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1).
 *
 * Product runtime (Scout, Brand Presence, BE footprint, operators-by-brand) uses
 * Hotel Property Census via hpc-scout-record-adapter / platform-base exports —
 * not this module's table constant.
 */

/**
 * Active CLI table — Hotel Property Census.
 * @deprecated Name retained for import compatibility; value is HPC, not legacy Census.
 */
export const HOTEL_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_CENSUS_TABLE ||
  process.env.AIRTABLE_HOTEL_PROPERTY_CENSUS_TABLE ||
  "Hotel Property Census";

/** Explicit legacy table name — migration-read tooling only. */
export const LEGACY_HOTEL_CENSUS_TABLE_NAME = "Hotel Census";
export const LEGACY_HOTEL_CENSUS_TABLE_ID = "tblgj2qEwxjTcg6q0";

export const BRAND_ALIAS_TABLE =
  process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping";

/**
 * Active CLI field map — HPC canonical names.
 * Keys preserved for script compatibility (CENSUS_FIELDS.name etc.).
 */
export const CENSUS_FIELDS = {
  name: "Property Name",
  affiliation: "Current Brand",
  parentCompany: "Parent Company",
  status: "Radar Status",
  rooms: "Rooms / Keys",
  country: "Country",
  city: "City",
  market: "Market",
  region: "Continent",
  subContinent: "Sub-Continent",
  submarket: "Submarket",
  chainScale: "Hotel Class / Segment",
  location: "Location",
  projectPhase: "project_phase",
  operationType: "Operation Type",
  managementCompany: "Operator / Management Company",
  propertyType: "Asset Context",
  hotelServiceModel: "Hotel Service Model",
  includeInBrandExplorer:
    process.env.AIRTABLE_CENSUS_INCLUDE_IN_BRAND_EXPLORER_FIELD || "Include in Brand Explorer",
  dataConfidence:
    process.env.AIRTABLE_CENSUS_DATA_CONFIDENCE_FIELD || "Data Confidence",
};

/**
 * Legacy Hotel Census field names — migration-read / reconciliation only.
 * Do not use for ordinary CLI writes.
 */
export const LEGACY_CENSUS_FIELDS = {
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
