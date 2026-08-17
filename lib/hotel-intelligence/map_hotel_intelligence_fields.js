/**
 * Central field mapping: Dealality Hotel Property Census ↔ MCP canonical hotel.
 * Never scatter raw Airtable field names outside this map + census-read adapter.
 */

export const MAP_HOTEL_INTELLIGENCE_VERSION = "map-hotel-intelligence-fields-v1";

/** Production SoT table (Deal Capture Platform). */
export const MAP_HOTEL_PROPERTY_CENSUS = Object.freeze({
  tableName: "Hotel Property Census",
  tableId: "tbl9aY5ijiuIzzWam",
});

/**
 * Airtable field names on Hotel Property Census.
 * Keys = canonical MCP / internal keys.
 */
export const MAP_CENSUS_FIELDS = Object.freeze({
  propertyName: "Property Name",
  officialName: "Canonical Property Name",
  propertyIdentityKey: "Property Identity Key",
  address: "Address",
  city: "City",
  stateRegion: "State / Region",
  country: "Country",
  postalCode: "Postal Code",
  latitude: "Latitude",
  longitude: "Longitude",
  market: "Market",
  submarket: "Submarket",
  roomCount: "Rooms / Keys",
  propertyType: "Property Type",
  chainScale: "Chain Scale",
  status: "Production Use Status",
  enrichmentStatus: "Enrichment Status",
  brandName: "Current Brand",
  parentCompanyName: "Brand Family",
  affiliationStatus: "Affiliation Status",
  website: "Official Property URL",
  phone: "Phone",
  identityConfidence: "Identity Confidence",
  dataConfidenceTier: "Data Confidence Tier",
  sourceConfidence: "Source Confidence",
  lastReviewedAt: "Last Reviewed Date",
  reviewStatus: "Review Status",
  humanReviewRequired: "Human Review Required",
  hbxHotelCode: "HBX Hotel Code",
  hbxChainCode: "HBX Chain Code",
  hbxCategoryCode: "HBX Category Code",
  hbxLinkageConfidence: "HBX Linkage Confidence",
  hbxSourceStatus: "HBX Source Status",
});

/** Providers known to the MCP (extensible). */
export const MAP_PROVIDER_IDS = Object.freeze({
  hotelbeds: "hotelbeds",
  census: "dealality_census",
  stayingapi: "stayingapi",
  serpapi: "serpapi",
  giata_drive: "giata_drive",
  tripadvisor_apify: "tripadvisor_apify",
  google_places: "google_places",
  openstreetmap: "openstreetmap",
  official_site: "official_site",
  brand_directory: "brand_directory",
  manual: "manual",
});

/** MVP canonical fields exposed by hotel_get / resolve / enrich. */
export const MAP_MVP_CANONICAL_FIELDS = Object.freeze([
  "hotel_id",
  "official_name",
  "display_name",
  "address_line_1",
  "city",
  "country",
  "latitude",
  "longitude",
  "room_count",
  "brand_name",
  "parent_company_name",
  "website",
  "phone",
  "status",
  "record_confidence",
  "last_verified_at",
]);
