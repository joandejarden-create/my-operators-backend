/**
 * Tripadvisor → Hotel Census / Hotel Intelligence field map + write tiers.
 * READ/POLICY only — does not invent Airtable columns.
 */

import { MAP_CENSUS_FIELDS } from "../map_hotel_intelligence_fields.js";

export const TRIPADVISOR_CENSUS_PROFILE_PACK_VERSION =
  "tripadvisor-census-profile-pack-v1";

/** Write policy tiers */
export const WRITE_TIER = Object.freeze({
  A_SAFE_GAP_FILL: "TIER_A_SAFE_GAP_FILL",
  B_CONDITIONAL: "TIER_B_CONDITIONAL",
  C_CANDIDATE_ONLY: "TIER_C_CANDIDATE_ONLY",
  HI_ONLY: "HOTEL_INTELLIGENCE_ONLY",
  DO_NOT_STORE: "DO_NOT_STORE",
});

/**
 * Map Tripadvisor Actor fields → Dealality destination.
 * censusField = Airtable name when EXISTING; null when HI-only / external-id / do-not-store.
 */
export const TA_FIELD_MAP = Object.freeze([
  {
    ta: "id",
    destination: "external_id_registry",
    classification: "PROVENANCE_ONLY",
    tier: WRITE_TIER.A_SAFE_GAP_FILL,
    note: "Tripadvisor property ID via hotel_id ↔ external_ids (not a census column)",
  },
  {
    ta: "name",
    destination: "match_only",
    classification: "DO_NOT_STORE",
    tier: WRITE_TIER.DO_NOT_STORE,
    note: "Use for identity match; do not overwrite census names",
  },
  {
    ta: "webUrl",
    destination: "provenance",
    classification: "PROVENANCE_ONLY",
    tier: WRITE_TIER.HI_ONLY,
    note: "Source URL for observations",
  },
  {
    ta: "website",
    censusField: MAP_CENSUS_FIELDS.website, // Official Property URL
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.A_SAFE_GAP_FILL,
    note: "Null-fill Official Property URL when domain validates",
  },
  {
    ta: "phone",
    censusField: MAP_CENSUS_FIELDS.phone,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.A_SAFE_GAP_FILL,
    note: "Null-fill Phone + Phone Source Type/URL/Confidence when empty",
  },
  {
    ta: "email",
    censusField: null,
    classification: "NEW_CENSUS_FIELD_RECOMMENDED",
    recommendedCensusFields: [
      "Email",
      "Email Confidence",
      "Email Source Type",
      "Email Source URL",
    ],
    tier: WRITE_TIER.B_CONDITIONAL,
    note: "No Email column today — recommend Phone-parity provenance suite; until then HI-only",
  },
  {
    ta: "address / addressObj",
    censusField: MAP_CENSUS_FIELDS.address,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.A_SAFE_GAP_FILL,
    note: "Null-fill Address when missing; city/country usually already present on shells",
  },
  {
    ta: "addressObj.city",
    censusField: MAP_CENSUS_FIELDS.city,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.B_CONDITIONAL,
    note: "Only fill if City empty AND country compatible — rare on census shells",
  },
  {
    ta: "addressObj.state",
    censusField: MAP_CENSUS_FIELDS.stateRegion,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.B_CONDITIONAL,
    note: "Null-fill State / Region cautiously",
  },
  {
    ta: "addressObj.country",
    censusField: MAP_CENSUS_FIELDS.country,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.DO_NOT_STORE,
    note: "Never overwrite Country from Tripadvisor",
  },
  {
    ta: "addressObj.postalcode",
    censusField: null,
    classification: "DO_NOT_STORE",
    tier: WRITE_TIER.DO_NOT_STORE,
    note: "Postal Code not on live census schema — do not invent column unless product asks",
  },
  {
    ta: "latitude",
    censusField: MAP_CENSUS_FIELDS.latitude,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.A_SAFE_GAP_FILL,
    note: "Null-fill coords only; ENABLE_COORDINATE_WRITES must be on for apply",
  },
  {
    ta: "longitude",
    censusField: MAP_CENSUS_FIELDS.longitude,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.A_SAFE_GAP_FILL,
    note: "Paired with latitude",
  },
  {
    ta: "numberOfRooms",
    censusField: MAP_CENSUS_FIELDS.roomCount,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.C_CANDIDATE_ONLY,
    note: "Never auto-authoritative; candidate/provenance model only",
  },
  {
    ta: "hotelClass",
    censusField: "Hotel Class / Segment",
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.B_CONDITIONAL,
    note: "Retain attribution; methodologies differ (often Giata)",
  },
  {
    ta: "subcategories",
    censusField: MAP_CENSUS_FIELDS.propertyType,
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.B_CONDITIONAL,
    note: "Map cautiously to Property Type; do not invent options",
  },
  {
    ta: "amenities",
    censusField: "Amenities - Structured Tags",
    classification: "EXISTING_CENSUS_FIELD",
    tier: WRITE_TIER.B_CONDITIONAL,
    note: "Append/normalize tags when empty; keep Amenities - Source Text as research-only",
  },
  {
    ta: "brand (inferred)",
    censusField: MAP_CENSUS_FIELDS.brandName,
    classification: "DO_NOT_STORE",
    tier: WRITE_TIER.DO_NOT_STORE,
    note: "Do not infer Current Brand from Tripadvisor name",
  },
  // Hotel Intelligence only
  ...[
    "rating",
    "numberOfReviews",
    "rankingPosition",
    "rankingDenominator",
    "rankingString",
    "ratingHistogram",
    "categoryReviewScores",
    "priceLevel",
    "priceRange",
    "photoCount",
    "travelerChoiceAward",
    "rawRanking",
  ].map((ta) => ({
    ta,
    censusField: null,
    classification: "HOTEL_INTELLIGENCE_ONLY",
    tier: WRITE_TIER.HI_ONLY,
    note: "Dynamic observational Profile Pack — not durable census identity",
  })),
  ...[
    "description",
    "roomTips",
    "aiReviewsSummary",
    "image",
    "photos",
    "offers",
    "checkInDate",
    "checkOutDate",
  ].map((ta) => ({
    ta,
    censusField: null,
    classification: "DO_NOT_STORE",
    tier: WRITE_TIER.DO_NOT_STORE,
    note: "Rights / transient — research input only",
  })),
]);

/** Tier A fields eligible for null-fill (Airtable names). */
export const TIER_A_CENSUS_FIELDS = Object.freeze([
  MAP_CENSUS_FIELDS.website,
  MAP_CENSUS_FIELDS.phone,
  MAP_CENSUS_FIELDS.address,
  MAP_CENSUS_FIELDS.latitude,
  MAP_CENSUS_FIELDS.longitude,
]);

/** Tier B conditional. */
export const TIER_B_CENSUS_FIELDS = Object.freeze([
  "Hotel Class / Segment",
  MAP_CENSUS_FIELDS.propertyType,
  MAP_CENSUS_FIELDS.stateRegion,
  MAP_CENSUS_FIELDS.city,
  "Amenities - Structured Tags",
]);

/** Completeness priority fields for gap matrix. */
export const COMPLETENESS_PRIORITY_FIELDS = Object.freeze([
  MAP_CENSUS_FIELDS.address,
  MAP_CENSUS_FIELDS.city,
  MAP_CENSUS_FIELDS.stateRegion,
  MAP_CENSUS_FIELDS.country,
  MAP_CENSUS_FIELDS.latitude,
  MAP_CENSUS_FIELDS.longitude,
  MAP_CENSUS_FIELDS.roomCount,
  "Hotel Class / Segment",
  MAP_CENSUS_FIELDS.website,
  MAP_CENSUS_FIELDS.phone,
  MAP_CENSUS_FIELDS.brandName,
  MAP_CENSUS_FIELDS.propertyType,
]);

export const CALA_COUNTRIES = Object.freeze([
  "Mexico",
  "Dominican Republic",
  "Colombia",
  "Costa Rica",
  "Panama",
  "Puerto Rico",
  "Jamaica",
  "Cuba",
  "Bahamas",
  "Barbados",
  "Trinidad and Tobago",
  "Aruba",
  "Curaçao",
  "Curacao",
  "Guatemala",
  "Honduras",
  "El Salvador",
  "Nicaragua",
  "Belize",
  "Haiti",
  "Peru",
  "Ecuador",
  "Chile",
  "Argentina",
  "Brazil",
  "Uruguay",
  "Paraguay",
  "Bolivia",
  "Venezuela",
]);
