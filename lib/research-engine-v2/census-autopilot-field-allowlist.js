/**
 * Census Autopilot v2 — production field allowlist / forbid list.
 * Apply writes only High-confidence values on allowlisted Hotel Property Census fields.
 */

import {
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";

export const AUTOPILOT_TARGET_BASE_LABEL = productionHotelPropertyCensus.baseName;
export const AUTOPILOT_TARGET_TABLE = productionHotelPropertyCensus.tableName;
export const AUTOPILOT_TARGET_TABLE_ID =
  productionHotelPropertyCensus.tableId || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

/** Allowed early Autopilot production writes (High confidence only). */
export const AUTOPILOT_ALLOWED_WRITE_FIELDS = Object.freeze([
  // Core identity / source (key-field completion)
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "City",
  "State / Region",
  "Country",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Discovery Source",
  "Source Candidate Type",
  "Candidate Source Count",
  "Review Status",
  "Shell Insert Batch ID",
  "Shell Insert Country Batch",
  "Shell Insert Date",
  "Shell Insert Source Mix",
  "Shell Dedupe Confidence",
  "Candidate Brand Text",
  "Candidate Brand Family",
  "Candidate Brand Source",
  "Candidate Brand Confidence",
  "Brand Validation Status",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "Production Use Status",
  // Contact
  "Phone",
  "Phone Confidence",
  "Phone Source Type",
  "Phone Source URL",
  "Phone Review Status",
  "Phone Reviewed Date",
  "Phone Notes",
  "Notes for Steward",
  // HBX identity / provenance (Hotelbeds Content API linkage)
  "HBX Hotel Code",
  "HBX Chain Code",
  "HBX Category Code",
  "HBX Category Name",
  "HBX Accommodation Type",
  "HBX License / Registration Number",
  "HBX Last Update",
  "HBX Linkage Confidence",
  "HBX Source Status",
  "HBX Content Review Status",
  // Geography / Radar
  "Address",
  "Latitude",
  "Longitude",
  "Address Confidence",
  "Address Source URL",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
  "Postal Code",
  "Radar Display Status",
  "Radar Display Reason",
  "Radar Geography Status",
  "Public Census Eligibility",
  "Public Display Confidence",
  "Public Display Review Status",
  // Descriptions / amenities / asset
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Property Type",
  "Asset Context",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Market / Submarket",
  "Data Confidence Tier",
  "F&B Flag",
  "Meeting Space Flag",
  "Resort / Leisure Flag",
  "Extended Stay Flag",
  "Mixed-Use Flag",
  "Branded Residences Flag",
  // Rooms / Keys (+ optional v1.1.4)
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Rooms Notes",
  // Governance
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Human Review Required",
]);

export const AUTOPILOT_GEOCODE_FIELDS = Object.freeze([
  "Latitude",
  "Longitude",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
]);

export const AUTOPILOT_FORBIDDEN_FIELDS = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Developer",
  "Operator / Management Company",
  "Opening Date",
  "Renovation / Conversion Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "External Display Status",
  "Release Status",
  "Primary Release",
]);

export const AUTOPILOT_PROTECTED_BRAND_EXPLORER_HINTS = Object.freeze([
  /brand explorer/i,
  /presentation/i,
  /company validated/i,
  /company validation/i,
  /brand verified/i,
  /brand status/i,
  /recent momentum/i,
  /release/i,
]);

/**
 * @param {string} fieldName
 */
export function isAllowedAutopilotField(fieldName) {
  return AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(fieldName);
}

/**
 * @param {string} fieldName
 */
export function isForbiddenAutopilotField(fieldName) {
  if (AUTOPILOT_FORBIDDEN_FIELDS.includes(fieldName)) return true;
  return AUTOPILOT_PROTECTED_BRAND_EXPLORER_HINTS.some((re) => re.test(fieldName));
}

/**
 * @param {string} fieldName
 */
export function isGeocodeField(fieldName) {
  return AUTOPILOT_GEOCODE_FIELDS.includes(fieldName);
}

/**
 * Strip patch to allowlisted fields only; report drops.
 * @param {Record<string, unknown>} patch
 * @param {{ allowGeocode?: boolean, schemaV114Ready?: boolean }} [opts]
 */
export function sanitizeAutopilotPatch(patch = {}, opts = {}) {
  const out = {};
  const dropped = [];
  const v114 = new Set(["Rooms Source Type", "Rooms Reviewed Date", "Rooms Notes"]);

  for (const [k, v] of Object.entries(patch || {})) {
    if (k.startsWith("_")) continue;
    if (isForbiddenAutopilotField(k)) {
      dropped.push({ field: k, reason: "forbidden" });
      continue;
    }
    if (!isAllowedAutopilotField(k)) {
      dropped.push({ field: k, reason: "not_allowlisted" });
      continue;
    }
    if (v114.has(k) && !opts.schemaV114Ready) {
      dropped.push({ field: k, reason: "v114_schema_missing" });
      continue;
    }
    if (isGeocodeField(k) && !opts.allowGeocode) {
      dropped.push({ field: k, reason: "provider_decision_needed" });
      continue;
    }
    if (v === undefined) continue;
    out[k] = v;
  }
  return { fields: out, dropped };
}
