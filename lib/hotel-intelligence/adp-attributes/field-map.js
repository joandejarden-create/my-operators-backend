/**
 * Hotel ADP Attributes — Airtable field map + controlled values.
 * Derived audit trail of attributes ADP consumes (not a parallel hotel SoT).
 */

export const HOTEL_ADP_ATTRIBUTES_TABLE = "Hotel ADP Attributes";
export const HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION = "hotel_adp_attributes_v1";
export const HOTEL_ADP_ATTRIBUTE_VERSION = "adp_attr_v1";

export const MAP_HOTEL_ADP_ATTRIBUTE = Object.freeze({
  attributeKey: "Attribute Key",
  hpcHotelId: "HPC Hotel ID",
  dealalityHotelId: "Dealality Hotel ID",
  adpPropertyId: "ADP Property ID",
  hotelName: "Hotel Name",
  attributeCategory: "Attribute Category",
  attributeName: "Attribute Name",
  attributeValue: "Attribute Value",
  normalizedValue: "Normalized Value",
  usedInAdp: "Used in ADP?",
  adpUseType: "ADP Use Type",
  sourceType: "Source Type",
  sourceRecordId: "Source Record ID",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  confidence: "Confidence",
  effectiveFrom: "Effective From",
  effectiveTo: "Effective To",
  attributeVersion: "Attribute Version",
  lastVerifiedAt: "Last Verified At",
  active: "Active?",
  notes: "Notes",
  dedupeKey: "Dedupe Key",
  schemaVersion: "Schema Version",
});

export const VAL_ATTRIBUTE_CATEGORY = Object.freeze([
  "Identity",
  "Location",
  "Commercial",
  "Demand Node",
  "Meeting / Group",
  "Positioning",
  "Amenity",
  "Seasonality",
  "Need Period",
  "Segment Relevance",
  "Competitive Context",
  "Other",
]);

export const VAL_ADP_USE_TYPE = Object.freeze([
  "Query Generation",
  "Prompt Context",
  "Demand Family Selection",
  "Scoring",
  "Exclusion Logic",
  "Recommendation Context",
  "Reporting Only",
]);

export const VAL_SOURCE_TYPE = Object.freeze([
  "HPC",
  "Hotel Commercial Profile",
  "Hotel Demand Node",
  "Hotel Event Space",
  "Hotel Seasonality",
  "Hotel Supplied",
  "Public Research",
  "Model Derived",
]);

export const VAL_CONFIDENCE = Object.freeze(["HIGH", "MEDIUM", "LOW", "UNVERIFIED"]);

/**
 * Build stable dedupe key: HPC + category + attribute name + attribute version.
 */
export function buildAttributeDedupeKey(
  hpcHotelId,
  attributeName,
  attributeVersion,
  attributeCategory = ""
) {
  const h = String(hpcHotelId || "").trim();
  const c = String(attributeCategory || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
  const n = String(attributeName || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
  const v = String(attributeVersion || HOTEL_ADP_ATTRIBUTE_VERSION).trim();
  return c ? `${h}::${c}::${n}::${v}` : `${h}::${n}::${v}`;
}
