/**
 * StayingAPI field firewall — allowed vs prohibited Census mappings.
 * STAYINGAPI_ROOMS_CAPABILITY = NOT_SUPPORTED (no total hotel keys field).
 */

export const STAYINGAPI_ROOMS_CAPABILITY = "NOT_SUPPORTED";

export const ALLOWED_CANDIDATE_FIELDS = Object.freeze([
  "Property Name",
  "Address",
  "City",
  "State / Region",
  "Country",
  "Postal Code",
  "Latitude",
  "Longitude",
  "Property Type",
  "Amenities",
  "external_platform_property_id",
  "external_property_url",
  "image_urls_reference_only",
]);

/** Explicit never-map rules. */
export const PROHIBITED_MAPPINGS = Object.freeze([
  { from: "bedrooms", to: "Rooms / Keys", reason: "bedrooms_are_not_hotel_keys" },
  { from: "maxOccupancy", to: "Rooms / Keys", reason: "occupancy_is_not_hotel_keys" },
  { from: "room_types", to: "Rooms / Keys", reason: "room_types_are_not_hotel_keys" },
  { from: "listing_inventory", to: "Rooms / Keys", reason: "inventory_is_not_hotel_keys" },
  { from: "starRating|guestRating", to: "Dealality Segment / Positioning", reason: "ratings_are_not_segment" },
  { from: "OTA category", to: "Market|Submarket", reason: "ota_taxonomy_not_dealality_geo" },
  { from: "host", to: "Owner Name|Operator / Management Company", reason: "host_is_not_owner_or_operator" },
  { from: "platform", to: "Operator / Management Company", reason: "platform_is_not_operator" },
]);

export const NEVER_INFER_FIELDS = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Floors",
  "Rooms / Keys",
  "Restaurant Count",
  "Total Meeting Space",
  "Number of Meeting Rooms",
]);

/**
 * Strip prohibited fields from a normalized candidate before enrichment proposals.
 * @param {object} candidate
 */
export function applyFieldFirewall(candidate) {
  const out = { ...candidate };
  delete out.bedrooms;
  delete out.max_occupancy;
  delete out.host;
  delete out.guest_rating;
  delete out.star_rating;
  delete out.rooms_keys; // never allow
  out.rooms_capability = STAYINGAPI_ROOMS_CAPABILITY;
  out.firewall_version = "stayingapi-field-firewall-v1";
  return out;
}

export function buildFieldFirewallArtifact() {
  return {
    version: "stayingapi-field-firewall-v1",
    STAYINGAPI_ROOMS_CAPABILITY,
    allowed_candidate_fields: ALLOWED_CANDIDATE_FIELDS,
    prohibited_mappings: PROHIBITED_MAPPINGS,
    never_infer: NEVER_INFER_FIELDS,
    bedrooms_mapped_to_rooms_keys: false,
    note: "bedrooms / maxOccupancy MUST NEVER map to Rooms / Keys",
  };
}
