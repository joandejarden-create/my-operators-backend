/**
 * SerpApi Google Hotels field firewall.
 * SERPAPI_ROOMS_CAPABILITY = NOT_SUPPORTED — no total hotel Rooms/Keys field.
 *
 * Documented `rooms` arrays are bookable room *types* (e.g. "King Room"), not keys.
 * `essential_info` bedroom counts are vacation-rental bedrooms, not hotel keys.
 */

export const SERPAPI_ROOMS_CAPABILITY = "NOT_SUPPORTED";

export const ALLOWED_CANDIDATE_FIELDS = Object.freeze([
  "Property Name",
  "Address",
  "City",
  "State / Region",
  "Country",
  "Postal Code",
  "Latitude",
  "Longitude",
  "Telephone",
  "Website",
  "Hotel Class (raw source value)",
  "Property Type (where safely mappable)",
  "Amenities",
  "Excluded Amenities",
  "Google Hotels Property Token / ID",
  "Google property URL",
  "image_urls_reference_only",
  "description_analysis_only",
]);

export const PROHIBITED_MAPPINGS = Object.freeze([
  { from: "hotel_class|extracted_hotel_class", to: "STR Chain Scale", reason: "google_class_is_not_str_scale" },
  { from: "hotel_class|extracted_hotel_class", to: "Dealality Segment", reason: "no_auto_segment_mapping" },
  { from: "rooms[] (bookable room types)", to: "Rooms / Keys", reason: "room_types_are_not_hotel_keys" },
  { from: "essential_info bedrooms", to: "Rooms / Keys", reason: "vr_bedrooms_are_not_hotel_keys" },
  { from: "num_guests|occupancy", to: "Rooms / Keys", reason: "occupancy_is_not_hotel_keys" },
  { from: "price|rate_per_night|availability", to: "Rooms / Keys", reason: "price_is_not_keys" },
  { from: "reviews|overall_rating", to: "Rooms / Keys", reason: "reviews_are_not_keys" },
  { from: "booking source|platform", to: "Operator / Management Company", reason: "platform_is_not_operator" },
  { from: "host/provider", to: "Owner Name", reason: "host_is_not_owner" },
  { from: "inferred dates", to: "Opening Date|Renovation Date", reason: "no_date_inference" },
  { from: "google geo labels", to: "Market|Submarket", reason: "do_not_replace_dealality_geo" },
  { from: "images", to: "production image reuse", reason: "image_reuse_not_approved" },
]);

export const NEVER_INFER_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Owner Name",
  "Developer Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Market",
  "Submarket",
  "STR Chain Scale",
  "Dealality Segment",
]);

/**
 * Strip prohibited fields from a normalized candidate before enrichment proposals.
 * @param {object} candidate
 */
export function applyFieldFirewall(candidate) {
  const out = { ...candidate };
  delete out.rooms_keys;
  delete out.bedrooms;
  delete out.max_occupancy;
  delete out.bookable_room_types; // never promote to Keys
  out.rooms_capability = SERPAPI_ROOMS_CAPABILITY;
  out.firewall_version = "serpapi-google-hotels-field-firewall-v1";
  return out;
}

export function buildFieldFirewallArtifact() {
  return {
    version: "serpapi-google-hotels-field-firewall-v1",
    SERPAPI_ROOMS_CAPABILITY,
    allowed_candidate_fields: ALLOWED_CANDIDATE_FIELDS,
    prohibited_mappings: PROHIBITED_MAPPINGS,
    never_infer: NEVER_INFER_FIELDS,
    rooms_field_meaning:
      "SerpApi Google Hotels `rooms` arrays are bookable room-type offers, not total hotel keys. Vacation-rental `essential_info` bedroom counts are not hotel keys.",
    bedrooms_mapped_to_rooms_keys: false,
    note: "SERPAPI_ROOMS_CAPABILITY = NOT_SUPPORTED. Never invent Rooms/Keys from SerpApi.",
  };
}
