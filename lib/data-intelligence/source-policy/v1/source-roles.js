/**
 * Canonical source roles for Dealality hotel / property facts.
 * Policy version: source-policy-v1
 */

export const SOURCE_POLICY_VERSION = "source-policy-v1";

export const SourceRole = Object.freeze({
  VERIFIED_PRIMARY: "VERIFIED_PRIMARY",
  VERIFIED_SECONDARY: "VERIFIED_SECONDARY",
  DISCOVERY_ONLY: "DISCOVERY_ONLY",
  UNVERIFIED: "UNVERIFIED",
  BLOCKED: "BLOCKED",
});

/** Content / domain classifier (orthogonal to SourceRole). */
export const SourceContentDomain = Object.freeze({
  CVENT_VENUE_HOTEL: "CVENT_VENUE_HOTEL",
  CVENT_EVENT_PLATFORM: "CVENT_EVENT_PLATFORM",
  FIRST_PARTY_HOTEL: "FIRST_PARTY_HOTEL",
  BRAND_API: "BRAND_API",
  OTA: "OTA",
  HBX: "HBX",
  GIATA: "GIATA",
  OSM_WIKIDATA: "OSM_WIKIDATA",
  SEARCH_RESULT: "SEARCH_RESULT",
  OTHER: "OTHER",
});

export const VerificationStatus = Object.freeze({
  VERIFIED_PRIMARY: "VERIFIED_PRIMARY",
  VERIFIED_MULTI_SOURCE: "VERIFIED_MULTI_SOURCE",
  PROVISIONAL: "PROVISIONAL",
  CONFLICT: "CONFLICT",
  UNVERIFIED: "UNVERIFIED",
  DISCOVERY_ONLY: "DISCOVERY_ONLY",
  NEEDS_SOURCE_REVIEW: "NEEDS_SOURCE_REVIEW",
});

/** Hotel property fact fields that Cvent venue pages must not canonicalize. */
export const CVENT_VENUE_BLOCKED_FIELDS = Object.freeze([
  "Rooms / Keys",
  "roomsKeys",
  "suites",
  "totalMeetingSpaceSqFt",
  "totalMeetingSpaceSqM",
  "meetingRoomCount",
  "largestMeetingSpaceSqFt",
  "largestEventCapacity",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "listingText",
  "hotelOverview",
  "yearBuilt",
  "yearRenovated",
  "airportDistanceMiles",
  "Address",
  "Phone",
  "Current Brand",
]);
