/**
 * GDI Private Events V1.1 — Airtable field maps + enums.
 * Tables live on canonical intelligence base (appa2cE7FTRmIbB32), not Deal Capture MVP.
 */

export const PE_VENUES_TABLE_NAME = "Private Event Venues";
export const HOTEL_VENUE_FIT_TABLE_NAME = "Hotel Venue Fit";
export const PE_SIGNALS_TABLE_NAME = "Private Event Signals";

/** Stable venue ID prefix — Dealality-style canonical, not hotel-scoped. */
export const PE_VENUE_ID_PREFIX = "pev_";

export const MAP_PE_VENUE = Object.freeze({
  venueId: "Venue ID",
  venueName: "Venue Name",
  venueAliases: "Venue Aliases",
  venueType: "Venue Type",
  address: "Address",
  city: "City",
  region: "State / Region",
  postalCode: "Postal Code",
  country: "Country",
  latitude: "Latitude",
  longitude: "Longitude",
  website: "Website",
  officialDomain: "Official Domain",
  minCapacity: "Min Capacity",
  maxCapacity: "Max Capacity",
  onSiteLodgingStatus: "On-Site Lodging Status",
  onSiteGuestrooms: "On-Site Guestrooms",
  weddingsAdvertised: "Weddings Advertised",
  privateEventsAdvertised: "Private Events Advertised",
  estimatedAnnualPrivateEvents: "Estimated Annual Private Events",
  annualEventVolumeStatus: "Annual Event Volume Status",
  preferredHotelListed: "Preferred Hotel Listed",
  exclusiveHotelRelationship: "Exclusive Hotel Relationship",
  knownHotelPartners: "Known Hotel Partners",
  /** V1.4 — multi-signal repeated activity (not only exact annual count). */
  eventActivityEvidenceStatus: "Event Activity Evidence Status",
  activityEvidenceJson: "Activity Evidence JSON",
  partnerStatus: "Partner Status",
  eventContactName: "Event Contact Name",
  eventContactRole: "Event Contact Role",
  eventContactEmail: "Event Contact Email",
  eventContactPhone: "Event Contact Phone",
  sourceUrls: "Source URLs",
  primarySourceUrl: "Primary Source URL",
  sourceAuthority: "Source Authority",
  researchStatus: "Research Status",
  confidence: "Confidence",
  firstSeen: "First Seen",
  lastSeen: "Last Seen",
  lastVerified: "Last Verified",
  canonicalStatus: "Canonical Status",
  matchedBy: "Matched By",
  matchConfidence: "Match Confidence",
  mergeReason: "Merge Reason",
  schemaVersion: "Schema Version",
  venuePayloadJson: "Venue Payload JSON",
});

export const MAP_HOTEL_VENUE_FIT = Object.freeze({
  fitId: "Hotel Venue Fit ID",
  hotelId: "Hotel ID",
  hotelName: "Hotel Name",
  venueId: "Venue ID",
  venueName: "Venue Name",
  /** Linked records to Private Event Venues (populated after schema ensure). */
  venueLink: "Venue",
  distanceMiles: "Distance Miles",
  distanceKm: "Distance KM",
  driveTimeMinutes: "Drive Time Minutes",
  lodgingCatchmentFit: "Lodging Catchment Fit",
  productFit: "Product Fit",
  partnershipPotential: "Partnership Potential",
  lodgingCapturePotential: "Lodging Capture Potential",
  existingHotelRelationshipStatus: "Existing Hotel Relationship Status",
  preferredPartnerStatus: "Preferred Partner Status",
  fitRationale: "Fit Rationale",
  whyHotelCouldWin: "Why Hotel Could Win",
  constraints: "Constraints",
  currentGdiOpportunityId: "Current GDI Opportunity ID",
  currentGdiOpportunityStatus: "Current GDI Opportunity Status",
  firstEvaluated: "First Evaluated",
  lastEvaluated: "Last Evaluated",
  lastChanged: "Last Changed",
  evidenceUrls: "Evidence URLs",
  confidence: "Confidence",
  schemaVersion: "Schema Version",
  fitPayloadJson: "Fit Payload JSON",
});

export const MAP_PE_SIGNAL = Object.freeze({
  signalId: "Event Signal ID",
  venueId: "Venue ID",
  venueName: "Venue Name",
  venueLink: "Venue",
  demandSignalType: "Demand Signal Type",
  eventName: "Event Name",
  eventType: "Event Type",
  eventStartDate: "Event Start Date",
  eventEndDate: "Event End Date",
  dateGranularity: "Date Granularity",
  estimatedAttendance: "Estimated Attendance",
  attendanceStatus: "Attendance Status",
  potentialRoomsLow: "Potential Rooms Low",
  potentialRoomsHigh: "Potential Rooms High",
  roomDemandStatus: "Room Demand Status",
  lodgingMentioned: "Lodging Mentioned",
  roomBlockMentioned: "Room Block Mentioned",
  hotelMentioned: "Hotel Mentioned",
  transportationMentioned: "Transportation Mentioned",
  plannerCompany: "Planner Company",
  plannerName: "Planner Name",
  plannerRole: "Planner Role",
  sourceUrl: "Source URL",
  sourceType: "Source Type",
  sourceAuthority: "Source Authority",
  signalStatus: "Signal Status",
  hotelDemandThesis: "Hotel Demand Thesis",
  evidenceText: "Evidence Text",
  firstSeen: "First Seen",
  lastSeen: "Last Seen",
  lastVerified: "Last Verified",
  promotedGdiOpportunityId: "Promoted GDI Opportunity ID",
  hotelVenueFitId: "Hotel Venue Fit ID",
  schemaVersion: "Schema Version",
  signalPayloadJson: "Signal Payload JSON",
});

/** GDI Opportunities extension fields (link PE graph → canonical opp). */
export const MAP_GDI_PE_LINK = Object.freeze({
  peVenueId: "peVenueId",
  peSignalId: "peSignalId",
  hotelVenueFitId: "hotelVenueFitId",
  demandFamily: "demandFamily",
  demandSignalType: "demandSignalType",
});

export const VAL_PE_VENUE_TYPE = Object.freeze([
  "ESTATE",
  "COUNTRY_CLUB",
  "PRIVATE_CLUB",
  "MUSEUM",
  "HISTORIC_VENUE",
  "GARDEN",
  "WINERY",
  "BANQUET_HALL",
  "RELIGIOUS_VENUE",
  "EVENT_CENTER",
  "CONFERENCE_CENTER",
  "RESTAURANT_EVENT_SPACE",
  "HOTEL_EVENT_SPACE",
  "RESORT",
  "UNIVERSITY_VENUE",
  "CULTURAL_VENUE",
  "SPORTS_VENUE",
  "OTHER",
]);

export const VAL_ON_SITE_LODGING = Object.freeze([
  "NO_LODGING",
  "LIMITED_LODGING",
  "ADEQUATE_LODGING",
  "UNKNOWN",
]);

export const VAL_RESEARCH_STATUS = Object.freeze([
  "DISCOVERED",
  "PARTIALLY_RESEARCHED",
  "VERIFIED",
  "NEEDS_REVIEW",
  "STALE",
]);

export const VAL_ANNUAL_EVENT_VOLUME_STATUS = Object.freeze([
  "CONFIRMED",
  "ESTIMATED",
  "UNKNOWN",
]);

export const VAL_CANONICAL_STATUS = Object.freeze([
  "ACTIVE",
  "MERGED",
  "DEPRECATED",
  "NEEDS_REVIEW",
]);

export const VAL_LODGING_CATCHMENT_FIT = Object.freeze([
  "CORE",
  "COMPETITIVE",
  "STRETCH",
  "OUTSIDE",
  "UNKNOWN",
]);

export const VAL_PRODUCT_FIT = Object.freeze([
  "STRONG",
  "MODERATE",
  "WEAK",
  "POOR",
  "UNKNOWN",
]);

export const VAL_PARTNERSHIP_POTENTIAL = Object.freeze([
  "HIGH",
  "MEDIUM",
  "LOW",
  "NONE",
  "UNKNOWN",
]);

export const VAL_LODGING_CAPTURE_POTENTIAL = Object.freeze([
  "HIGH",
  "MODERATE",
  "LOW",
  "UNKNOWN",
]);

export const VAL_SIGNAL_STATUS = Object.freeze([
  "NEW_SIGNAL",
  "RESEARCHING",
  "QUALIFIED",
  "WATCH",
  "REJECTED",
  "PROMOTED_TO_GDI",
]);

export const VAL_ROOM_DEMAND_CLAIM = Object.freeze([
  "CONFIRMED",
  "ESTIMATED",
  "MODELED",
  "UNKNOWN",
]);

export const VAL_ATTENDANCE_STATUS = Object.freeze([
  "CONFIRMED",
  "ESTIMATED",
  "UNKNOWN",
]);

export const VAL_PE_DEMAND_SIGNAL_TYPE = Object.freeze([
  "WEDDING",
  "PRIVATE_EVENT",
  "FAMILY_REUNION",
  "RELIGIOUS_CELEBRATION",
  "SOCIAL_EVENT",
  "DESTINATION_WEDDING",
  "VENUE_PARTNERSHIP",
  "EVENT_PLANNER_PARTNERSHIP",
  "OTHER_PRIVATE_EVENT",
]);

export const VAL_EVENT_ACTIVITY_EVIDENCE_STATUS = Object.freeze([
  "CONFIRMED_VOLUME",
  "STRONG_REPEATED_ACTIVITY",
  "MODERATE_ACTIVITY",
  "LIMITED_ACTIVITY",
  "UNKNOWN",
]);

export const VAL_PARTNER_STATUS = Object.freeze([
  "EXCLUSIVE_PARTNER_FOUND",
  "PREFERRED_PARTNER_FOUND",
  "NONEXCLUSIVE_PARTNERS_FOUND",
  "NO_PUBLIC_PARTNER_FOUND",
  "UNKNOWN",
]);

export const PE_AIRTABLE_SCHEMA_VERSION = "gdi_private_events_airtable_v1_4";

/** Map internal V1 venue types → Airtable enum. */
export function toAirtableVenueType(raw) {
  const t = String(raw || "").toUpperCase().replace(/[\s-]+/g, "_");
  const map = {
    WEDDING_VENUE: "EVENT_CENTER",
    EVENT_VENUE: "EVENT_CENTER",
    HISTORIC_ESTATE: "ESTATE",
    HISTORIC_VENUE: "HISTORIC_VENUE",
    COUNTRY_CLUB: "COUNTRY_CLUB",
    PRIVATE_CLUB: "PRIVATE_CLUB",
    MUSEUM: "MUSEUM",
    GARDEN: "GARDEN",
    WINERY: "WINERY",
    BANQUET_HALL: "BANQUET_HALL",
    RELIGIOUS_VENUE: "RELIGIOUS_VENUE",
    CONFERENCE_EVENT_CENTER: "CONFERENCE_CENTER",
    SOCIAL_EVENT_SPACE: "RESTAURANT_EVENT_SPACE",
    RESORT_OFFSITE: "RESORT",
    OTHER: "OTHER",
  };
  if (VAL_PE_VENUE_TYPE.includes(t)) return t;
  return map[t] || "OTHER";
}
