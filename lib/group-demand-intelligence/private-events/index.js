/**
 * GDI Private Events + Wedding Demand V1 — public surface.
 */

export {
  PRIVATE_EVENTS_V1,
  VENUE_TYPE,
  ON_SITE_LODGING_STATUS,
  VENUE_PRIORITY,
  BUYER_PATH,
  COMMERCIAL_BUYER_PATHS,
  ATTENDANCE_STATUS,
  ROOM_DEMAND_CLAIM,
  NONLOCALITY,
  HOTEL_ARCHETYPE_PE,
  CATCHMENT_MILES_BY_ARCHETYPE,
  ARCHETYPE_VENUE_MIX,
  EVENT_ACTIVITY_EVIDENCE_STATUS,
  ACTIVITY_EVIDENCE_TYPE,
  PARTNER_STATUS,
  PARTNERSHIP_CONFIDENCE,
} from "./constants.js";

export {
  computeVenueId,
  buildVenueEntity,
  createVenueGraph,
  normalizeOnSiteLodgingStatus,
  normalizeVenueType,
} from "./venue-entity.js";

export {
  resolveHotelArchetype,
  practicalCatchmentMiles,
  distanceMiles,
  buildHotelContext,
  buildHotelVenueRelationship,
} from "./hotel-context.js";

export {
  classifyAttendance,
  classifyNonlocality,
  classifyBuyerPath,
  isPrivatePersonSourceRejected,
  modelRoomDemand,
  assessLodgingCapture,
} from "./lodging-capture.js";

export {
  classifyEventActivityEvidence,
  extractActivityEvidenceFromPage,
  dedupeActivityEvidence,
  classifyPartnerStatus,
  activitySupportsTruePartnership,
  isOfficialVenueUrl,
  isDirectoryUrl,
} from "./activity-evidence.js";

export { classifyVenuePriority } from "./venue-priority.js";

export {
  qualifySpecificEvent,
  qualifyVenuePartnership,
  LIFECYCLE,
} from "./qualify.js";

export { runPrivateEventsForHotel } from "./run-pipeline.js";

export {
  researchVenuePartnershipSecondPass,
  buildPartnershipSecondPassQueries,
} from "./partnership-second-pass.js";

export {
  LIVE_VENUE_DISCOVERY_V1_2,
  buildVenueDiscoveryQueries,
  discoverLiveVenues,
  mapExtractToVenue,
} from "./live-venue-discovery.js";

export {
  LIVE_EVENT_SIGNALS_V1_2,
  buildForwardEventQueries,
  discoverForwardEventSignals,
} from "./live-event-signals.js";

export {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  MAP_GDI_PE_LINK,
  PE_AIRTABLE_SCHEMA_VERSION,
  toAirtableVenueType,
} from "./airtable-field-map.js";

export {
  findVenueMatch,
  upsertVenue,
  listVenuesFromAirtable,
  computeStableVenueId,
} from "./airtable-venue-store.js";

export { upsertHotelVenueFit } from "./airtable-fit-store.js";

export { upsertPrivateEventSignal } from "./airtable-signal-store.js";

export {
  buildGdiOpportunityFromPrivateEvent,
  computePeOpportunityId,
  peLinkAirtableFields,
} from "./promote-to-gdi.js";

export {
  enrichPrivateEventOpportunityDetail,
  normalizePeCustomerSources,
  isPrivateEventOpportunity,
  isVenuePartnership,
  PE_CUSTOMER_LABELS,
} from "./customer-detail-enrichment.js";
