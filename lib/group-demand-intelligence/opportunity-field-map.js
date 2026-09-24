/**
 * Group Demand Opportunities — Airtable field map (GDI-specific table).
 * Decisions / Decision Events remain shared module-neutral tables.
 */

export const GDI_OPPORTUNITIES_TABLE_NAME = "Group Demand Opportunities";

export const MAP_GDI_OPPORTUNITY = Object.freeze({
  opportunityId: "opportunityId",
  hotelId: "hotelId",
  organizationId: "organizationId",
  opportunityName: "opportunityName",
  organizationName: "organizationName",
  organizationEntityId: "organizationEntityId",
  opportunityType: "opportunityType",
  priority: "priority",
  actionStatus: "actionStatus",
  eventStartDate: "eventStartDate",
  eventEndDate: "eventEndDate",
  segment: "segment",
  territory: "territory",
  venueStatus: "venueStatus",
  sourcingStatus: "sourcingStatus",
  attendance: "attendance",
  attendanceStatus: "attendanceStatus",
  peakRooms: "peakRooms",
  roomDemandStatus: "roomDemandStatus",
  hotelFit: "hotelFit",
  qualification: "qualification",
  evidenceConfidence: "evidenceConfidence",
  whyNow: "whyNow",
  whyThisMatters: "whyThisMatters",
  hotelWinThesis: "hotelWinThesis",
  recommendedAction: "recommendedAction",
  primaryContactId: "primaryContactId",
  primaryContactName: "primaryContactName",
  primaryContactRole: "primaryContactRole",
  primaryContactEmail: "primaryContactEmail",
  primaryContactPhone: "primaryContactPhone",
  contactQuality: "contactQuality",
  sourceCount: "sourceCount",
  sourceSummary: "sourceSummary",
  lastResearchedAt: "lastResearchedAt",
  researchVersion: "researchVersion",
  decisionId: "decisionId",
  createdAt: "createdAt",
  updatedAt: "updatedAt",
  schemaVersion: "schemaVersion",
  /** Lossless customer-facing opportunity JSON for UI fidelity. */
  opportunityPayloadJson: "opportunityPayloadJson",
  hotelIdentityStatus: "hotelIdentityStatus",
  runId: "runId",
  /** Private Events V1.1 — graph linkage into canonical GDI opportunity */
  peVenueId: "peVenueId",
  peSignalId: "peSignalId",
  hotelVenueFitId: "hotelVenueFitId",
  demandFamily: "demandFamily",
  demandSignalType: "demandSignalType",
  /** When true, excluded from customer list/share/CSV APIs. */
  isTestData: "isTestData",
});

/** Live GDI enums (preserve existing codes; include brief aliases). */
export const VAL_GDI_PRIORITY = Object.freeze([
  "HIGH_PRIORITY",
  "MEDIUM_PRIORITY",
  "WATCHLIST",
  "DISQUALIFIED",
  "HIGH",
  "MEDIUM",
]);

export const VAL_GDI_OPPORTUNITY_TYPE = Object.freeze([
  "PRIMARY_PURSUIT",
  "OVERFLOW_HOUSING",
  "REACTIVATION",
  "FUTURE_CYCLE",
  "SPECIFIC_PRIVATE_EVENT",
  "VENUE_PARTNERSHIP",
  "CLOSED_DISQUALIFIED",
]);

/**
 * Full opportunityType allowlist for Airtable column writes.
 * PE types (SPECIFIC_PRIVATE_EVENT, VENUE_PARTNERSHIP) are first-class after
 * the canonical schema repair; payload remains a fidelity fallback only.
 */
export const VAL_GDI_OPPORTUNITY_TYPE_AIRTABLE_SAFE = VAL_GDI_OPPORTUNITY_TYPE;

export const VAL_GDI_VENUE_STATUS = Object.freeze([
  "OPEN_UNRESOLVED",
  "RFP_ACTIVE_SOURCING",
  "HOTEL_VENUE_TBD",
  "PARTIALLY_PLACED",
  "PRIMARY_SELECTED_OVERFLOW_POSSIBLE",
  "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
  "PRIMARY_SELECTED_NO_OVERFLOW",
  "PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE",
  "FULLY_PLACED",
  "CURRENT_CYCLE_CLOSED",
  "UNKNOWN",
]);

export const VAL_GDI_ROOM_DEMAND = Object.freeze([
  "VERIFIED_ROOM_BLOCK",
  "VERIFIED_HOUSING_PROGRAM",
  "STRONG_ROOM_DEMAND_EVIDENCE",
  "ESTIMATED_ROOM_DEMAND",
  "LOCAL_LIMITED_ROOM_DEMAND",
  "OVERFLOW_ONLY",
  "UNKNOWN",
]);

export const GDI_OPPORTUNITY_SCHEMA_VERSION = "gdi_opportunity_airtable_v1";
