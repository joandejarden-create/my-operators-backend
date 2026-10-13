/**
 * Promote qualified PE signals / venue partnerships into canonical GDI opportunities.
 * Does NOT create a separate opportunity table.
 */

import { createHash } from "node:crypto";
import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { DEMAND_FAMILY, DEMAND_SIGNAL_TYPE } from "../demand-signal-types.js";
import { MAP_GDI_PE_LINK } from "./airtable-field-map.js";

export function computePeOpportunityId({ hotelId, venueId, signalId, kind }) {
  const seed = [kind || "pe", hotelId || "", venueId || "", signalId || ""].join("|");
  return `gdi_pe_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Build a canonical GDI opportunity object (in-memory) from PE graph nodes.
 * Persistence goes through existing airtable-opportunity-store when apply=true.
 */
export function buildGdiOpportunityFromPrivateEvent({
  hotel,
  venue,
  fit,
  signal = null,
  qualification = {},
  isTestData = false,
} = {}) {
  const hotelId = hotel?.hotelId || hotel?.id;
  const isPartnership = !signal || qualification.opportunityType === OPPORTUNITY_TYPE.VENUE_PARTNERSHIP;
  const opportunityType = isPartnership
    ? OPPORTUNITY_TYPE.VENUE_PARTNERSHIP
    : OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT;
  const demandSignalType = isPartnership
    ? DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP
    : qualification.demandSignalType ||
      signal?.demandSignalType ||
      signal?.eventType ||
      DEMAND_SIGNAL_TYPE.PRIVATE_EVENT;

  const id = computePeOpportunityId({
    hotelId,
    venueId: venue?.venueId,
    signalId: signal?.signalId || null,
    kind: isPartnership ? "partnership" : "event",
  });

  const title = isPartnership
    ? `Venue partnership: ${venue?.venueName || "Private event venue"}`
    : signal?.eventName || `Private event at ${venue?.venueName || "venue"}`;

  const sources = [
    ...(venue?.sourceUrls || []).map((url) => ({ url, type: "venue" })),
    ...(signal?.sourceUrl ? [{ url: signal.sourceUrl, type: "signal" }] : []),
  ];
  const exampleDomain = sources.some((s) => /\.example\b/i.test(s.url || ""));
  const syntheticName =
    /\b(?:GARDEN|WEDDING(?:\s+VENUE)?|COUNTRY(?:\s+CLUB)?|BANQUET(?:\s+HALL)?|EVENT(?:\s+VENUE)?|MUSEUM|RELIGIOUS(?:\s+VENUE)?|HISTORIC(?:\s+ESTATE)?|PRIVATE(?:\s+CLUB)?|WINERY)\s+\d+\b/i.test(
      String(venue?.venueName || "")
    );
  const treatAsTest = Boolean(isTestData || qualification.isTestData || exampleDomain || syntheticName);

  return {
    id,
    opportunityId: id,
    hotelId,
    title,
    opportunityName: title,
    organizationName: venue?.venueName || signal?.plannerCompany || null,
    opportunityType,
    demandFamily: DEMAND_FAMILY.PRIVATE_EVENTS,
    demandSignalType,
    peVenueId: venue?.venueId || null,
    peSignalId: signal?.signalId || null,
    hotelVenueFitId: fit?.fitId || null,
    eventStartDate: signal?.eventStartDate || signal?.eventDate || null,
    eventEndDate: signal?.eventEndDate || null,
    attendance: signal?.estimatedAttendance ?? null,
    attendanceStatus: signal?.attendanceStatus || null,
    peakRooms: signal?.potentialRoomsHigh ?? null,
    roomDemandStatus:
      signal?.roomDemandStatus === "MODELED"
        ? "ESTIMATED_ROOM_DEMAND"
        : signal?.roomDemandStatus === "CONFIRMED"
          ? "VERIFIED_ROOM_BLOCK"
          : "UNKNOWN",
    whyNow: qualification.whyOpportunityExists || qualification.whyHotelCouldWin || null,
    hotelWinThesis: qualification.whyHotelCouldWin || fit?.whyHotelCouldWin || null,
    recommendedAction: qualification.recommendedAction || null,
    sources,
    potentialRoomsLow: signal?.potentialRoomsLow ?? null,
    potentialRoomsHigh: signal?.potentialRoomsHigh ?? null,
    lodgingCatchmentFit: fit?.lodgingCatchmentFit || null,
    qualificationGate: qualification.qualification || null,
    lifecycle: qualification.lifecycle || null,
    actionable: Boolean(qualification.actionable),
    isTestData: treatAsTest,
    customerVisible: treatAsTest ? false : true,
  };
}

/** Fields to merge onto GDI Airtable opportunity row for PE linkage. */
export function peLinkAirtableFields(opp) {
  return {
    [MAP_GDI_PE_LINK.peVenueId]: opp.peVenueId || null,
    [MAP_GDI_PE_LINK.peSignalId]: opp.peSignalId || null,
    [MAP_GDI_PE_LINK.hotelVenueFitId]: opp.hotelVenueFitId || null,
    [MAP_GDI_PE_LINK.demandFamily]: opp.demandFamily || DEMAND_FAMILY.PRIVATE_EVENTS,
    [MAP_GDI_PE_LINK.demandSignalType]: opp.demandSignalType || null,
  };
}
