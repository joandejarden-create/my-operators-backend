/**
 * Shared market opportunity packet + hotel-specific layer (shadow model).
 * Does not mutate Airtable. Separates hotel-neutral evidence from hotel fit/thesis.
 */

import { computeMarketOpportunityId } from "./market-geography-v1.js";
import { classifyOpportunityGeography } from "./market-geography-v1.js";

/**
 * Extract hotel-neutral market packet from a hotel opportunity row.
 */
export function extractMarketOpportunityPacket(opp = {}, sourceHotelId = null) {
  const eventName =
    opp.eventName ||
    String(opp.title || "")
      .replace(/\s*[—\-]\s*(EXHIBITOR|VENDOR|SPONSOR|OVERFLOW|VIP).*$/i, "")
      .trim();
  const location =
    opp.destinationStatus ||
    opp.eventLocation ||
    opp.location ||
    opp.venueStatus ||
    null;
  const marketOpportunityId = computeMarketOpportunityId({
    organizationName: opp.organizationName || opp.company,
    eventName,
    eventStartDate: opp.eventStartDate,
    eventYear: opp.eventYear,
    venueOrLocation: location,
  });
  const geo = classifyOpportunityGeography(opp);

  return {
    marketOpportunityId,
    eventSeriesId: opp.eventSeriesId || null,
    eventCycleId: opp.eventCycleId || null,
    subEventId: opp.subEventId || null,
    title: opp.title || opp.opportunityName || null,
    organizationName: opp.organizationName || opp.company || null,
    eventName,
    eventStartDate: opp.eventStartDate || null,
    eventEndDate: opp.eventEndDate || null,
    eventYear: opp.eventYear || null,
    venueStatus: opp.venueStatus || null,
    destinationStatus: opp.destinationStatus || null,
    location,
    geography: geo,
    officialSource: opp.officialSource || null,
    discoverySource: opp.discoverySource || null,
    sources: Array.isArray(opp.sources) ? opp.sources : [],
    lodgingEvidence: opp.lodgingEvidence || opp.lodging || null,
    commercialStatus: opp.priority || opp.actionStatus || null,
    customerFacingState: opp.customerFacingState || null,
    who: {
      primaryContact: opp.primaryContact || null,
      contactPathClass: opp.contactPathClass || null,
      contactResearchState: opp.contactResearchState || null,
    },
    roomDemand: {
      estimatedPeakRooms: opp.estimatedPeakRooms ?? opp.peakRooms ?? null,
      estimatedAttendance: opp.estimatedAttendance ?? opp.attendance ?? null,
      roomDemandStatus: opp.roomDemandStatus || null,
    },
    summaryWhat: opp.summaryWhat || null,
    summaryQuality: opp.summaryQuality || null,
    sourceHotelOpportunityId: opp.id || opp.opportunityId || null,
    sourceHotelId,
    // Explicitly excluded hotel-specific fields (kept only as provenance)
    hotelSpecificExcluded: {
      hotelFitScore: opp.hotelFitScore,
      summaryWhyHotel: opp.summaryWhyHotel,
      whyNow: opp.whyNow,
      recommendedAction: opp.recommendedAction || opp.recommendedNextStep,
      priority: opp.priority,
    },
  };
}

/**
 * Hotel-specific evaluation layer (shadow — not written to customer store).
 */
export function buildHotelOpportunityLayer({
  marketPacket,
  hotelProfile,
  geographicApplicability,
  hotelFit,
  finalState,
  missingData = [],
  jevNextAction = null,
  seedHotelOpportunityId = null,
} = {}) {
  return {
    marketOpportunityId: marketPacket?.marketOpportunityId || null,
    hotelId: hotelProfile?.hotelId || null,
    hotelName: hotelProfile?.displayName || null,
    seedHotelOpportunityId,
    geographicApplicability: geographicApplicability?.applicability || null,
    geoReasons: geographicApplicability?.reasons || [],
    territoryClass: geographicApplicability?.territoryClass || null,
    hotelFitScore: hotelFit?.hotelFitScore ?? null,
    hotelFitComponents: hotelFit?.components || null,
    fitReasons: hotelFit?.reasons || [],
    missingData,
    jevNextAction,
    finalState,
    developmentState: mapFinalToDevelopment(finalState),
  };
}

export const HOTEL_OPP_FINAL_STATE = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  HOTEL_MATCHED_NEEDS_MORE_DATA: "HOTEL_MATCHED_NEEDS_MORE_DATA",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  NOT_FIT: "NOT_FIT",
  CLOSED: "CLOSED",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
});

export const DEVELOPMENT_STATE = Object.freeze({
  DISCOVERED: "DISCOVERED",
  VALIDATED: "VALIDATED",
  LODGING_SUPPORTED: "LODGING_SUPPORTED",
  HOTEL_MATCHED: "HOTEL_MATCHED",
  WHO_RESOLVED: "WHO_RESOLVED",
  ACTIONABLE: "ACTIONABLE",
  CUSTOMER_READY: "CUSTOMER_READY",
});

function mapFinalToDevelopment(finalState) {
  switch (finalState) {
    case HOTEL_OPP_FINAL_STATE.CUSTOMER_READY:
      return DEVELOPMENT_STATE.CUSTOMER_READY;
    case HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA:
      return DEVELOPMENT_STATE.HOTEL_MATCHED;
    case HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE:
    case HOTEL_OPP_FINAL_STATE.NOT_FIT:
    case HOTEL_OPP_FINAL_STATE.CLOSED:
      return DEVELOPMENT_STATE.VALIDATED;
    default:
      return DEVELOPMENT_STATE.DISCOVERED;
  }
}
