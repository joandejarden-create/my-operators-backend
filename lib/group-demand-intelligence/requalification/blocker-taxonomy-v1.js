/**
 * GDI blocker taxonomy for requalification / HI recovery diagnostics.
 */

export const GDI_BLOCKER = Object.freeze({
  HOTEL_CAPABILITY_UNKNOWN: "HOTEL_CAPABILITY_UNKNOWN",
  HOTEL_CAPABILITY_TOO_LOW: "HOTEL_CAPABILITY_TOO_LOW",
  MEETING_SPACE_UNKNOWN: "MEETING_SPACE_UNKNOWN",
  MEETING_SPACE_TOO_LOW: "MEETING_SPACE_TOO_LOW",
  ROOM_COUNT_UNKNOWN: "ROOM_COUNT_UNKNOWN",
  ROOM_COUNT_TOO_LOW: "ROOM_COUNT_TOO_LOW",
  MARKET_MISMATCH: "MARKET_MISMATCH",
  EVENT_INVALID: "EVENT_INVALID",
  EVENT_CLOSED: "EVENT_CLOSED",
  CURRENT_CYCLE_CLOSED: "CURRENT_CYCLE_CLOSED",
  FULLY_PLACED: "FULLY_PLACED",
  NO_LODGING_SIGNAL: "NO_LODGING_SIGNAL",
  LODGING_STATUS_UNKNOWN: "LODGING_STATUS_UNKNOWN",
  ROOM_DEMAND_UNKNOWN: "ROOM_DEMAND_UNKNOWN",
  HOTEL_SELECTION_STATUS_UNKNOWN: "HOTEL_SELECTION_STATUS_UNKNOWN",
  OVERFLOW_UNKNOWN: "OVERFLOW_UNKNOWN",
  WHO_UNKNOWN: "WHO_UNKNOWN",
  WHO_WEAK: "WHO_WEAK",
  ACTION_PATH_UNKNOWN: "ACTION_PATH_UNKNOWN",
  SOURCE_WEAK: "SOURCE_WEAK",
  SUMMARY_THIN: "SUMMARY_THIN",
  FIT_TOO_LOW: "FIT_TOO_LOW",
  PRIORITY_LOW: "PRIORITY_LOW",
  FUTURE_CYCLE_UNCONFIRMED: "FUTURE_CYCLE_UNCONFIRMED",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  ENTITY_MISMATCH: "ENTITY_MISMATCH",
  OTHER: "OTHER",
  READY: "READY",
  NONE: "NONE",
});

const HARD_TERMINAL = new Set([
  GDI_BLOCKER.MARKET_MISMATCH,
  GDI_BLOCKER.EVENT_INVALID,
  GDI_BLOCKER.EVENT_CLOSED,
  GDI_BLOCKER.CURRENT_CYCLE_CLOSED,
  GDI_BLOCKER.FULLY_PLACED,
  GDI_BLOCKER.ENTITY_MISMATCH,
]);

export function isHardTerminalBlocker(blocker) {
  return HARD_TERMINAL.has(blocker);
}

/**
 * Classify dominant blocker from opportunity + hotel profile + readiness.
 */
export function classifyDominantBlocker({
  opp = {},
  hotelProfile = null,
  readiness = null,
  fit = null,
} = {}) {
  const state = String(opp.customerFacingState || "").toUpperCase();
  const priority = String(opp.priority || "").toUpperCase();
  const venue = String(opp.venueStatus || "").toUpperCase();
  const blob = [
    opp.hotelOpportunityThesis,
    opp.summaryWhyHotel,
    opp.summaryWhyMatters,
    opp.whyNow,
    opp.recommendedAction,
    opp.notes,
  ]
    .map((x) => String(x || ""))
    .join(" ");

  if (priority === "DISQUALIFIED" || state === "CLOSED") {
    if (/fully.?placed|no third-party|single-hotel arrangement/i.test(blob) || venue === "FULLY_PLACED") {
      return { blocker: GDI_BLOCKER.FULLY_PLACED, confidence: "HIGH" };
    }
    if (/cancel|cancelled|postponed indefinitely/i.test(blob)) {
      return { blocker: GDI_BLOCKER.EVENT_CLOSED, confidence: "HIGH" };
    }
    return { blocker: GDI_BLOCKER.EVENT_CLOSED, confidence: "MEDIUM" };
  }

  if (venue === "FULLY_PLACED" && /no third-party|single-hotel/i.test(blob)) {
    return { blocker: GDI_BLOCKER.FULLY_PLACED, confidence: "HIGH" };
  }

  if (/wrong.?market|not in (nyc|new york|santo domingo)|outside territory/i.test(blob)) {
    return { blocker: GDI_BLOCKER.MARKET_MISMATCH, confidence: "HIGH" };
  }

  if (fit?.finalState === "NOT_APPLICABLE") {
    return { blocker: GDI_BLOCKER.MARKET_MISMATCH, confidence: "HIGH" };
  }
  if (fit?.finalState === "CLOSED") {
    return { blocker: GDI_BLOCKER.EVENT_CLOSED, confidence: "HIGH" };
  }
  if (fit?.finalState === "NOT_FIT" && fit?.productConstraint === "LODGING_COMMERCIALLY_CLOSED") {
    return { blocker: GDI_BLOCKER.FULLY_PLACED, confidence: "HIGH" };
  }

  const eventSem = hotelProfile?.eventSpaceCapability;
  const needsMeeting = /\b(meeting|conference|symposium|convention|ballroom|plenary)\b/i.test(
    `${opp.title || ""} ${opp.summaryWhat || ""} ${opp.opportunityType || ""}`
  );
  const isOverflow = /overflow|housing|room.?block/i.test(
    `${opp.title || ""} ${opp.opportunityType || ""} ${JSON.stringify(opp.lodgingEvidence || {})}`
  );

  if (
    needsMeeting &&
    !isOverflow &&
    eventSem?.meetingCapabilityState === "UNKNOWN_UNRESEARCHED"
  ) {
    return { blocker: GDI_BLOCKER.MEETING_SPACE_UNKNOWN, confidence: "HIGH", hiRelated: true };
  }
  if (
    needsMeeting &&
    !isOverflow &&
    (hotelProfile?.meetingSqFt == null || hotelProfile?.meetingSqFt === 0) &&
    eventSem?.meetingCapabilityState !== "KNOWN_POPULATED"
  ) {
    return {
      blocker:
        eventSem?.meetingCapabilityState === "KNOWN_EMPTY"
          ? GDI_BLOCKER.MEETING_SPACE_TOO_LOW
          : GDI_BLOCKER.MEETING_SPACE_UNKNOWN,
      confidence: "MEDIUM",
      hiRelated: true,
    };
  }
  if (
    needsMeeting &&
    !isOverflow &&
    hotelProfile?.meetingSqFt != null &&
    hotelProfile.meetingSqFt > 0 &&
    hotelProfile.meetingSqFt < 800 &&
    fit?.productConstraint === "LIMITED_MEETING_SPACE"
  ) {
    return { blocker: GDI_BLOCKER.MEETING_SPACE_TOO_LOW, confidence: "HIGH" };
  }

  if (hotelProfile?.rooms == null && (opp.estimatedPeakRooms != null || opp.peakRooms != null)) {
    return { blocker: GDI_BLOCKER.ROOM_COUNT_UNKNOWN, confidence: "MEDIUM", hiRelated: true };
  }
  if (fit?.productConstraint === "ROOM_SCALE_OVER") {
    return { blocker: GDI_BLOCKER.ROOM_COUNT_TOO_LOW, confidence: "HIGH" };
  }

  if ((fit?.missingData || []).includes("room_demand_peak")) {
    return { blocker: GDI_BLOCKER.ROOM_DEMAND_UNKNOWN, confidence: "MEDIUM" };
  }
  if ((fit?.missingData || []).includes("lodging_evidence")) {
    return { blocker: GDI_BLOCKER.NO_LODGING_SIGNAL, confidence: "MEDIUM" };
  }

  const failed = readiness?.failed || [];
  if (failed.includes("summary_quality")) {
    return { blocker: GDI_BLOCKER.SUMMARY_THIN, confidence: "HIGH" };
  }
  if (failed.includes("who_research_not_attempted")) {
    return { blocker: GDI_BLOCKER.WHO_UNKNOWN, confidence: "HIGH" };
  }
  if (failed.includes("source")) {
    return { blocker: GDI_BLOCKER.SOURCE_WEAK, confidence: "HIGH" };
  }
  if (failed.includes("hotel_fit") || failed.includes("why_now") || failed.includes("recommended_action")) {
    return { blocker: GDI_BLOCKER.ACTION_PATH_UNKNOWN, confidence: "MEDIUM" };
  }

  if (fit?.hotelFitScore != null && fit.hotelFitScore < 45) {
    return { blocker: GDI_BLOCKER.FIT_TOO_LOW, confidence: "HIGH" };
  }

  if (state === "FUTURE_WATCH") {
    return { blocker: GDI_BLOCKER.FUTURE_CYCLE_UNCONFIRMED, confidence: "MEDIUM" };
  }

  if (readiness?.ok) {
    return { blocker: GDI_BLOCKER.READY, confidence: "HIGH" };
  }

  if (state === "WATCH" || state === "INTERNAL_ONLY" || state === "UNKNOWN" || !state) {
    if (opp.hotelFitScore == null && hotelProfile?.meetingSqFt == null) {
      return { blocker: GDI_BLOCKER.HOTEL_CAPABILITY_UNKNOWN, confidence: "MEDIUM", hiRelated: true };
    }
    return { blocker: GDI_BLOCKER.OTHER, confidence: "LOW" };
  }

  return { blocker: GDI_BLOCKER.OTHER, confidence: "LOW" };
}

export function wasHeldDueToIncompleteHi(classification = {}) {
  return (
    classification.hiRelated === true ||
    [
      GDI_BLOCKER.HOTEL_CAPABILITY_UNKNOWN,
      GDI_BLOCKER.MEETING_SPACE_UNKNOWN,
      GDI_BLOCKER.ROOM_COUNT_UNKNOWN,
    ].includes(classification.blocker)
  );
}
