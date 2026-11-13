/**
 * Three independent commercial status dimensions for GDI opportunities.
 * Do not collapse competitor venue confirmation into target-hotel sourcing
 * or into Sales Ready / Account Development maturity.
 */

export const COMPETITOR_VENUE_STATUS = Object.freeze({
  CONFIRMED_CONTRACTED_BLOCK: "CONFIRMED_CONTRACTED_BLOCK",
  CONFIRMED_EVENT_VENUE: "CONFIRMED_EVENT_VENUE",
  VERIFIED_HISTORICAL_VENUE: "VERIFIED_HISTORICAL_VENUE",
  ANNOUNCED_VENUE: "ANNOUNCED_VENUE",
  NOT_VERIFIED: "NOT_VERIFIED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const COMPETITOR_VENUE_STATUS_LABEL = Object.freeze({
  [COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK]:
    "Confirmed contracted room block",
  [COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE]: "Confirmed event venue",
  [COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE]:
    "Verified historical venue",
  [COMPETITOR_VENUE_STATUS.ANNOUNCED_VENUE]: "Announced venue",
  [COMPETITOR_VENUE_STATUS.NOT_VERIFIED]: "Competitor venue not verified",
  [COMPETITOR_VENUE_STATUS.NOT_APPLICABLE]: "Competitor venue not applicable",
});

export const TARGET_HOTEL_SOURCING_STATUS = Object.freeze({
  OPEN_CONFIRMED: "OPEN_CONFIRMED",
  OVERFLOW_CONFIRMED: "OVERFLOW_CONFIRMED",
  SOURCING_CLOSED: "SOURCING_CLOSED",
  NOT_ESTABLISHED: "NOT_ESTABLISHED",
  FUTURE_CYCLE_TO_MONITOR: "FUTURE_CYCLE_TO_MONITOR",
  UNKNOWN: "UNKNOWN",
});

export const TARGET_HOTEL_SOURCING_STATUS_LABEL = Object.freeze({
  [TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED]:
    "Open sourcing confirmed for target hotel",
  [TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED]:
    "Overflow sourcing confirmed for target hotel",
  [TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED]:
    "Sourcing closed for this edition",
  [TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED]:
    "Target-hotel sourcing not established",
  [TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR]:
    "Future cycle to monitor",
  [TARGET_HOTEL_SOURCING_STATUS.UNKNOWN]: "Target-hotel sourcing unknown",
});

/** Maps onto existing GDI commercial maturity / population contracts. */
export const COMMERCIAL_ACTIONABILITY = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  QUALIFICATION_REQUIRED: "QUALIFICATION_REQUIRED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  COMPETITIVE_MARKET_INTELLIGENCE: "COMPETITIVE_MARKET_INTELLIGENCE",
});

export const COMMERCIAL_ACTIONABILITY_LABEL = Object.freeze({
  [COMMERCIAL_ACTIONABILITY.SALES_READY]: "Sales Ready",
  [COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT]: "Account Development",
  [COMMERCIAL_ACTIONABILITY.QUALIFICATION_REQUIRED]: "Qualification Required",
  [COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION]:
    "Research Continuation / Monitor",
  [COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE]:
    "Competitive Market Intelligence",
});

/**
 * Recommended next-step templates keyed by verified state (not generic RFP spam).
 */
export function recommendCommercialAction({
  competitorVenueStatus,
  targetHotelSourcingStatus,
  commercialActionability,
  competitorHotel = "",
  contactHint = "",
  targetHotelName = "",
} = {}) {
  const contact = String(contactHint || "").trim();
  const comp = String(competitorHotel || "").trim() || "the competitor hotel";
  const target =
    String(targetHotelName || "").trim() || "the target hotel";

  if (
    targetHotelSourcingStatus === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED
  ) {
    return contact
      ? `Review published requirements; contact ${contact}; request RFP or housing inclusion where eligibility is confirmed.`
      : "Review published requirements and contact the eligible buyer to request RFP or housing inclusion where eligibility is confirmed.";
  }
  if (
    targetHotelSourcingStatus === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED
  ) {
    return contact
      ? `Confirm overflow room need with ${contact} and propose ${target} as secondary lodging.`
      : `Confirm overflow room need with the housing decision-maker and propose ${target} as secondary lodging.`;
  }
  if (
    competitorVenueStatus ===
      COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE ||
    targetHotelSourcingStatus ===
      TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR
  ) {
    return `Re-verify the next edition housing page when available; do not treat the prior competitor booking at ${comp} as available business for ${target}.`;
  }
  if (
    competitorVenueStatus ===
      COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK ||
    competitorVenueStatus === COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE
  ) {
    if (
      targetHotelSourcingStatus ===
        TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
      targetHotelSourcingStatus === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN
    ) {
      return `Contact the relevant housing representative to establish whether overflow accommodation or a secondary room block is needed for ${target}. Competitor booking is confirmed at ${comp} — do not request an RFP as host.`;
    }
    if (
      targetHotelSourcingStatus ===
      TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED
    ) {
      return `Competitor holds this edition at ${comp}. Identify the future sourcing window; do not pursue as an open RFP.`;
    }
  }
  if (
    commercialActionability === COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT
  ) {
    return contact
      ? `Contact ${contact} with a hotel-specific thesis grounded in verified demand; do not claim open sourcing without evidence.`
      : "Identify the relevant buyer or activity-connected contact and build a realistic hotel-specific thesis; do not claim open sourcing without evidence.";
  }
  if (
    commercialActionability === COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION ||
    commercialActionability === COMMERCIAL_ACTIONABILITY.QUALIFICATION_REQUIRED
  ) {
    return "Verify demand, buyer authority, and hotel-specific fit before pursuit.";
  }
  return "Monitor for sourcing signals; do not claim open RFP or overflow without evidence.";
}

/**
 * Customer-facing hotel sourcing line — keeps competitor venue confirmation
 * independent from target-hotel overflow / open sourcing status.
 */
export function composeCustomerFacingHotelSourcingLabel(opp = {}) {
  const target = normalizeTargetHotelSourcingFromLegacy(opp);
  const competitorStatus = String(opp.competitorVenueStatus || "").trim();
  const comp = String(
    opp.cmiCompetitorHotel || opp.competitorHotel || opp.likelyCompetitor || ""
  ).trim();
  const competitorConfirmed =
    competitorStatus === COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK ||
    competitorStatus === COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE ||
    competitorStatus === COMPETITOR_VENUE_STATUS.ANNOUNCED_VENUE;
  const competitorHistorical =
    competitorStatus === COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE;

  if (target === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED) {
    return "Hotel Sourcing Open";
  }
  if (target === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED) {
    return "Overflow sourcing confirmed for this hotel";
  }
  if (target === TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED) {
    return "Sourcing closed for this edition";
  }
  if (
    competitorConfirmed &&
    (target === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
      target === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN)
  ) {
    return comp
      ? `Competitor venue confirmed (${comp}); target-hotel overflow sourcing unconfirmed`
      : "Competitor venue confirmed; target-hotel overflow sourcing unconfirmed";
  }
  if (
    competitorHistorical ||
    target === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR
  ) {
    return comp
      ? `Historical competitor venue (${comp}); future cycle to monitor`
      : "Historical competitor venue; future cycle to monitor";
  }
  if (target === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED) {
    return "Target-hotel sourcing not established";
  }
  return "Hotel Sourcing Unconfirmed";
}

export function normalizeTargetHotelSourcingFromLegacy(opp = {}) {
  const raw = String(
    opp.targetHotelSourcingStatus ||
      opp.hotelSourcingStatusNormalized ||
      opp.venueSourcingStatus ||
      opp.hotelSourcingStatus ||
      opp.sourcingStatus ||
      ""
  )
    .trim()
    .toUpperCase();
  if (/RFP_ACTIVE|OPEN_CONFIRMED|HOUSING_OPEN/.test(raw) && /OVERFLOW/.test(raw)) {
    return TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED;
  }
  if (/RFP_ACTIVE|OPEN_CONFIRMED/.test(raw)) {
    return TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED;
  }
  if (/OVERFLOW_CONFIRMED|OVERFLOW_VERIFIED/.test(raw)) {
    return TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED;
  }
  if (/FULLY_PLACED|SOURCING_CLOSED|NO_OVERFLOW|CLOSED/.test(raw)) {
    return TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED;
  }
  if (/FUTURE_CYCLE|NEXT_EDITION|MONITOR/.test(raw)) {
    return TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR;
  }
  if (
    /PRIMARY_VENUE_SELECTED|OVERFLOW_POSSIBLE|OPEN_UNRESOLVED|UNKNOWN|NOT_ESTABLISHED/.test(
      raw
    ) ||
    !raw
  ) {
    // PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE is a hypothesis, not confirmed open sourcing.
    if (/PRIMARY_VENUE_SELECTED/.test(raw)) {
      return TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED;
    }
    if (/OPEN_UNRESOLVED|UNKNOWN|^$/.test(raw)) {
      return TARGET_HOTEL_SOURCING_STATUS.UNKNOWN;
    }
    return TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED;
  }
  return TARGET_HOTEL_SOURCING_STATUS.UNKNOWN;
}
