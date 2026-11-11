/**
 * Room-night-optional commercial eligibility V1
 *
 * Qualifying hotel demand requires:
 * 1) Verified activity / programme evidence
 * 2) At least one credible hotel revenue mechanism (not necessarily guestrooms)
 * 3) Hotel facility fit for that mechanism
 * 4) At least one verified named commercially relevant contact
 * 5) Concrete sales approach
 *
 * Does NOT rewrite existing Scaling/Events outcomes.
 * Does NOT invent volumes, budgets, or committed bookings.
 */

import {
  GUESTROOM_DEMAND_STATUS,
  HOTEL_REVENUE_TYPE,
  buildRevenueMechanismStamp,
  normalizeRevenueTypeTags,
} from "./hotel-revenue-mechanism-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const ROOM_NIGHT_OPTIONAL_ELIGIBILITY_V1 =
  "gdi-room-night-optional-eligibility-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function hasNamedRelevantContact(input = {}) {
  const candidates = [
    input.primaryContact,
    input.namedContact,
    input.contact,
    ...(Array.isArray(input.contacts) ? input.contacts : []),
  ].filter(Boolean);
  for (const c of candidates) {
    const name = text(typeof c === "string" ? c : c.name || c.displayName);
    if (!name || !looksLikePersonName(name)) continue;
    const relevant =
      c.buyerEligible === true ||
      c.commerciallyRelevant === true ||
      c.customerFacingNamed === true ||
      /decision|organizer|planner|coordinator|manager|referral|influencer/i.test(
        text(c.role || c.title || c.whyRelevant || ""),
      ) ||
      input.requireNamedContactOnly === true;
    if (relevant || looksLikePersonName(name)) {
      // Named + any role/title OR explicit relevance flag
      if (text(c.title || c.role || c.professionalTitle) || relevant) return true;
      if (input.allowNamedWithoutRole === true) return true;
    }
  }
  // Flat fields
  if (looksLikePersonName(input.contactName) && text(input.contactTitle)) {
    return true;
  }
  return false;
}

function hasFacilityFit(input = {}) {
  if (input.hotelFacilityFit === false) return false;
  if (input.hotelFacilityFit === true) return true;
  const types = normalizeRevenueTypeTags(input.revenueTypes || []);
  const facilities = input.hotelFacilities || input.authoritativeHotelContext || {};
  const hasMeeting =
    facilities.hasMeetingSpace === true ||
    facilities.meetingRooms > 0 ||
    /meeting|salon|ballroom|banquet/i.test(text(facilities.meetingSummary));
  const hasCatering =
    facilities.hasCatering === true ||
    facilities.hasBanquet === true ||
    facilities.hasPrivateDining === true ||
    /catering|banquet|F&B|restaurant/i.test(text(facilities.fbSummary));
  const hasRooms =
    Number(facilities.guestrooms || facilities.rooms || 0) > 0 ||
    facilities.hasGuestrooms === true;

  if (!types.length) return input.assumeFacilityFit !== false;
  for (const t of types) {
    if (
      (t === HOTEL_REVENUE_TYPE.MEETING_SPACE_RENTAL ||
        t === HOTEL_REVENUE_TYPE.LOCAL_CATERING ||
        t === HOTEL_REVENUE_TYPE.BANQUETS ||
        t === HOTEL_REVENUE_TYPE.PRIVATE_DINING ||
        t === HOTEL_REVENUE_TYPE.AV_EVENT_ANCILLARY) &&
      (hasMeeting || hasCatering || input.assumeFacilityFit !== false)
    ) {
      return true;
    }
    if (
      (t === HOTEL_REVENUE_TYPE.GUESTROOMS ||
        t === HOTEL_REVENUE_TYPE.GROUP_GUESTROOMS ||
        t === HOTEL_REVENUE_TYPE.EXTENDED_STAY ||
        t === HOTEL_REVENUE_TYPE.RESORT_ALL_INCLUSIVE_GROUP) &&
      (hasRooms || input.assumeFacilityFit !== false)
    ) {
      return true;
    }
  }
  return input.assumeFacilityFit !== false;
}

/**
 * Evaluate commercial eligibility with room-nights optional.
 */
export function evaluateRoomNightOptionalEligibility(input = {}) {
  const revenue = buildRevenueMechanismStamp(input);
  const activityVerified =
    input.activityVerified === true ||
    input.evidenceSupported === true ||
    Boolean(text(input.verifiedActivitySummary)) ||
    (Array.isArray(input.sources) && input.sources.length > 0);

  const hasRevenueMechanism = revenue.revenueTypes.length > 0;
  const facilityFit = hasFacilityFit({ ...input, revenueTypes: revenue.revenueTypes });
  const namedContact = hasNamedRelevantContact(input);
  const salesApproach = Boolean(
    text(
      input.recommendedNextStep ||
        input.salesApproach ||
        input.commercialIntelligenceBrief?.recommendedSalesApproach?.nextAction,
    ),
  );

  const reasons = [];
  if (!activityVerified) reasons.push("activity_not_verified");
  if (!hasRevenueMechanism) reasons.push("no_credible_revenue_mechanism");
  if (!facilityFit) reasons.push("hotel_facility_fit_missing");
  if (!namedContact) reasons.push("named_commercially_relevant_contact_missing");
  if (!salesApproach) reasons.push("sales_approach_missing");

  // Critical rule: ZERO guestrooms does NOT fail when other revenue exists
  const zeroRoomsOk =
    revenue.guestroomDemandStatus === GUESTROOM_DEMAND_STATUS.NONE_CONFIRMED &&
    hasRevenueMechanism &&
    revenue.revenueTypes.some((t) =>
      [
        HOTEL_REVENUE_TYPE.LOCAL_CATERING,
        HOTEL_REVENUE_TYPE.MEETING_SPACE_RENTAL,
        HOTEL_REVENUE_TYPE.BANQUETS,
        HOTEL_REVENUE_TYPE.PRIVATE_DINING,
        HOTEL_REVENUE_TYPE.AV_EVENT_ANCILLARY,
      ].includes(t),
    );

  const customerReady =
    activityVerified &&
    hasRevenueMechanism &&
    facilityFit &&
    namedContact &&
    salesApproach;

  return {
    version: ROOM_NIGHT_OPTIONAL_ELIGIBILITY_V1,
    customerReady,
    disposition: customerReady
      ? "Ready — Pursue Now"
      : reasons.includes("named_commercially_relevant_contact_missing") ||
          reasons.includes("activity_not_verified")
        ? "Research Continuation"
        : "Watch",
    revenue,
    zeroGuestroomEligible: zeroRoomsOk,
    guestroomNotRequired: true,
    reasons,
    doesNotRewriteScalingOrEventsOutcomes: true,
  };
}
