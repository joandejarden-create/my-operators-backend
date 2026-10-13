/**
 * Structured lodging capture assessment + modeled room demand.
 * Never invent confirmed attendance or falsely precise room counts.
 */

import {
  ATTENDANCE_STATUS,
  BUYER_PATH,
  NONLOCALITY,
  ON_SITE_LODGING_STATUS,
  ROOM_DEMAND_CLAIM,
  SENSITIVE_PERSONAL_RE,
} from "./constants.js";

function clean(s) {
  return String(s || "").trim();
}

function blobOf(obj = {}) {
  return [
    obj.eventName,
    obj.eventType,
    obj.publicEventEvidence,
    obj.lodgingMentioned,
    obj.roomBlockMentioned,
    obj.hotelMentioned,
    obj.transportationMentioned,
    obj.sourceSnippet,
    ...(obj.evidenceTexts || []),
  ]
    .filter(Boolean)
    .join(" \n ");
}

export function classifyAttendance(signal = {}) {
  const status = clean(signal.attendanceStatus).toUpperCase();
  if (Object.values(ATTENDANCE_STATUS).includes(status)) {
    return {
      status,
      estimatedAttendance:
        signal.estimatedAttendance != null
          ? Number(signal.estimatedAttendance)
          : null,
    };
  }
  if (
    signal.estimatedAttendance != null &&
    Number.isFinite(Number(signal.estimatedAttendance))
  ) {
    return {
      status: ATTENDANCE_STATUS.ESTIMATED,
      estimatedAttendance: Number(signal.estimatedAttendance),
    };
  }
  return { status: ATTENDANCE_STATUS.UNKNOWN, estimatedAttendance: null };
}

export function classifyNonlocality(signal = {}) {
  const blob = blobOf(signal).toLowerCase();
  if (
    /destination\s+wedding|out[- ]?of[- ]?town|traveling\s+guests?|guest\s+travel|airport\s+(?:shuttle|transfer|info|guidance)|hotel\s+block|room\s+block|accommodations?\s+for\s+guests/.test(
      blob
    )
  ) {
    if (/destination\s+wedding|out[- ]?of[- ]?town\s+guests?/.test(blob)) {
      return NONLOCALITY.STRONG;
    }
    return NONLOCALITY.MODERATE;
  }
  if (/local\s+(?:guests?|wedding|celebration)|community\s+only/.test(blob)) {
    return NONLOCALITY.LOCAL_LIKELY;
  }
  return NONLOCALITY.UNKNOWN;
}

export function classifyBuyerPath(signalOrVenue = {}) {
  const explicit = clean(signalOrVenue.buyerPath || signalOrVenue.buyerAccessibility)
    .toUpperCase();
  if (Object.values(BUYER_PATH).includes(explicit)) return explicit;

  if (clean(signalOrVenue.plannerCompany) || clean(signalOrVenue.plannerName)) {
    if (clean(signalOrVenue.plannerCompany)) return BUYER_PATH.NAMED_PLANNER;
    return BUYER_PATH.NAMED_PLANNER;
  }
  if (
    /event\s+director|director\s+of\s+events|catering\s+director/i.test(
      clean(signalOrVenue.eventContactRole)
    )
  ) {
    return BUYER_PATH.VENUE_EVENT_DIRECTOR;
  }
  if (
    /sales\s+(?:manager|director|coordinator)|venue\s+sales/i.test(
      clean(signalOrVenue.eventContactRole)
    )
  ) {
    return BUYER_PATH.VENUE_SALES;
  }
  if (
    /event\s+team|events?\s+(?:manager|coordinator|office)/i.test(
      clean(signalOrVenue.eventContactRole)
    )
  ) {
    return BUYER_PATH.EVENT_TEAM;
  }
  if (clean(signalOrVenue.eventContact) || clean(signalOrVenue.eventContactName)) {
    return BUYER_PATH.FUNCTIONAL_EVENT_CONTACT;
  }
  if (clean(signalOrVenue.eventContactEmail) || clean(signalOrVenue.eventContactPhone)) {
    return BUYER_PATH.FUNCTIONAL_EVENT_CONTACT;
  }
  if (clean(signalOrVenue.eventCompany)) return BUYER_PATH.EVENT_COMPANY;
  if (clean(signalOrVenue.organizationPath)) return BUYER_PATH.ORGANIZATION_PATH;
  // Public inquiry / booking path on official site = organization path
  if (
    signalOrVenue.hasPublicInquiryForm === true ||
    signalOrVenue.publicBookingLanguage === true ||
    /ORGANIZATION_PATH/i.test(clean(signalOrVenue.commercialContactPath))
  ) {
    return BUYER_PATH.ORGANIZATION_PATH;
  }
  return BUYER_PATH.NO_BUYER_PATH;
}

/**
 * Reject private-person profiling sources.
 */
export function isPrivatePersonSourceRejected(signal = {}) {
  const blob = blobOf(signal);
  if (SENSITIVE_PERSONAL_RE.test(blob)) return true;
  if (signal.sourceType === "PRIVATE_PERSONAL" || signal.privatePersonOnly) {
    return true;
  }
  // Couple names without business entity
  if (
    signal.coupleNamesOnly &&
    !signal.plannerCompany &&
    !signal.eventCompany &&
    !signal.venueId
  ) {
    return true;
  }
  const name = String(signal.eventName || "");
  const url = String(signal.sourceUrl || "");
  // Public couple wedding pages (directories) — not commercial buyer path
  if (
    /\band\b.+\b('s|’s)?\s*wedding\b/i.test(name) ||
    /theknot\.com\/us\/[^/]+-and-[^/]+-\d{4}/i.test(url) ||
    /weddingwire\.com\/.+\/real-weddings/i.test(url)
  ) {
    return true;
  }
  return false;
}

/**
 * Model potential rooms as a RANGE with MODELED status, or UNKNOWN.
 * Never return a single "needs N rooms" claim as confirmed.
 */
export function modelRoomDemand({
  signal = {},
  venue = {},
  hotelCtx = {},
  relationship = {},
} = {}) {
  const attendance = classifyAttendance(signal);
  const nonlocality = classifyNonlocality(signal);
  const lodgingGap =
    venue.onSiteLodgingStatus || ON_SITE_LODGING_STATUS.UNKNOWN;

  const factors = {
    eventSize: attendance.status,
    nonlocality,
    venueLodgingGap: lodgingGap,
    hotelProximity: relationship.lodgingCatchmentFit || "UNKNOWN",
    hotelProductFit: relationship.productFit || "UNKNOWN",
    timing: signal.eventDate ? "HAS_DATE" : "UNKNOWN",
    buyerAccessibility: classifyBuyerPath({ ...venue, ...signal }),
    competitiveCapture: venue.exclusiveHotelRelationship
      ? "EXCLUSIVE_PARTNER_BLOCKS"
      : venue.preferredHotelListed
        ? "PREFERRED_LISTED"
        : "OPEN",
  };

  // Weak inputs → UNKNOWN
  const hasSize =
    attendance.status !== ATTENDANCE_STATUS.UNKNOWN &&
    Number.isFinite(attendance.estimatedAttendance) &&
    attendance.estimatedAttendance > 0;
  const capacityFallback =
    !hasSize &&
    Number.isFinite(Number(venue.maxCapacity)) &&
    Number(venue.maxCapacity) > 0;

  if (
    lodgingGap === ON_SITE_LODGING_STATUS.ADEQUATE_LODGING &&
    Number(venue.onSiteGuestrooms || 0) >= 100
  ) {
    return {
      potentialRoomsLow: null,
      potentialRoomsHigh: null,
      roomDemandStatus: ROOM_DEMAND_CLAIM.UNKNOWN,
      factors,
      reason: "Venue has substantial on-site lodging; external capture weak",
      falsePrecision: false,
    };
  }

  if (nonlocality === NONLOCALITY.LOCAL_LIKELY && !signal.roomBlockMentioned) {
    return {
      potentialRoomsLow: null,
      potentialRoomsHigh: null,
      roomDemandStatus: ROOM_DEMAND_CLAIM.UNKNOWN,
      factors,
      reason: "Local-likely event without lodging signal",
      falsePrecision: false,
    };
  }

  if (!hasSize && !capacityFallback) {
    return {
      potentialRoomsLow: null,
      potentialRoomsHigh: null,
      roomDemandStatus: ROOM_DEMAND_CLAIM.UNKNOWN,
      factors,
      reason: "Insufficient size evidence to model rooms",
      falsePrecision: false,
    };
  }

  if (
    nonlocality === NONLOCALITY.UNKNOWN &&
    !signal.roomBlockMentioned &&
    lodgingGap !== ON_SITE_LODGING_STATUS.NO_LODGING
  ) {
    return {
      potentialRoomsLow: null,
      potentialRoomsHigh: null,
      roomDemandStatus: ROOM_DEMAND_CLAIM.UNKNOWN,
      factors,
      reason: "Nonlocality and lodging gap too weak to model",
      falsePrecision: false,
    };
  }

  const headcount = hasSize
    ? attendance.estimatedAttendance
    : Number(venue.maxCapacity) * 0.55;

  // Guests → rooms: conservative range; destination higher share
  let lowShare = 0.15;
  let highShare = 0.28;
  if (nonlocality === NONLOCALITY.STRONG) {
    lowShare = 0.28;
    highShare = 0.45;
  } else if (nonlocality === NONLOCALITY.MODERATE) {
    lowShare = 0.2;
    highShare = 0.35;
  }
  if (lodgingGap === ON_SITE_LODGING_STATUS.NO_LODGING) {
    lowShare += 0.05;
    highShare += 0.08;
  } else if (lodgingGap === ON_SITE_LODGING_STATUS.LIMITED_LODGING) {
    lowShare += 0.02;
    highShare += 0.04;
  }

  let low = Math.max(5, Math.round((headcount * lowShare) / 2));
  let high = Math.max(low + 5, Math.round((headcount * highShare) / 2));

  // Cap vs hotel inventory when known
  const hotelRooms = Number(hotelCtx.roomCount || 0);
  if (hotelRooms > 0) {
    high = Math.min(high, Math.round(hotelRooms * 0.45));
    low = Math.min(low, high);
  }

  return {
    potentialRoomsLow: low,
    potentialRoomsHigh: high,
    roomDemandStatus: ROOM_DEMAND_CLAIM.MODELED,
    factors,
    reason: "Modeled from attendance/capacity + nonlocality + lodging gap",
    falsePrecision: false,
    displayLabel: `${low}–${high} rooms MODELED`,
  };
}

export function assessLodgingCapture(input = {}) {
  const modeled = modelRoomDemand(input);
  return {
    ...modeled,
    attendance: classifyAttendance(input.signal || {}),
    nonlocality: classifyNonlocality(input.signal || {}),
    buyerPath: classifyBuyerPath({
      ...(input.venue || {}),
      ...(input.signal || {}),
    }),
  };
}
