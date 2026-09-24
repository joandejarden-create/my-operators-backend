/**
 * TRUE_ACTIONABLE gates for specific private events + venue partnerships.
 * V1.4: venue partnership uses multi-signal activity evidence (annual count optional).
 */

import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { DEMAND_SIGNAL_TYPE, DEMAND_FAMILY } from "../demand-signal-types.js";
import {
  BUYER_PATH,
  COMMERCIAL_BUYER_PATHS,
  ON_SITE_LODGING_STATUS,
  PARTNER_STATUS,
  PARTNERSHIP_CONFIDENCE,
  ROOM_DEMAND_CLAIM,
  VENUE_PRIORITY,
  EVENT_ACTIVITY_EVIDENCE_STATUS,
} from "./constants.js";
import {
  assessLodgingCapture,
  classifyBuyerPath,
  isPrivatePersonSourceRejected,
} from "./lodging-capture.js";
import { classifyVenuePriority } from "./venue-priority.js";
import {
  activitySupportsTruePartnership,
  classifyEventActivityEvidence,
  classifyPartnerStatus,
} from "./activity-evidence.js";

/** Commercial gate labels (aligned with GDI New Opp TRUE/WATCH/FALSE). */
const GATE = Object.freeze({
  TRUE: "TRUE",
  WATCH: "WATCH",
  FALSE: "FALSE",
});

const LIFECYCLE = Object.freeze({
  NEW: "NEW",
  WATCH: "WATCH",
  FUTURE_WATCH: "FUTURE_WATCH",
  ACTIONABLE_NOW: "ACTIONABLE_NOW",
  CLOSED: "CLOSED",
});

function clean(s) {
  return String(s || "").trim();
}

function parseDate(s) {
  if (!s) return null;
  const d = new Date(String(s));
  return Number.isNaN(d.getTime()) ? null : d;
}

function isFutureEvent(signal, asOf = new Date()) {
  const d = parseDate(signal.eventDate);
  if (!d) return false;
  const day = new Date(asOf);
  day.setHours(0, 0, 0, 0);
  return d >= day;
}

/**
 * Qualify a specific upcoming private event against a hotel.
 */
export function qualifySpecificEvent({
  signal,
  venue,
  hotelCtx,
  relationship,
  asOf = new Date(),
} = {}) {
  const reject = [];
  const downgrade = [];

  if (isPrivatePersonSourceRejected(signal)) {
    reject.push("PRIVATE_PERSON_SOURCE");
  }
  if (!signal?.eventType && !signal?.eventName) {
    reject.push("NO_EVENT_EVIDENCE");
  }
  if (!isFutureEvent(signal, asOf)) {
    reject.push("PAST_OR_UNDATED_EVENT");
  }
  if (!venue?.venueId && !clean(signal?.venueName) && !clean(signal?.location)) {
    reject.push("NO_VENUE_OR_LOCATION");
  }
  if (!clean(signal?.sourceUrl) && !(signal?.sourceUrls || []).length) {
    reject.push("NO_MEANINGFUL_SOURCE");
  }
  if (relationship?.lodgingCatchmentFit === "OUTSIDE") {
    reject.push("OUTSIDE_CATCHMENT");
  }
  if (
    venue?.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.ADEQUATE_LODGING &&
    Number(venue.onSiteGuestrooms || 0) >= 100
  ) {
    reject.push("VENUE_HAS_SUBSTANTIAL_ON_SITE_ROOMS");
  }
  if (venue?.exclusiveHotelRelationship) {
    reject.push("EXCLUSIVE_HOTEL_PARTNER");
  }

  const capture = assessLodgingCapture({
    signal,
    venue,
    hotelCtx,
    relationship,
  });

  if (capture.roomDemandStatus === ROOM_DEMAND_CLAIM.UNKNOWN) {
    if (
      capture.nonlocality === "LOCAL_LIKELY" ||
      capture.reason?.includes("Local-likely")
    ) {
      reject.push("LOCAL_ONLY_NO_LODGING_THESIS");
    } else {
      downgrade.push("WEAK_LODGING_THESIS");
    }
  }

  if (capture.buyerPath === BUYER_PATH.NO_BUYER_PATH) {
    downgrade.push("NO_BUYER_PATH");
  }

  if (reject.length) {
    return {
      opportunityType: OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT,
      demandFamily: DEMAND_FAMILY.PRIVATE_EVENTS,
      demandSignalType: mapEventSignalType(signal),
      qualification: GATE.FALSE,
      lifecycle: LIFECYCLE.CLOSED,
      customerFacingState: LIFECYCLE.CLOSED,
      rejectReasons: reject,
      downgradeReasons: downgrade,
      lodgingCapture: capture,
      actionable: false,
    };
  }

  const actionable =
    capture.roomDemandStatus !== ROOM_DEMAND_CLAIM.UNKNOWN &&
    capture.buyerPath !== BUYER_PATH.NO_BUYER_PATH &&
    relationship?.lodgingCatchmentFit !== "OUTSIDE" &&
    downgrade.length === 0;

  const watch =
    !actionable &&
    capture.roomDemandStatus !== ROOM_DEMAND_CLAIM.UNKNOWN &&
    relationship?.lodgingCatchmentFit !== "OUTSIDE";

  return {
    opportunityType: OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT,
    demandFamily: DEMAND_FAMILY.PRIVATE_EVENTS,
    demandSignalType: mapEventSignalType(signal),
    qualification: actionable ? GATE.TRUE : GATE.WATCH,
    lifecycle: actionable
      ? LIFECYCLE.ACTIONABLE_NOW
      : watch
        ? LIFECYCLE.WATCH
        : LIFECYCLE.FUTURE_WATCH,
    customerFacingState: actionable
      ? LIFECYCLE.ACTIONABLE_NOW
      : watch
        ? LIFECYCLE.WATCH
        : LIFECYCLE.FUTURE_WATCH,
    rejectReasons: reject,
    downgradeReasons: downgrade,
    lodgingCapture: capture,
    actionable: Boolean(actionable),
    whyHotelCouldWin: buildSpecificWhy({ venue, relationship, capture, hotelCtx }),
    recommendedAction: actionable
      ? "Contact venue event director / planner about preferred lodging + room block"
      : "Watch for lodging language and buyer path; refresh before pursuing",
  };
}

function partnershipConfidence({
  activityStatus,
  lodgingGap,
  geoOk,
  productOk,
  hasCommercialPath,
  partnerStatus,
  officialEvidence,
}) {
  const uncertainties = [];
  if (activityStatus !== EVENT_ACTIVITY_EVIDENCE_STATUS.CONFIRMED_VOLUME) {
    if (activityStatus !== EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY) {
      uncertainties.push("WEAK_ACTIVITY");
    } else {
      uncertainties.push("ACTIVITY_NOT_CONFIRMED_VOLUME");
    }
  }
  if (!lodgingGap) uncertainties.push("LODGING");
  if (!geoOk) uncertainties.push("GEOGRAPHY");
  if (!productOk) uncertainties.push("PRODUCT_FIT");
  if (!hasCommercialPath) uncertainties.push("COMMERCIAL_PATH");
  if (
    partnerStatus === PARTNER_STATUS.UNKNOWN ||
    partnerStatus === PARTNER_STATUS.PREFERRED_PARTNER_FOUND
  ) {
    uncertainties.push("PARTNER_STATUS");
  }
  if (!officialEvidence) uncertainties.push("SOURCE_QUALITY");

  if (
    activitySupportsTruePartnership(activityStatus) &&
    lodgingGap &&
    geoOk &&
    productOk &&
    hasCommercialPath &&
    officialEvidence &&
    partnerStatus !== PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND &&
    uncertainties.filter((u) => u !== "ACTIVITY_NOT_CONFIRMED_VOLUME").length === 0
  ) {
    return PARTNERSHIP_CONFIDENCE.HIGH;
  }
  if (
    activitySupportsTruePartnership(activityStatus) &&
    lodgingGap &&
    geoOk &&
    productOk &&
    partnerStatus !== PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND &&
    uncertainties.length <= 2
  ) {
    return PARTNERSHIP_CONFIDENCE.MODERATE;
  }
  return PARTNERSHIP_CONFIDENCE.LOW;
}

/**
 * Qualify a venue partnership opportunity (no single event required).
 */
export function qualifyVenuePartnership({
  venue,
  hotelCtx,
  relationship,
} = {}) {
  const reject = [];
  const downgrade = [];

  const activity = classifyEventActivityEvidence(
    venue,
    venue.activityEvidence || []
  );
  const partner = classifyPartnerStatus(venue);
  const buyerPath = classifyBuyerPath({
    ...venue,
    hasPublicInquiryForm: venue.hasPublicInquiryForm,
    publicBookingLanguage: venue.publicBookingLanguage,
    commercialContactPath: venue.commercialContactPath,
  });

  const venueForPriority = {
    ...venue,
    eventActivityEvidenceStatus: activity.eventActivityEvidenceStatus,
    activityEvidence: activity.activityEvidence,
    activityDecisionReason: activity.decisionReason,
    partnerStatus: partner.partnerStatus,
    _activityClassified: true,
  };
  const priorityResult = classifyVenuePriority(venueForPriority, relationship);

  if (!venue?.venueId || !clean(venue.venueName)) {
    reject.push("NO_REAL_VENUE");
  }
  if (!(venue.sourceUrls || []).length && !venue.website) {
    reject.push("NO_MEANINGFUL_SOURCE");
  }
  if (relationship?.lodgingCatchmentFit === "OUTSIDE") {
    reject.push("OUTSIDE_CATCHMENT");
  }
  if (
    venue.exclusiveHotelRelationship ||
    partner.partnerStatus === PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND
  ) {
    reject.push("EXCLUSIVE_HOTEL_PARTNER");
  }
  if (
    venue.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.ADEQUATE_LODGING &&
    Number(venue.onSiteGuestrooms || 0) >= 100
  ) {
    reject.push("VENUE_HAS_SUBSTANTIAL_ON_SITE_ROOMS");
  }

  const lodgingGap =
    venue.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.NO_LODGING ||
    venue.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.LIMITED_LODGING;

  if (venue.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.UNKNOWN) {
    reject.push("LODGING_STATUS_UNKNOWN");
  } else if (!lodgingGap) {
    reject.push("NO_CREDIBLE_LODGING_GAP");
  }

  const activityOk = activitySupportsTruePartnership(
    activity.eventActivityEvidenceStatus
  );
  if (!activityOk) {
    reject.push("NO_REPEATED_PRIVATE_EVENT_ACTIVITY");
  }

  const geoOk =
    relationship?.lodgingCatchmentFit === "CORE" ||
    relationship?.lodgingCatchmentFit === "COMPETITIVE";
  if (relationship?.lodgingCatchmentFit === "STRETCH") {
    downgrade.push("STRETCH_GEOGRAPHY");
  }

  const productOk =
    relationship?.productFit === "STRONG" ||
    relationship?.productFit === "MODERATE";
  if (!productOk) {
    reject.push("WEAK_PRODUCT_FIT");
  }

  const hasCommercialPath = COMMERCIAL_BUYER_PATHS.includes(buyerPath);
  if (!hasCommercialPath) {
    downgrade.push("NO_BUYER_PATH");
  }

  const officialEvidence =
    (activity.officialCount || 0) > 0 ||
    Boolean(venue.website) ||
    Boolean(venue.officialDomain) ||
    (Array.isArray(venue.sourceUrls) && venue.sourceUrls.length > 0) ||
    (venue.sourceAuthority || "").toUpperCase().includes("OFFICIAL");

  if (!officialEvidence && !(venue.sourceUrls || []).length) {
    reject.push("NO_CREDIBLE_SOURCE");
  }

  const confidence = partnershipConfidence({
    activityStatus: activity.eventActivityEvidenceStatus,
    lodgingGap,
    geoOk,
    productOk,
    hasCommercialPath,
    partnerStatus: partner.partnerStatus,
    officialEvidence,
  });

  const thesis = buildPartnershipThesis({
    venue,
    hotelCtx,
    relationship,
    activity,
    partner,
    buyerPath,
  });

  if (reject.length) {
    return {
      opportunityType: OPPORTUNITY_TYPE.VENUE_PARTNERSHIP,
      demandFamily: DEMAND_FAMILY.PRIVATE_EVENTS,
      demandSignalType: DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP,
      qualification: GATE.FALSE,
      lifecycle: LIFECYCLE.CLOSED,
      customerFacingState: LIFECYCLE.CLOSED,
      venuePriority: priorityResult.priority,
      priorityFactors: priorityResult.factors,
      rejectReasons: reject,
      downgradeReasons: downgrade,
      buyerPath,
      partnerStatus: partner.partnerStatus,
      eventActivityEvidenceStatus: activity.eventActivityEvidenceStatus,
      activityEvidence: activity.activityEvidence,
      activityDecisionReason: activity.decisionReason,
      partnershipConfidence: confidence,
      actionable: false,
      partnershipThesis: thesis,
    };
  }

  // TRUE gate — do not promote on MODERATE_ACTIVITY or LOW confidence
  const actionable =
    activityOk &&
    lodgingGap &&
    geoOk &&
    productOk &&
    hasCommercialPath &&
    partner.partnerStatus !== PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND &&
    officialEvidence &&
    confidence !== PARTNERSHIP_CONFIDENCE.LOW &&
    priorityResult.priority === VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER &&
    !downgrade.includes("STRETCH_GEOGRAPHY");

  const watch =
    !actionable &&
    lodgingGap &&
    relationship?.lodgingCatchmentFit !== "OUTSIDE" &&
    (activityOk ||
      activity.eventActivityEvidenceStatus ===
        EVENT_ACTIVITY_EVIDENCE_STATUS.MODERATE_ACTIVITY);

  return {
    opportunityType: OPPORTUNITY_TYPE.VENUE_PARTNERSHIP,
    demandFamily: DEMAND_FAMILY.PRIVATE_EVENTS,
    demandSignalType: DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP,
    qualification: actionable ? GATE.TRUE : watch ? GATE.WATCH : GATE.FALSE,
    lifecycle: actionable
      ? LIFECYCLE.ACTIONABLE_NOW
      : watch
        ? LIFECYCLE.WATCH
        : LIFECYCLE.FUTURE_WATCH,
    customerFacingState: actionable
      ? LIFECYCLE.ACTIONABLE_NOW
      : watch
        ? LIFECYCLE.WATCH
        : LIFECYCLE.FUTURE_WATCH,
    venuePriority: priorityResult.priority,
    priorityFactors: priorityResult.factors,
    rejectReasons: reject,
    downgradeReasons: downgrade,
    buyerPath,
    partnerStatus: partner.partnerStatus,
    eventActivityEvidenceStatus: activity.eventActivityEvidenceStatus,
    activityEvidence: activity.activityEvidence,
    activityDecisionReason: activity.decisionReason,
    partnershipConfidence: confidence,
    actionable: Boolean(actionable),
    whyOpportunityExists: priorityResult.reasons.join("; "),
    partnershipThesis: thesis,
    recommendedAction: actionable
      ? "Propose preferred lodging / room-block partnership with venue events team"
      : hasCommercialPath
        ? "Monitor venue frequency and refresh lodging/partner evidence before outreach"
        : "Identify commercial contact path (events team / inquiry) before pursuing",
  };
}

function buildPartnershipThesis({
  venue,
  hotelCtx,
  relationship,
  activity,
  partner,
  buyerPath,
}) {
  return {
    whyVenueCreatesHotelDemand:
      activity.decisionReason ||
      "Venue markets private events that may generate overnight guest demand",
    whyHotelRelevant: [
      relationship?.distanceMiles != null
        ? `${relationship.distanceMiles} mi (${relationship.lodgingCatchmentFit || "unknown"} catchment)`
        : null,
      relationship?.productFit
        ? `product fit ${relationship.productFit}`
        : null,
      hotelCtx?.archetype ? `archetype ${hotelCtx.archetype}` : null,
    ]
      .filter(Boolean)
      .join("; "),
    lodgingGap:
      venue.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.NO_LODGING
        ? "No on-site guestrooms"
        : venue.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.LIMITED_LODGING
          ? "Limited on-site lodging"
          : `Lodging status: ${venue.onSiteLodgingStatus || "UNKNOWN"}`,
    repeatedActivityEvidence: activity.decisionReason,
    partnerStatusKnown:
      partner.note ||
      "Bounded public research; absence of listing ≠ confirmed no relationship",
    whoToApproach:
      buyerPath === BUYER_PATH.NO_BUYER_PATH
        ? "Unknown — need events/sales contact or inquiry path"
        : buyerPath.replace(/_/g, " ").toLowerCase(),
    firstCommercialAction:
      "Introduce preferred lodging partnership and ask about guest room needs for upcoming private events",
    doNotEstimateAnnualRevenue: true,
  };
}

function mapEventSignalType(signal = {}) {
  const t = clean(signal.eventType || signal.demandSignalType)
    .toUpperCase()
    .replace(/[\s-]+/g, "_");
  if (DEMAND_SIGNAL_TYPE[t]) return DEMAND_SIGNAL_TYPE[t];
  if (/destination/.test(t)) return DEMAND_SIGNAL_TYPE.DESTINATION_WEDDING;
  if (/wedding/.test(t)) return DEMAND_SIGNAL_TYPE.WEDDING;
  if (/reunion/.test(t)) return DEMAND_SIGNAL_TYPE.FAMILY_REUNION;
  if (/mitzvah|quince|religious/.test(t)) {
    return DEMAND_SIGNAL_TYPE.RELIGIOUS_CELEBRATION;
  }
  if (/social|gala/.test(t)) return DEMAND_SIGNAL_TYPE.SOCIAL_EVENT;
  return DEMAND_SIGNAL_TYPE.PRIVATE_EVENT;
}

function buildSpecificWhy({ venue, relationship, capture, hotelCtx }) {
  const parts = [];
  if (relationship?.distanceMiles != null) {
    parts.push(`${relationship.distanceMiles} mi from hotel`);
  }
  if (venue?.onSiteLodgingStatus === ON_SITE_LODGING_STATUS.NO_LODGING) {
    parts.push("venue has no on-site guestrooms");
  }
  if (capture.displayLabel) parts.push(`potential ${capture.displayLabel}`);
  if (hotelCtx?.archetype) parts.push(`fits ${hotelCtx.archetype} catchment`);
  return parts.join("; ") || "Credible private-event lodging thesis in catchment";
}

export { LIFECYCLE };
