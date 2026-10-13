/**
 * Explicit venue priority classification (not opaque score-only).
 * V1.4: repeated activity via eventActivityEvidenceStatus — annual count optional.
 */

import {
  VENUE_PRIORITY,
  ON_SITE_LODGING_STATUS,
  EVENT_ACTIVITY_EVIDENCE_STATUS,
  COMMERCIAL_BUYER_PATHS,
} from "./constants.js";
import {
  classifyEventActivityEvidence,
  activitySupportsTruePartnership,
  classifyPartnerStatus,
} from "./activity-evidence.js";
import { classifyBuyerPath } from "./lodging-capture.js";

/**
 * @returns {{ priority, factors: string[], reasons: string[], activity?, partnerStatus?, buyerPath? }}
 */
export function classifyVenuePriority(venue, relationship = {}) {
  const factors = [];
  const reasons = [];

  const miles = relationship.distanceMiles;
  const catchment = relationship.lodgingCatchmentFit;
  const lodging = venue.onSiteLodgingStatus;
  const annual = Number(venue.estimatedAnnualPrivateEvents || 0);
  const exclusive = Boolean(venue.exclusiveHotelRelationship);
  const capacity = Number(venue.maxCapacity || 0);

  const activity =
    venue.eventActivityEvidenceStatus && venue._activityClassified
      ? {
          eventActivityEvidenceStatus: venue.eventActivityEvidenceStatus,
          decisionReason: venue.activityDecisionReason || null,
          activityEvidence: venue.activityEvidence || [],
        }
      : classifyEventActivityEvidence(venue, venue.activityEvidence || []);

  const partner = classifyPartnerStatus(venue);
  const buyerPath = classifyBuyerPath(venue);
  const hasCommercialPath = COMMERCIAL_BUYER_PATHS.includes(buyerPath);

  if (exclusive || partner.partnerStatus === "EXCLUSIVE_PARTNER_FOUND") {
    factors.push("EXCLUSIVE_HOTEL_RELATIONSHIP");
    reasons.push("Exclusive hotel partner already listed");
    return {
      priority: VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  if (catchment === "OUTSIDE") {
    factors.push("OUTSIDE_CATCHMENT");
    reasons.push("Outside practical lodging catchment");
    return {
      priority: VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  if (
    lodging === ON_SITE_LODGING_STATUS.ADEQUATE_LODGING &&
    Number(venue.onSiteGuestrooms || 0) >= 100
  ) {
    factors.push("SUBSTANTIAL_ON_SITE_ROOMS");
    reasons.push("Venue has substantial on-site guestrooms");
    return {
      priority: VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  const lodgingGap =
    lodging === ON_SITE_LODGING_STATUS.NO_LODGING ||
    lodging === ON_SITE_LODGING_STATUS.LIMITED_LODGING;

  const activityStatus =
    activity.eventActivityEvidenceStatus || EVENT_ACTIVITY_EVIDENCE_STATUS.UNKNOWN;
  const strongActivity = activitySupportsTruePartnership(activityStatus);

  if (lodgingGap) factors.push("LODGING_GAP");
  if (catchment === "CORE") factors.push("CORE_CATCHMENT");
  if (catchment === "COMPETITIVE") factors.push("COMPETITIVE_CATCHMENT");
  if (catchment === "STRETCH") factors.push("STRETCH_CATCHMENT");
  if (activityStatus === EVENT_ACTIVITY_EVIDENCE_STATUS.CONFIRMED_VOLUME) {
    factors.push("CONFIRMED_VOLUME");
  } else if (activityStatus === EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY) {
    factors.push("STRONG_REPEATED_ACTIVITY");
  } else if (activityStatus === EVENT_ACTIVITY_EVIDENCE_STATUS.MODERATE_ACTIVITY) {
    factors.push("MODERATE_ACTIVITY");
  }
  if (annual >= 20) factors.push("HIGH_EVENT_FREQUENCY");
  else if (annual >= 8) factors.push("MODERATE_EVENT_FREQUENCY");
  if (capacity >= 150) factors.push("MEANINGFUL_CAPACITY");
  if (
    venue.weddingsAdvertised ||
    venue.privateEventsAdvertised ||
    strongActivity ||
    activityStatus === EVENT_ACTIVITY_EVIDENCE_STATUS.MODERATE_ACTIVITY
  ) {
    factors.push("PRIVATE_EVENT_FOCUS");
  }
  if (hasCommercialPath) factors.push("CONTACT_PATH");
  if (relationship.productFit === "STRONG") factors.push("ARCHETYPE_VENUE_FIT");
  if (relationship.productFit === "MODERATE") factors.push("MODERATE_PRODUCT_FIT");
  if (
    partner.partnerStatus === "NO_PUBLIC_PARTNER_FOUND" ||
    partner.partnerStatus === "UNKNOWN"
  ) {
    factors.push("NO_PUBLIC_PARTNER_FOUND");
  }

  const geoOk = catchment === "CORE" || catchment === "COMPETITIVE";
  const productOk =
    relationship.productFit === "STRONG" || relationship.productFit === "MODERATE";

  // HIGH: strong/confirmed activity + lodging gap + practical geography + product fit
  // Exact annual count is NOT required when STRONG_REPEATED_ACTIVITY is established.
  if (
    strongActivity &&
    lodgingGap &&
    geoOk &&
    productOk &&
    (capacity >= 100 ||
      capacity === 0 ||
      !Number.isFinite(capacity) ||
      capacity == null)
  ) {
    reasons.push(
      activity.decisionReason ||
        "Repeated private-event activity evidenced, lodging gap, practical geography"
    );
    return {
      priority: VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  if (catchment !== "OUTSIDE" && lodgingGap && factors.includes("PRIVATE_EVENT_FOCUS")) {
    reasons.push("Viable venue for forward event-signal monitoring");
    return {
      priority: VENUE_PRIORITY.EVENT_SIGNAL_TARGET,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  if (catchment === "STRETCH" && lodgingGap && strongActivity) {
    reasons.push("Stretch geography — watch despite strong activity");
    return {
      priority: VENUE_PRIORITY.WATCH,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  if (
    catchment !== "OUTSIDE" &&
    (factors.includes("PRIVATE_EVENT_FOCUS") || capacity > 0 || lodgingGap)
  ) {
    reasons.push("Incomplete evidence; watch for refresh");
    return {
      priority: VENUE_PRIORITY.WATCH,
      factors,
      reasons,
      activity,
      partnerStatus: partner.partnerStatus,
      buyerPath,
    };
  }

  reasons.push("Weak hotel opportunity signals");
  return {
    priority: VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY,
    factors,
    reasons,
    activity,
    partnerStatus: partner.partnerStatus,
    buyerPath,
  };
}
