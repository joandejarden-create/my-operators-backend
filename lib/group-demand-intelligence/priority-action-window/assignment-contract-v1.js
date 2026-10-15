/**
 * GDI Priority + Action Window assignment contract v1.
 *
 * Separates four concerns that were previously collapsed by import defaults:
 * 1) Prospect value / hotel fit → priority (HIGH / MEDIUM / WATCHLIST)
 * 2) Recommended action + timing → bookingWindowStatus (Pursue Now / Contact / Watch / Too Early)
 * 3) Commercial packaging → prospectReviewSection (PRIORITY | EXPLORATORY) — NOT the Priority filter
 * 4) Customer-recorded actions/outcomes → actionStatus / validation / decision (untouched)
 *
 * Does not invent urgency. Unknown sourcing alone does not force Watch.
 * Missing usable contact → enrichment disposition, not a timing Watch disguise.
 */

import {
  BOOKING_WINDOW,
  PRIORITY,
  OPPORTUNITY_TYPE,
  VENUE_SOURCING_STATUS,
  OPPORTUNITY_QUALIFICATION,
} from "../claim-types.js";
import { hasUsableContactRoute } from "../prospect-publication-eligibility-v1.js";

export const PRIORITY_ACTION_ASSIGNMENT_VERSION =
  "gdi_priority_action_assignment_v1";

export const PROSPECT_COMMERCIAL_CLASS = Object.freeze({
  PRIORITY: "PRIORITY",
  EXPLORATORY: "EXPLORATORY",
});

/** Customer-safe labels that do not collide with Priority High/Medium/Watch. */
export const PROSPECT_COMMERCIAL_CLASS_LABEL = Object.freeze({
  PRIORITY: "Core prospect",
  EXPLORATORY: "Exploratory account",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function upper(v) {
  return text(v).toUpperCase();
}

export function isVenuePartnershipType(opp = {}) {
  const t = upper(opp.opportunityType);
  return (
    t === "VENUE_PARTNERSHIP" ||
    t === "VENUE_REFERRAL_PARTNERSHIP" ||
    upper(opp.demandFamily) === "PRIVATE_EVENTS" ||
    Boolean(opp.peVenueId)
  );
}

export function hasCredibleHotelFitThesis(opp = {}) {
  if (num(opp.hotelFitScore) != null && num(opp.hotelFitScore) >= 50) return true;
  const thesis = text(
    opp.hotelOpportunityThesis ||
      opp.summaryWhyHotel ||
      opp.fitExplanation ||
      opp.hotelFitRationale ||
      opp.hotelRelevanceResearch?.fitBasis ||
      opp.hotelRelevanceResearch?.whyFits
  );
  if (thesis.length >= 40) return true;
  const disposition = upper(
    opp.hotelRelevanceDisposition || opp.hotelRelevanceResearch?.disposition
  );
  return disposition === "READY_FOR_CARD" || disposition === "READY";
}

export function hasStrongHotelFit(opp = {}) {
  const fit = num(opp.hotelFitScore);
  if (fit != null && fit >= 75) return true;
  if (fit != null && fit >= 72) {
    const cap = upper(opp.captureCapacityState);
    if (cap === "IDEAL_CAPTURE_RANGE" || cap === "PLAUSIBLE_CAPTURE") return true;
  }
  return false;
}

function confirmedOpenBuying(opp = {}) {
  if (opp.confirmedBuying === true || opp.openBuyingConfirmed === true) return true;
  if (opp.openBuyingInference === true) return true;
  if (opp.openBuyingInference === false) {
    // Explicit research negation — do not treat UNKNOWN sourcing as open
  }
  const sourcing = upper(opp.venueSourcingStatus || opp.venueStatus);
  if (sourcing === VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING) return true;
  return (
    sourcing === "OPEN_UNRESOLVED" &&
    upper(opp.opportunityQualification) === OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN &&
    upper(opp.opportunityType) === OPPORTUNITY_TYPE.PRIMARY_PURSUIT
  );
}

function placedNoOverflow(opp = {}) {
  const sourcing = upper(opp.venueSourcingStatus || opp.venueStatus);
  return (
    sourcing === VENUE_SOURCING_STATUS.FULLY_PLACED ||
    sourcing === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE ||
    sourcing === "PRIMARY_SELECTED_NO_OVERFLOW"
  );
}

function placedWithPossibleOverflow(opp = {}) {
  const sourcing = upper(opp.venueSourcingStatus || opp.venueStatus);
  return (
    sourcing === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE ||
    sourcing === "PRIMARY_SELECTED_OVERFLOW_POSSIBLE" ||
    sourcing === VENUE_SOURCING_STATUS.PARTIALLY_PLACED ||
    upper(opp.opportunityType) === OPPORTUNITY_TYPE.OVERFLOW_HOUSING
  );
}

function isTooEarlyOccasion(opp = {}) {
  if (upper(opp.bookingWindowStatus) === BOOKING_WINDOW.TOO_EARLY) {
    // Only trust existing TOO_EARLY when research stamped a reason, else reassess
    if (text(opp.bookingWindowReason || opp.timingReason)) return true;
  }
  if (upper(opp.opportunityType) === OPPORTUNITY_TYPE.FUTURE_CYCLE) {
    const planning = upper(opp.planningWindowStatus || opp.bookingCycleStatus);
    if (planning === "NOT_OPEN" || planning === "TOO_EARLY" || planning === "UNKNOWN") {
      // Future cycle with unknown planning → Too Early only when year is far and no contact-now evidence
      const y = Number(String(opp.eventStartDate || opp.eventDateDisplay || "").slice(0, 4));
      const nowY = new Date().getUTCFullYear();
      if (Number.isFinite(y) && y >= nowY + 2) return true;
    }
  }
  return false;
}

function stayToPlayOnly(opp = {}) {
  const blob = `${opp.roomDemandStatus || ""} ${opp.housingStatus || ""} ${opp.summaryWhat || ""} ${opp.whyNow || ""}`;
  return (
    /stay[\s-]?to[\s-]?play|housing\s*provider|onpeak|passkey/i.test(blob) &&
    !confirmedOpenBuying(opp) &&
    !placedWithPossibleOverflow(opp)
  );
}

/**
 * Assign priority (prospect value / hotel fit band).
 */
export function assignPriorityV1(opp = {}, opts = {}) {
  const route = opts.route || hasUsableContactRoute(opp);
  const reasons = [];

  if (upper(opp.priority) === PRIORITY.DISQUALIFIED || opp.isTestData === true) {
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason: "Disqualified or test data — priority unchanged",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (placedNoOverflow(opp) && !isVenuePartnershipType(opp)) {
    if (upper(opp.opportunityType) === OPPORTUNITY_TYPE.FUTURE_CYCLE) {
      return {
        priority: PRIORITY.WATCHLIST,
        reason: "Primary venue placed — future-cycle watch only",
        ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
      };
    }
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason: "Primary venue placed with no overflow evidence",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  const credible = hasCredibleHotelFitThesis(opp);
  const strong = hasStrongHotelFit(opp);
  const conf = num(opp.evidenceConfidence);
  const qual = upper(opp.opportunityQualification || opp.qualification);

  if (
    strong &&
    conf != null &&
    conf >= 55 &&
    route.ok &&
    confirmedOpenBuying(opp) &&
    (upper(opp.opportunityType) === OPPORTUNITY_TYPE.PRIMARY_PURSUIT ||
      upper(opp.opportunityType) === OPPORTUNITY_TYPE.OVERFLOW_HOUSING) &&
    !isTooEarlyOccasion(opp)
  ) {
    return {
      priority: PRIORITY.HIGH,
      reason:
        "Strong hotel fit + evidence + usable contact + confirmed open buying / active sourcing",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (credible && !isTooEarlyOccasion(opp)) {
    reasons.push("credible_hotel_fit_thesis");
    if (route.ok) reasons.push("usable_contact_route");
    if (qual === "STRONG" || qual === "VERIFIED_OPEN" || qual === "MODERATE") {
      reasons.push(`qualification_${qual.toLowerCase()}`);
    }
    // Reviewed PRIORITY section is packaging, not a High upgrade
    if (upper(opp.prospectReviewSection) === "PRIORITY") {
      reasons.push("core_prospect_packaging");
    }
    return {
      priority: PRIORITY.MEDIUM,
      reason: `Credible hotel-fit path (${reasons.join(", ")}); High reserved for confirmed open buying + strong scored fit`,
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (isTooEarlyOccasion(opp) && credible) {
    return {
      priority: PRIORITY.WATCHLIST,
      reason: "Credible future occasion — planning window too early for pursuit priority",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (!route.ok && credible) {
    return {
      priority: PRIORITY.MEDIUM,
      reason:
        "Credible hotel fit but no usable contact route — priority retains Medium; contact gap is enrichment not Watch timing",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
      enrichmentNeeded: true,
    };
  }

  return {
    priority: PRIORITY.WATCHLIST,
    reason: credible
      ? "Retained on watch — insufficient fit/timing evidence for Medium"
      : "No credible hotel-fit thesis or score — Watch until fit research completes",
    ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
  };
}

/**
 * Assign action window (recommended timing / next commercial motion).
 */
export function assignActionWindowV1(opp = {}, opts = {}) {
  const route = opts.route || hasUsableContactRoute(opp);
  const priorityResult = opts.priorityResult || assignPriorityV1(opp, { route });

  if (priorityResult.priority === PRIORITY.DISQUALIFIED) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      reason: "Disqualified — no pursue/contact action window",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (!route.ok) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      reason:
        "No usable contact route — internal enrichment required (not a timing decision)",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
      enrichmentNeeded: true,
      enrichmentTask:
        "Establish source-supported usable contact (email, phone, events form, or intermediary)",
    };
  }

  if (isTooEarlyOccasion(opp) && !confirmedOpenBuying(opp)) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.TOO_EARLY,
      reason: "Occasion planning window not open — Watch–Too Early",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (isVenuePartnershipType(opp)) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      reason:
        "Ongoing venue partnership / lodging referral — contact for relationship development (not a dated pursue)",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (confirmedOpenBuying(opp) && !stayToPlayOnly(opp)) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      reason: "Confirmed open buying or active RFP / open sourcing — Pursue Now",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (placedWithPossibleOverflow(opp) && hasCredibleHotelFitThesis(opp)) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      reason:
        "Host/primary may be placed but overflow/housing unconfirmed — Contact to investigate, not Pursue Now",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (stayToPlayOnly(opp)) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      reason:
        "Stay-to-play / housing provider alone is not confirmed overflow — Contact to qualify, not Pursue Now",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  if (hasCredibleHotelFitThesis(opp)) {
    const sourcing = upper(opp.venueSourcingStatus || opp.venueStatus);
    return {
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      reason:
        sourcing === "UNKNOWN" || !sourcing
          ? "Usable contact + credible hotel fit; sourcing unconfirmed — Contact to investigate (unknown sourcing ≠ Watch)"
          : "Usable contact + credible hotel fit — Contact / qualify now",
      ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    };
  }

  return {
    bookingWindowStatus: BOOKING_WINDOW.WATCH,
    reason: "Insufficient hotel-fit evidence for Contact; monitor while fit research continues",
    ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
  };
}

/**
 * Full assignment for one opportunity. Never mutates customer action/outcome fields.
 */
export function assignPriorityAndActionWindowV1(opp = {}, opts = {}) {
  const route = hasUsableContactRoute(opp);
  const priorityResult = assignPriorityV1(opp, { ...opts, route });
  const actionResult = assignActionWindowV1(opp, {
    ...opts,
    route,
    priorityResult,
  });
  const section = upper(opp.prospectReviewSection);
  const commercialClass =
    section === PROSPECT_COMMERCIAL_CLASS.EXPLORATORY
      ? PROSPECT_COMMERCIAL_CLASS.EXPLORATORY
      : section === PROSPECT_COMMERCIAL_CLASS.PRIORITY
        ? PROSPECT_COMMERCIAL_CLASS.PRIORITY
        : null;

  const before = {
    priority: opp.priority || null,
    bookingWindowStatus: opp.bookingWindowStatus || null,
  };
  const after = {
    priority: priorityResult.priority,
    bookingWindowStatus: actionResult.bookingWindowStatus,
  };
  const changed =
    before.priority !== after.priority ||
    before.bookingWindowStatus !== after.bookingWindowStatus;

  return {
    ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    assessedAt: new Date().toISOString(),
    before,
    after,
    changed,
    priorityReason: priorityResult.reason,
    bookingWindowReason: actionResult.reason,
    enrichmentNeeded: Boolean(
      priorityResult.enrichmentNeeded || actionResult.enrichmentNeeded
    ),
    enrichmentTask: actionResult.enrichmentTask || null,
    usableContactRoute: route,
    commercialClass,
    commercialClassLabel: commercialClass
      ? PROSPECT_COMMERCIAL_CLASS_LABEL[commercialClass]
      : null,
    note:
      "prospectReviewSection (Core prospect / Exploratory) is packaging — independent of Priority High/Medium/Watch",
  };
}

/**
 * Apply assignment onto a copy of the opportunity (idempotent fields).
 * Preserves decisionId, validation, actionStatus, outcomes.
 */
export function applyPriorityActionAssignment(opp = {}, opts = {}) {
  const assignment = assignPriorityAndActionWindowV1(opp, opts);
  const force = opts.force === true;
  const onlyIfMissing = opts.onlyIfMissing === true;

  let nextPriority = opp.priority;
  let nextBw = opp.bookingWindowStatus;

  if (force) {
    nextPriority = assignment.after.priority;
    nextBw = assignment.after.bookingWindowStatus;
  } else if (onlyIfMissing) {
    if (!opp.priority) nextPriority = assignment.after.priority;
    if (!opp.bookingWindowStatus) nextBw = assignment.after.bookingWindowStatus;
  } else {
    // Overwrite import-default Watch stamps when evidence supports better.
    // Never demote evidence-backed HIGH / CONTACT_NOW / QUALIFY_NOW / TOO_EARLY
    // unless force=true.
    const priIsDefaultWatch =
      !opp.priority ||
      (opp.priority === PRIORITY.WATCHLIST &&
        !text(opp.priorityReason) &&
        !opp.scoreAudit?.priority);
    const bwIsDefaultWatch =
      !opp.bookingWindowStatus ||
      (opp.bookingWindowStatus === BOOKING_WINDOW.WATCH &&
        !text(opp.bookingWindowReason));

    if (priIsDefaultWatch) {
      nextPriority = assignment.after.priority;
    }
    if (bwIsDefaultWatch) {
      nextBw = assignment.after.bookingWindowStatus;
    }
  }

  return {
    opportunity: {
      ...opp,
      priority: nextPriority,
      bookingWindowStatus: nextBw,
      priorityReason: assignment.priorityReason,
      bookingWindowReason: assignment.bookingWindowReason,
      priorityActionAssignmentVersion: assignment.ruleVersion,
      priorityActionAssessedAt: assignment.assessedAt,
      prospectCommercialClassLabel: assignment.commercialClassLabel,
    },
    assignment,
    applied: {
      priority: nextPriority !== opp.priority,
      bookingWindowStatus: nextBw !== opp.bookingWindowStatus,
    },
  };
}
