/**
 * GDI customer classification presentation v1
 *
 * Aligns tile pills + browse filter keys. Does NOT invent commercial priority or
 * timing — presentation/filter normalization only.
 * Does NOT replace assignment-contract-v1 (authoritative assignment).
 *
 * Customer-facing three-pill tile contract (Universal Sales Action Pill V1):
 *   1) Priority (High / Medium / Watch)
 *   2) Recommended Sales Action — eight approved values only
 *   3) Dates / timing — verified upcoming demand/event date, else "Dates TBD"
 *
 * Commercial Stage remains an internal research/filter field — never the middle pill.
 * Events never render customer-facing Unclassified priority — missing priority → Watch.
 */

import {
  BOOKING_WINDOW,
  PRIORITY,
  canonicalizePriorityKey,
} from "./claim-types.js";
import { isAssociationConferenceEventsOpportunity } from "./events-opportunity-overview-v1.js";
import { planEventsCustomerTileBadges } from "./events-tile-commercial-pills-v1.js";
import { attachSalesOpportunityFilterFields } from "./sales-opportunity-quick-filters-v1.js";
import {
  attachCustomerSalesActionProjection,
  resolveCustomerSalesAction,
} from "./customer-sales-action-resolver-v1.js";
import { resolveCustomerCommercialStagePill } from "./commercial-stage-pill-v1.js";
import {
  applyUniversalTimingOverlay,
  resolveUniversalTileTiming,
  TIMING_TYPE,
} from "./universal-timing-intelligence-v1.js";

export const CUSTOMER_CLASSIFICATION_PRESENTATION_V1 =
  "gdi-customer-classification-presentation-v1";

/** Filter + pill labels — must match dealality-gdi-ui legend copy. */
export const PRIORITY_FILTER = Object.freeze({
  HIGH: PRIORITY.HIGH,
  MEDIUM: PRIORITY.MEDIUM,
  WATCH: PRIORITY.WATCHLIST,
  UNCLASSIFIED: "UNCLASSIFIED",
});

export const PRIORITY_FILTER_LABEL = Object.freeze({
  [PRIORITY.HIGH]: "High",
  [PRIORITY.MEDIUM]: "Medium",
  [PRIORITY.WATCHLIST]: "Watch",
  UNCLASSIFIED: "Unclassified",
  [PRIORITY.DISQUALIFIED]: "Disqualified",
});

export const ACTION_TIMING_FILTER = Object.freeze({
  PURSUE_NOW: BOOKING_WINDOW.CONTACT_NOW,
  FUTURE_WATCH: BOOKING_WINDOW.WATCH,
  TOO_EARLY: BOOKING_WINDOW.TOO_EARLY,
  UNCLASSIFIED: "UNCLASSIFIED",
});

export const ACTION_TIMING_FILTER_LABEL = Object.freeze({
  [BOOKING_WINDOW.CONTACT_NOW]: "Pursue Now",
  [BOOKING_WINDOW.WATCH]: "Future Watch",
  [BOOKING_WINDOW.TOO_EARLY]: "Too Early",
  UNCLASSIFIED: "Unclassified",
});

/** Non-canonical timing codes historically written onto CAR rows. */
const NON_CANONICAL_TIMING = new Set([
  "OPEN_ENDED",
  "FUTURE_WINDOW",
  "UNDETERMINED",
  "UNKNOWN",
  "N/A",
  "NA",
]);

function upper(v) {
  return String(v || "")
    .trim()
    .toUpperCase();
}

/**
 * Normalize stored priority for filter membership + tile pill.
 * Missing / invalid → UNCLASSIFIED (do not silently default to Watch).
 */
export function normalizePriorityForFilter(priority) {
  // Overlay aliases (HIGH/MEDIUM) must canonicalize — never silently become Watch.
  const canon = canonicalizePriorityKey(priority);
  if (canon) return canon;
  return PRIORITY_FILTER.UNCLASSIFIED;
}

/**
 * Normalize bookingWindowStatus for Action Timing filters + pills.
 * OPEN_ENDED / FUTURE_WINDOW / empty → UNCLASSIFIED (not a customer timing pill).
 * QUALIFY_NOW / RESEARCH_FURTHER remain non-timing sales postures → UNCLASSIFIED for
 * the Action Timing legend (Pursue Now / Future Watch / Too Early only).
 */
export function normalizeActionTimingForFilter(bookingWindowStatus) {
  const raw = upper(bookingWindowStatus);
  if (!raw || NON_CANONICAL_TIMING.has(raw)) {
    return ACTION_TIMING_FILTER.UNCLASSIFIED;
  }
  if (raw === BOOKING_WINDOW.CONTACT_NOW || raw === "PURSUE_NOW") {
    return BOOKING_WINDOW.CONTACT_NOW;
  }
  if (raw === BOOKING_WINDOW.WATCH || raw === "FUTURE_WATCH") {
    return BOOKING_WINDOW.WATCH;
  }
  if (raw === BOOKING_WINDOW.TOO_EARLY) {
    return BOOKING_WINDOW.TOO_EARLY;
  }
  if (raw === BOOKING_WINDOW.LIKELY_TOO_LATE) {
    // Filter preset is Too Early — map late windows into that timing bucket for browse.
    return BOOKING_WINDOW.TOO_EARLY;
  }
  // QUALIFY_NOW / RESEARCH_FURTHER / other legacy codes are not Action Timing.
  return ACTION_TIMING_FILTER.UNCLASSIFIED;
}

export function hasVerifiedEventDate(opportunity = {}) {
  const start = String(
    opportunity.eventStartDate ||
      opportunity.event_date_start ||
      opportunity.tileEventStartDate ||
      "",
  ).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(start)) return true;
  const timing = resolveUniversalTileTiming(opportunity);
  return timing.hasVerifiedUpcomingDate === true;
}

/** Customer-facing commercial stage labels (inventory lane — not Priority / Action Timing). */
export const COMMERCIAL_STAGE_PILL_LABEL = Object.freeze({
  SALES_OPPORTUNITY: "Sales Opportunity",
  ACCOUNT_DEVELOPMENT: "Account Development",
  FUTURE_OPPORTUNITY: "Future Opportunity",
  RESEARCH_WATCHLIST: "Exploratory",
  EXCLUDED: "Excluded",
});

/**
 * Resolve commercial opportunity stage for tile pill 2.
 * Uses authoritative inventoryLane when present; Exploratory from research watchlist /
 * prospect EXPLORATORY when no stronger lane. Does not invent Sales Opportunity.
 */
export function resolveCommercialStagePill(opportunity = {}) {
  const lane = String(opportunity.inventoryLane || "")
    .trim()
    .toUpperCase();
  if (lane && COMMERCIAL_STAGE_PILL_LABEL[lane]) {
    return {
      key: lane,
      label: COMMERCIAL_STAGE_PILL_LABEL[lane],
      show: lane !== "EXCLUDED",
    };
  }
  const section = String(opportunity.prospectReviewSection || "")
    .trim()
    .toUpperCase();
  if (section === "EXPLORATORY") {
    return {
      key: "RESEARCH_WATCHLIST",
      label: COMMERCIAL_STAGE_PILL_LABEL.RESEARCH_WATCHLIST,
      show: true,
    };
  }
  return { key: null, label: null, show: false };
}

/**
 * Customer tile badge plan — Universal Sales Action Pill V1:
 *   1) Priority  2) Recommended Sales Action (8 values)  3) Dates / timing
 * Commercial Stage stays internal (research / filters) — never middle pill.
 */
export function planCustomerTileBadges(opportunity = {}) {
  const timed = applyUniversalTimingOverlay(opportunity);
  const actionTimingFilterKey = normalizeActionTimingForFilter(
    timed.bookingWindowStatus,
  );

  const baseSuppressed = [
    "OPEN ENDED",
    "OPEN_ENDED",
    "Potential opportunity",
    "Core prospect",
    "Account Development",
    "Exploratory",
    "Sales Opportunity",
    "Future Opportunity",
    "Research Needed",
    "Pursue Now",
    "Future Capture",
    "Relationship Development",
    "Qualification Needed",
    "Too Early",
    "Needs qualification",
    "Unclassified",
  ];

  if (isAssociationConferenceEventsOpportunity(timed)) {
    const eventsPlan = planEventsCustomerTileBadges(timed);
    const forResolve = {
      ...timed,
      commercialStageKey: eventsPlan.commercialStageKey,
      commercialStagePillLabel: eventsPlan.commercialStagePillLabel,
      inventoryLane: eventsPlan.commercialStageKey,
      actionTimingFilterKey,
      tileEventStartDate: eventsPlan.tileEventStartDate || timed.tileEventStartDate || null,
      tileEventEndDate: eventsPlan.tileEventEndDate || timed.tileEventEndDate || null,
    };
    const stage = resolveCustomerCommercialStagePill(forResolve);
    const sales = resolveCustomerSalesAction(forResolve);
    return {
      version: CUSTOMER_CLASSIFICATION_PRESENTATION_V1,
      cardPillStandard: "GDI_UNIVERSAL_SALES_ACTION_PILL_V1",
      eventsPillsVersion: eventsPlan.version,
      priorityFilterKey: eventsPlan.priorityFilterKey,
      priorityPillLabel: eventsPlan.priorityPillLabel,
      showPriorityPill: true,
      showReadinessPill: false,
      commercialStageKey: stage.key,
      commercialStagePillLabel: stage.label,
      commercialStagePillClass: stage.pillClass,
      commercialStageSource: stage.source,
      commercialStageMissingMapped: stage.missingMapped === true,
      showCommercialStagePill: false,
      salesActionKey: sales.key,
      salesActionPillLabel: sales.label,
      salesActionReason: sales.reason,
      salesActionSourceFields: sales.sourceFields,
      showSalesActionPill: true,
      actionTimingFilterKey,
      actionTimingPillLabel: null,
      showActionTimingPill: false,
      showDatePill: true,
      datePillLabel: eventsPlan.datePillLabel,
      datePillDisplayLabel:
        eventsPlan.datePillDisplayLabel ||
        timed.datePillDisplayLabel ||
        "Dates TBD",
      hasVerifiedEventDate: eventsPlan.hasVerifiedEventDate,
      tileEventStartDate: eventsPlan.tileEventStartDate || null,
      tileEventEndDate: eventsPlan.tileEventEndDate || null,
      canonicalTiming: eventsPlan.canonicalTiming || timed.canonicalTiming || null,
      timingType: eventsPlan.timingType || timed.canonicalTiming?.timingType || null,
      bookingWindowStatusRaw: opportunity.bookingWindowStatus || null,
      priorityRaw: opportunity.priority || null,
      suppressedCustomerPills: [
        ...(eventsPlan.suppressedCustomerPills || []),
        ...baseSuppressed,
      ],
    };
  }

  const priorityFilterKey = normalizePriorityForFilter(timed.priority);
  const legacyStage = resolveCommercialStagePill(timed);
  const forResolve = {
    ...timed,
    commercialStageKey: legacyStage.key,
    commercialStagePillLabel: legacyStage.label,
    actionTimingFilterKey,
  };
  const stage = resolveCustomerCommercialStagePill(forResolve);
  const sales = resolveCustomerSalesAction(forResolve);
  const tileTiming = resolveUniversalTileTiming(timed);
  const verifiedDate = tileTiming.hasVerifiedUpcomingDate === true;
  const displayLabel = tileTiming.displayLabel || "Dates TBD";

  return {
    version: CUSTOMER_CLASSIFICATION_PRESENTATION_V1,
    cardPillStandard: "GDI_UNIVERSAL_SALES_ACTION_PILL_V1",
    priorityFilterKey,
    priorityPillLabel: PRIORITY_FILTER_LABEL[priorityFilterKey] || "Unclassified",
    showPriorityPill: true,
    showReadinessPill: false,
    commercialStageKey: stage.key,
    commercialStagePillLabel: stage.label,
    commercialStagePillClass: stage.pillClass,
    commercialStageSource: stage.source,
    commercialStageMissingMapped: stage.missingMapped === true,
    showCommercialStagePill: false,
    salesActionKey: sales.key,
    salesActionPillLabel: sales.label,
    salesActionReason: sales.reason,
    salesActionSourceFields: sales.sourceFields,
    showSalesActionPill: true,
    actionTimingFilterKey,
    actionTimingPillLabel:
      actionTimingFilterKey === BOOKING_WINDOW.CONTACT_NOW ||
      actionTimingFilterKey === BOOKING_WINDOW.WATCH ||
      actionTimingFilterKey === BOOKING_WINDOW.TOO_EARLY
        ? ACTION_TIMING_FILTER_LABEL[actionTimingFilterKey]
        : null,
    showActionTimingPill: false,
    showDatePill: true,
    datePillLabel:
      verifiedDate || tileTiming.timingType === TIMING_TYPE.ONGOING
        ? tileTiming.timingType === TIMING_TYPE.ONGOING
          ? "Ongoing"
          : "VERIFIED_DATE"
        : "Dates TBD",
    datePillDisplayLabel: displayLabel,
    hasVerifiedEventDate: verifiedDate,
    tileEventStartDate: tileTiming.start || timed.tileEventStartDate || null,
    tileEventEndDate: tileTiming.end || timed.tileEventEndDate || null,
    canonicalTiming: tileTiming.canonicalTiming || timed.canonicalTiming || null,
    timingType: tileTiming.timingType || null,
    bookingWindowStatusRaw: opportunity.bookingWindowStatus || null,
    priorityRaw: opportunity.priority || null,
    suppressedCustomerPills: baseSuppressed,
  };
}

/**
 * Attach filter-aligned fields onto a list DTO without inventing priority/timing.
 * Mutates a shallow copy.
 */
export function attachCustomerClassificationPresentation(dto = {}) {
  const plan = planCustomerTileBadges(dto);
  const isEvents = isAssociationConferenceEventsOpportunity(dto);
  const classified = {
    ...dto,
    // Filter keys used by browse UI (Priority remains a filter dimension)
    priorityFilterKey: plan.priorityFilterKey,
    // Events: align customer priority with filter (Watch when previously null/Unclassified).
    // Preserve raw research priority separately — do not invent High.
    ...(isEvents && !dto.priority
      ? { priority: plan.priorityFilterKey }
      : {}),
    actionTimingFilterKey: plan.actionTimingFilterKey,
    // Preserve raw storage for audit / weekly refresh
    bookingWindowStatusRaw:
      dto.bookingWindowStatusRaw || dto.bookingWindowStatus || null,
    // Customer Action Timing filter membership uses normalized key
    bookingWindowStatus:
      plan.actionTimingFilterKey === ACTION_TIMING_FILTER.UNCLASSIFIED
        ? null
        : plan.actionTimingFilterKey,
    bookingWindowLabel: plan.actionTimingPillLabel,
    tileBadgePlan: plan,
    commercialStageKey: plan.commercialStageKey,
    commercialStagePillLabel: plan.commercialStagePillLabel,
    commercialStagePillClass: plan.commercialStagePillClass || null,
    commercialStageFilterKey: plan.commercialStageKey,
    // Sales-action is the customer-facing middle pill; stage stays internal.
    salesActionKey: plan.salesActionKey,
    salesActionPillLabel: plan.salesActionPillLabel,
    salesActionReason: plan.salesActionReason || null,
    salesActionSourceFields: plan.salesActionSourceFields || null,
    // Universal timing — verified upcoming / month / ongoing for all segments
    tileEventStartDate: plan.tileEventStartDate || dto.tileEventStartDate || null,
    tileEventEndDate: plan.tileEventEndDate || dto.tileEventEndDate || null,
    datePillDisplayLabel:
      plan.datePillDisplayLabel || dto.datePillDisplayLabel || null,
    canonicalTiming: plan.canonicalTiming || dto.canonicalTiming || null,
    timingType: plan.timingType || dto.timingType || null,
    eventStartDate:
      plan.tileEventStartDate ||
      dto.eventStartDate ||
      dto.tileEventStartDate ||
      null,
    eventEndDate:
      plan.tileEventEndDate || dto.eventEndDate || dto.tileEventEndDate || null,
    eventDateDisplay:
      (plan.datePillDisplayLabel &&
      !/^dates tbd$/i.test(plan.datePillDisplayLabel) &&
      plan.datePillDisplayLabel !== "VERIFIED_DATE"
        ? plan.datePillDisplayLabel
        : null) ||
      dto.eventDateDisplay ||
      null,
    // Legacy Exploratory / Account Development labels suppressed from middle pill.
    inventoryLaneCustomerPillSuppressed: true,
  };
  // Sales Opportunities + Commercial Stage browse keys (structured fields only).
  const withFilters = attachSalesOpportunityFilterFields(classified);
  // Keep sales-action projection; re-resolve canonical stage after filter enrich.
  const projected = attachCustomerSalesActionProjection(withFilters);
  const stage = resolveCustomerCommercialStagePill(projected);
  const syncedPlan = {
    ...plan,
    cardPillStandard: "GDI_UNIVERSAL_SALES_ACTION_PILL_V1",
    commercialStageKey: stage.key,
    commercialStagePillLabel: stage.label,
    commercialStagePillClass: stage.pillClass,
    commercialStageSource: stage.source,
    commercialStageMissingMapped: stage.missingMapped === true,
    showCommercialStagePill: false,
    showPriorityPill: true,
    showReadinessPill: false,
    showSalesActionPill: true,
    salesActionKey: projected.salesActionKey,
    salesActionPillLabel: projected.salesActionPillLabel,
    salesActionReason: projected.salesActionReason,
    salesActionSourceFields: projected.salesActionSourceFields,
  };
  return {
    ...projected,
    commercialStageKey: stage.key,
    commercialStagePillLabel: stage.label,
    commercialStagePillClass: stage.pillClass,
    commercialStageFilterKey: stage.key,
    tileBadgePlan: syncedPlan,
  };
}

/**
 * Weekly classification refresh proposal (simulation / offline).
 * Does not write Airtable. Does not invent values without evidence hooks.
 */
export function proposeWeeklyClassificationRefresh({
  previous = {},
  nextEvidence = {},
  salesFeedback = null,
  researchRunId = null,
} = {}) {
  const prevPlan = planCustomerTileBadges(previous);
  // Evidence hooks only — never auto-promote without explicit signals.
  let proposedPriority = previous.priority || null;
  let proposedTiming = previous.bookingWindowStatus || null;
  const reasons = [];

  if (salesFeedback?.notRelevant === true || salesFeedback?.existingBusiness === true) {
    reasons.push({
      field: "priority",
      reason: "sales_feedback_precedence",
      salesFeedback,
    });
    // Do not silently change; signal review required.
    return {
      version: CUSTOMER_CLASSIFICATION_PRESENTATION_V1,
      mode: "SALES_FEEDBACK_REVIEW_REQUIRED",
      opportunityId: previous.id || previous.opportunityId || null,
      previous: {
        priority: previous.priority || null,
        bookingWindowStatus: previous.bookingWindowStatus || null,
        plan: prevPlan,
      },
      proposed: null,
      reasons,
      researchRunId,
      evaluationDate: new Date().toISOString(),
      identityPreserved: true,
    };
  }

  if (nextEvidence?.activityCanceled === true || nextEvidence?.hotelFitLost === true) {
    proposedPriority = PRIORITY.WATCHLIST;
    proposedTiming = BOOKING_WINDOW.TOO_EARLY;
    reasons.push({
      field: "both",
      reason: nextEvidence.activityCanceled
        ? "activity_canceled_or_superseded"
        : "hotel_fit_no_longer_credible",
      evidence: nextEvidence,
    });
  } else if (nextEvidence?.becameActionable === true) {
    proposedTiming = BOOKING_WINDOW.CONTACT_NOW;
    reasons.push({
      field: "bookingWindowStatus",
      reason: "milestone_now_actionable",
      evidence: nextEvidence,
    });
  } else if (nextEvidence?.moreCompelling === true && previous.priority === PRIORITY.MEDIUM) {
    proposedPriority = PRIORITY.HIGH;
    reasons.push({
      field: "priority",
      reason: "stronger_commercial_evidence",
      evidence: nextEvidence,
    });
  } else if (nextEvidence?.budgetPaused === true) {
    reasons.push({
      field: "none",
      reason: "budget_paused_priority_unchanged",
      evidence: nextEvidence,
    });
  } else if (nextEvidence?.conflicting === true) {
    reasons.push({
      field: "none",
      reason: "conflicting_evidence_preserve_uncertainty",
      evidence: nextEvidence,
    });
  } else if (!nextEvidence || Object.keys(nextEvidence).length === 0) {
    reasons.push({ field: "none", reason: "no_material_new_evidence" });
  }

  const proposed = {
    ...previous,
    priority: proposedPriority,
    bookingWindowStatus: proposedTiming,
  };
  const nextPlan = planCustomerTileBadges(proposed);
  const changed =
    prevPlan.priorityFilterKey !== nextPlan.priorityFilterKey ||
    prevPlan.actionTimingFilterKey !== nextPlan.actionTimingFilterKey;

  return {
    version: CUSTOMER_CLASSIFICATION_PRESENTATION_V1,
    mode: changed ? "PROPOSED_CHANGE" : "NO_CHANGE",
    opportunityId: previous.id || previous.opportunityId || null,
    previous: {
      priority: previous.priority || null,
      bookingWindowStatus: previous.bookingWindowStatus || null,
      plan: prevPlan,
    },
    proposed: {
      priority: proposedPriority,
      bookingWindowStatus: proposedTiming,
      plan: nextPlan,
    },
    reasons,
    researchRunId,
    evaluationDate: new Date().toISOString(),
    identityPreserved: true,
    customerFacingUpdateRequiresPublicationLifecycle: true,
  };
}
