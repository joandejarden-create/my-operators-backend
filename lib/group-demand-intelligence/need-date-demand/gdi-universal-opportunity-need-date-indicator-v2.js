/**
 * GDI_UNIVERSAL_OPPORTUNITY_NEED_DATE_SVG_V2
 *
 * Every customer-visible GDI opportunity resolves exactly one need-date
 * indicator (never null / never hidden). Composes hotel-level configuration
 * with opportunity timing — does not overwrite timing with hotel status.
 *
 * Never invents verified matches against market-estimated or TEST_SCENARIO
 * periods. Does not mutate commercial eligibility.
 */

import { applyCustomerNeedDatePresentationV3 } from "./gdi-need-date-customer-presentation-v3.js";

/**
 * Hotel config status strings (duplicated to avoid circular imports with
 * gdi-universal-need-date-intelligence-v1 ↔ opportunity-need-date-tile-preview).
 */
const HOTEL_STATUS = Object.freeze({
  HOTEL_CONFIRMED: "HOTEL_CONFIRMED",
  MARKET_ESTIMATED: "MARKET_ESTIMATED",
  NOT_ESTABLISHED: "NOT_ESTABLISHED",
});

/** Timing states emitted by opportunity-need-date-tile-preview-v1. */
const TIMING = Object.freeze({
  VERIFIED_DATE_MATCH: "VERIFIED_DATE_MATCH",
  POTENTIALLY_FLEXIBLE: "POTENTIALLY_FLEXIBLE",
  TIMING_RESEARCH_REQUIRED: "TIMING_RESEARCH_REQUIRED",
  NO_MEANINGFUL_MATCH: "NO_MEANINGFUL_MATCH",
});

export const GDI_UNIVERSAL_OPPORTUNITY_NEED_DATE_INDICATOR_V2 =
  "GDI_UNIVERSAL_OPPORTUNITY_NEED_DATE_INDICATOR_V2";

export const OPPORTUNITY_NEED_DATE_INDICATOR_STATE = Object.freeze({
  VERIFIED_DATE_MATCH: "VERIFIED_DATE_MATCH",
  POTENTIALLY_FLEXIBLE: "POTENTIALLY_FLEXIBLE",
  TIMING_RESEARCH_REQUIRED: "TIMING_RESEARCH_REQUIRED",
  NO_DATE_MATCH: "NO_DATE_MATCH",
  HOTEL_NEED_DATES_NOT_ESTABLISHED: "HOTEL_NEED_DATES_NOT_ESTABLISHED",
  MARKET_NEED_DATES_ESTIMATED: "MARKET_NEED_DATES_ESTIMATED",
});

export const OPPORTUNITY_NEED_DATE_INDICATOR_ICON = Object.freeze({
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH]: "calendar-check",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE]: "calendar-clock",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED]:
    "calendar-search",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.NO_DATE_MATCH]: "calendar-x",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED]:
    "calendar-minus",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.MARKET_NEED_DATES_ESTIMATED]:
    "calendar-dashed",
});

export const OPPORTUNITY_NEED_DATE_INDICATOR_COLOR_CLASS = Object.freeze({
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH]:
    "gdi-need-date-icon--verified",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE]:
    "gdi-need-date-icon--flexible",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED]:
    "gdi-need-date-icon--research",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.NO_DATE_MATCH]:
    "gdi-need-date-icon--no-match",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED]:
    "gdi-need-date-icon--not-established",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.MARKET_NEED_DATES_ESTIMATED]:
    "gdi-need-date-icon--estimated",
});

export const OPPORTUNITY_NEED_DATE_INDICATOR_COLOR_TOKEN = Object.freeze({
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH]:
    "var(--system--green-300)",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE]:
    "var(--secondary--color-5)",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED]:
    "var(--accent--primary-1)",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.NO_DATE_MATCH]:
    "var(--neutral--500, #8b93a7)",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED]:
    "var(--neutral--500, #8b93a7)",
  [OPPORTUNITY_NEED_DATE_INDICATOR_STATE.MARKET_NEED_DATES_ESTIMATED]:
    "var(--secondary--color-5)",
});

const POSITIVE_TIMING = new Set([
  TIMING.VERIFIED_DATE_MATCH,
  TIMING.POTENTIALLY_FLEXIBLE,
  TIMING.TIMING_RESEARCH_REQUIRED,
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function parseIsoDate(v) {
  const s = text(v);
  if (!/^\d{4}-\d{2}-\d{2}/.test(s)) return null;
  const d = new Date(`${s.slice(0, 10)}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * Sufficient date evidence to assert a negative match (not merely missing dates).
 */
export function hasReliableOpportunityDemandDates(opportunity = {}) {
  const start =
    parseIsoDate(opportunity.eventStartDate) ||
    parseIsoDate(opportunity.tileEventStartDate) ||
    parseIsoDate(opportunity.demandStartDate) ||
    parseIsoDate(opportunity.startDate);
  const end =
    parseIsoDate(opportunity.eventEndDate) ||
    parseIsoDate(opportunity.tileEventEndDate) ||
    parseIsoDate(opportunity.demandEndDate) ||
    parseIsoDate(opportunity.endDate) ||
    start;
  return Boolean(start && end);
}

function hotelConfigFlags(hotelNeedDateConfig = {}) {
  const status = upper(hotelNeedDateConfig.status || HOTEL_STATUS.NOT_ESTABLISHED);
  const confirmed =
    Number(
      hotelNeedDateConfig.confirmedPeriodCount ??
        hotelNeedDateConfig.counts?.hotelConfirmedPeriods ??
        0,
    ) || 0;
  const estimated =
    Number(
      hotelNeedDateConfig.estimatedPeriodCount ??
        hotelNeedDateConfig.counts?.marketEstimatedPeriods ??
        0,
    ) || 0;
  const hasConfirmed =
    status === HOTEL_STATUS.HOTEL_CONFIRMED || confirmed > 0;
  const hasEstimated =
    !hasConfirmed &&
    (status === HOTEL_STATUS.MARKET_ESTIMATED || estimated > 0);
  return { status, confirmed, estimated, hasConfirmed, hasEstimated };
}

function copyPresentationFields(state, extra = {}) {
  return {
    version: GDI_UNIVERSAL_OPPORTUNITY_NEED_DATE_INDICATOR_V2,
    state,
    indicatorState: state,
    icon: OPPORTUNITY_NEED_DATE_INDICATOR_ICON[state],
    colorClass: OPPORTUNITY_NEED_DATE_INDICATOR_COLOR_CLASS[state],
    colorToken: OPPORTUNITY_NEED_DATE_INDICATOR_COLOR_TOKEN[state],
    visible: true,
    timingDoesNotImplyAccessibility: true,
    ...extra,
  };
}

function labelsForState(state) {
  if (state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH) {
    return {
      accessibleLabel: "Need-date verified match",
      tooltipText: "Date match — opportunity dates verified against a hotel-confirmed need period.",
    };
  }
  if (state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE) {
    return {
      accessibleLabel: "Need-date potentially flexible",
      tooltipText:
        "Potentially flexible timing — confirm availability with the organizer. Not a verified need-date overlap.",
    };
  }
  if (state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED) {
    return {
      accessibleLabel: "Need-date timing research required",
      tooltipText:
        "Hotel need periods exist, but opportunity demand timing requires further investigation.",
    };
  }
  if (state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.NO_DATE_MATCH) {
    return {
      accessibleLabel: "No need-date match",
      tooltipText:
        "Assessed demand timing does not overlap active hotel-confirmed need periods. The opportunity may still be commercially relevant.",
    };
  }
  if (
    state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED
  ) {
    return {
      accessibleLabel: "Hotel need dates not established",
      tooltipText:
        "Commercial timing has not yet been matched to hotel-defined need periods. The opportunity may still be a qualified commercial opportunity.",
    };
  }
  return {
    accessibleLabel: "Market-estimated need dates",
    tooltipText:
      "Hotel has market-estimated need periods only — not hotel confirmed. Do not treat as a verified need-date match.",
  };
}

/**
 * Resolve exactly one universal opportunity need-date indicator.
 */
export function resolveUniversalOpportunityNeedDateIndicatorV2({
  opportunity = {},
  hotelNeedDateConfig = {},
  timingPreview = null,
} = {}) {
  const hotel = hotelConfigFlags(hotelNeedDateConfig);
  const preview = timingPreview && typeof timingPreview === "object" ? timingPreview : {};
  const timingState = text(preview.indicatorState || preview.state);
  const suppressed =
    preview.suppressedPositiveSignal === true ||
    /suppressed_commercially_inaccessible/i.test(text(preview.reason));

  let state = null;
  let reason = "unresolved";
  let fromTiming = false;

  if (hotel.hasConfirmed) {
    if (!suppressed && timingState === TIMING.VERIFIED_DATE_MATCH) {
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH;
      reason = preview.reason || "verified_or_supported_period_overlap";
      fromTiming = true;
    } else if (!suppressed && timingState === TIMING.POTENTIALLY_FLEXIBLE) {
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE;
      reason = preview.reason || "ongoing_account_with_flexibility_signal";
      fromTiming = true;
    } else if (!suppressed && timingState === TIMING.TIMING_RESEARCH_REQUIRED) {
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED;
      reason = preview.reason || "need_date_research_timing_unconfirmed";
      fromTiming = true;
    } else if (
      suppressed &&
      (POSITIVE_TIMING.has(timingState) ||
        /suppressed_commercially_inaccessible/i.test(text(preview.reason)) ||
        Number(preview.internalTimingMatchCount) > 0)
    ) {
      // Never claim verified/flexible when commercially inaccessible.
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED;
      reason = "positive_timing_suppressed_commercially_inaccessible";
    } else if (hasReliableOpportunityDemandDates(opportunity)) {
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.NO_DATE_MATCH;
      reason = "reliable_dates_outside_confirmed_need_periods";
    } else {
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED;
      reason = "confirmed_periods_timing_unresolved";
    }
  } else if (hotel.hasEstimated) {
    // Never VERIFIED against estimated periods.
    if (
      !suppressed &&
      (timingState === TIMING.POTENTIALLY_FLEXIBLE ||
        timingState === TIMING.TIMING_RESEARCH_REQUIRED ||
        preview.demotedFromVerifiedBecause === "MARKET_ESTIMATED_PERIOD")
    ) {
      state =
        timingState === TIMING.TIMING_RESEARCH_REQUIRED
          ? OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED
          : OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE;
      reason = preview.reason || "estimated_period_timing_supported";
      fromTiming = true;
    } else {
      state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.MARKET_NEED_DATES_ESTIMATED;
      reason = "market_estimated_hotel_periods_no_confirmed";
    }
  } else {
    state = OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED;
    reason = "hotel_need_dates_not_established";
  }

  const labels = labelsForState(state);
  const monthLabels = Array.isArray(preview.displayMonthLabels)
    ? preview.displayMonthLabels
    : [];

  // Prefer richer timing tooltips when they already encode months.
  let tooltipText = labels.tooltipText;
  let accessibleLabel = labels.accessibleLabel;
  if (fromTiming && text(preview.tooltipText)) tooltipText = text(preview.tooltipText);
  if (fromTiming && text(preview.accessibleLabel)) {
    accessibleLabel = text(preview.accessibleLabel);
  }
  if (
    state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED
  ) {
    accessibleLabel = "Hotel need dates not established";
    tooltipText =
      "Commercial timing has not yet been matched to hotel-defined need periods. The opportunity may still be a qualified commercial opportunity.";
  }

  return copyPresentationFields(state, {
    needPeriodIds: Array.isArray(preview.needPeriodIds) ? preview.needPeriodIds : [],
    displayMonthLabels: monthLabels,
    dateEvidenceStatus: preview.dateEvidenceStatus || null,
    flexibilityClassification: preview.flexibilityClassification || null,
    commercialAccessibilityQualification:
      preview.commercialAccessibilityQualification || null,
    tooltipText,
    accessibleLabel,
    researchProvenance: preview.researchProvenance || null,
    qualificationQuestion: preview.qualificationQuestion || null,
    recommendedSalesApproach: preview.recommendedSalesApproach || null,
    drawerTitle: preview.drawerTitle || null,
    reason,
    hotelId:
      hotelNeedDateConfig.hotelId ||
      opportunity.hotelId ||
      preview.hotelId ||
      null,
    hotelNeedDateStatus: hotel.status,
    confirmedPeriodCount: hotel.confirmed,
    estimatedPeriodCount: hotel.estimated,
    suppressedPositiveSignal: suppressed || undefined,
    periodProvenance: preview.periodProvenance || null,
    verifiedHotelNeedDateMatch:
      state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH,
  });
}

/**
 * Attach canonical needDateIndicator + always-visible tileNeedDatePreview.
 * Customer-facing fields use V3 four-status presentation; underlying V2 state
 * remains available as underlyingState for audits.
 */
export function attachUniversalOpportunityNeedDateIndicator(
  opportunity = {},
  opts = {},
) {
  const indicator = resolveUniversalOpportunityNeedDateIndicatorV2({
    opportunity,
    hotelNeedDateConfig: opts.hotelNeedDateConfig || {},
    timingPreview: opts.timingPreview || opportunity.tileNeedDatePreview || null,
  });

  const priorPreview = opportunity.tileNeedDatePreview || {};
  const withUnderlying = {
    ...opportunity,
    needDateIndicator: {
      ...indicator,
      state: indicator.state,
      icon: indicator.icon,
      colorClass: indicator.colorClass,
      colorToken: indicator.colorToken,
      accessibleLabel: indicator.accessibleLabel,
      tooltipText: indicator.tooltipText,
      visible: true,
      version: indicator.version,
      hotelNeedDateStatus: indicator.hotelNeedDateStatus,
      reason: indicator.reason,
      verifiedHotelNeedDateMatch: indicator.verifiedHotelNeedDateMatch === true,
    },
    tileNeedDatePreview: {
      ...priorPreview,
      ...indicator,
      version: priorPreview.version || priorPreview.contract || indicator.version,
      indicatorVersion: indicator.version,
      contract: priorPreview.contract || null,
    },
  };

  return applyCustomerNeedDatePresentationV3(withUnderlying, {
    hotelNeedDateConfig: opts.hotelNeedDateConfig || {},
    underlyingIndicator: withUnderlying.needDateIndicator,
  });
}

/** Deterministic fixtures for the six-state contract. */
export function runUniversalOpportunityNeedDateIndicatorGate() {
  const failures = [];
  const hotelConfirmed = {
    hotelId: "recLuxvwwxID7U2B8",
    status: HOTEL_STATUS.HOTEL_CONFIRMED,
    confirmedPeriodCount: 8,
    estimatedPeriodCount: 0,
  };
  const hotelEstimated = {
    hotelId: "recgMYovrrZDJMqzX",
    status: HOTEL_STATUS.MARKET_ESTIMATED,
    confirmedPeriodCount: 0,
    estimatedPeriodCount: 4,
  };
  const hotelNone = {
    hotelId: "recKRJjcPnb4tVDDS",
    status: HOTEL_STATUS.NOT_ESTABLISHED,
    confirmedPeriodCount: 0,
    estimatedPeriodCount: 0,
  };

  const cases = [
    {
      id: "verified",
      opportunity: { id: "a", eventStartDate: "2027-01-14", eventEndDate: "2027-01-16" },
      hotelNeedDateConfig: hotelConfirmed,
      timingPreview: {
        indicatorState: TIMING.VERIFIED_DATE_MATCH,
        visible: true,
        icon: "calendar-check",
        tooltipText: "Date match: January 2027 — opportunity dates verified.",
        accessibleLabel: "Need-date verified match for January 2027",
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH,
    },
    {
      id: "flexible",
      opportunity: { id: "b" },
      hotelNeedDateConfig: hotelConfirmed,
      timingPreview: {
        indicatorState: TIMING.POTENTIALLY_FLEXIBLE,
        visible: true,
        icon: "calendar-clock",
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE,
    },
    {
      id: "research",
      opportunity: { id: "c" },
      hotelNeedDateConfig: hotelConfirmed,
      timingPreview: {
        indicatorState: TIMING.TIMING_RESEARCH_REQUIRED,
        visible: true,
        icon: "calendar-search",
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED,
    },
    {
      id: "no_date_match",
      opportunity: {
        id: "d",
        eventStartDate: "2028-06-10",
        eventEndDate: "2028-06-12",
      },
      hotelNeedDateConfig: hotelConfirmed,
      timingPreview: {
        indicatorState: TIMING.NO_MEANINGFUL_MATCH,
        visible: false,
        icon: null,
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.NO_DATE_MATCH,
    },
    {
      id: "unresolved_dates",
      opportunity: { id: "e", title: "Unknown timing account" },
      hotelNeedDateConfig: hotelConfirmed,
      timingPreview: {
        indicatorState: TIMING.NO_MEANINGFUL_MATCH,
        visible: false,
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.TIMING_RESEARCH_REQUIRED,
    },
    {
      id: "spice_not_established",
      opportunity: { id: "f", title: "CARPHA" },
      hotelNeedDateConfig: hotelNone,
      timingPreview: {
        indicatorState: TIMING.NO_MEANINGFUL_MATCH,
        visible: false,
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED,
    },
    {
      id: "waterstone_estimated",
      opportunity: { id: "g", eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" },
      hotelNeedDateConfig: hotelEstimated,
      timingPreview: {
        indicatorState: TIMING.NO_MEANINGFUL_MATCH,
        visible: false,
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.MARKET_NEED_DATES_ESTIMATED,
    },
    {
      id: "never_verified_on_estimated",
      opportunity: { id: "h", eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" },
      hotelNeedDateConfig: hotelEstimated,
      timingPreview: {
        indicatorState: TIMING.VERIFIED_DATE_MATCH,
        visible: true,
        icon: "calendar-check",
        demotedFromVerifiedBecause: "MARKET_ESTIMATED_PERIOD",
      },
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.POTENTIALLY_FLEXIBLE,
    },
    {
      id: "no_research_hotel",
      opportunity: { id: "i", title: "Fresh hotel opp" },
      hotelNeedDateConfig: {
        hotelId: "hotel_new",
        status: HOTEL_STATUS.NOT_ESTABLISHED,
        confirmedPeriodCount: 0,
        estimatedPeriodCount: 0,
      },
      timingPreview: null,
      expect: OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED,
    },
  ];

  const seen = new Set();
  for (const c of cases) {
    const ind = resolveUniversalOpportunityNeedDateIndicatorV2(c);
    if (ind.state !== c.expect) {
      failures.push({ id: c.id, detail: "state_mismatch", got: ind.state, expect: c.expect });
    }
    if (!ind.icon || !ind.state || ind.visible !== true) {
      failures.push({ id: c.id, detail: "missing_required_fields", got: ind });
    }
    if (ind.state === OPPORTUNITY_NEED_DATE_INDICATOR_STATE.VERIFIED_DATE_MATCH) {
      if (c.hotelNeedDateConfig.status !== HOTEL_STATUS.HOTEL_CONFIRMED) {
        failures.push({ id: c.id, detail: "verified_without_confirmed_hotel" });
      }
    }
    seen.add(ind.state);
  }

  for (const s of Object.values(OPPORTUNITY_NEED_DATE_INDICATOR_STATE)) {
    if (!seen.has(s)) {
      failures.push({ id: "coverage", detail: `missing_state_${s}` });
    }
  }

  return {
    pass: failures.length === 0,
    failures,
    statesCovered: [...seen].sort(),
    caseCount: cases.length,
  };
}
