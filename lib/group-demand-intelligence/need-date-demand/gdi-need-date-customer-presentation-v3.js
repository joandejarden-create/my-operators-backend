/**
 * GDI_NEED_DATE_INTELLIGENCE_UX_SIMPLIFICATION_V3
 *
 * Canonical mapping from underlying Need-Date intelligence states to FOUR
 * customer-facing opportunity display statuses. Presentation only — does not
 * change research, eligibility, Airtable, or publication.
 */

/**
 * Underlying V2 state strings (duplicated to avoid circular import with
 * gdi-universal-opportunity-need-date-indicator-v2.js).
 */
const UNDERLYING = Object.freeze({
  VERIFIED_DATE_MATCH: "VERIFIED_DATE_MATCH",
  POTENTIALLY_FLEXIBLE: "POTENTIALLY_FLEXIBLE",
  TIMING_RESEARCH_REQUIRED: "TIMING_RESEARCH_REQUIRED",
  NO_DATE_MATCH: "NO_DATE_MATCH",
  HOTEL_NEED_DATES_NOT_ESTABLISHED: "HOTEL_NEED_DATES_NOT_ESTABLISHED",
  MARKET_NEED_DATES_ESTIMATED: "MARKET_NEED_DATES_ESTIMATED",
});

export const GDI_NEED_DATE_CUSTOMER_PRESENTATION_V3 =
  "GDI_NEED_DATE_CUSTOMER_PRESENTATION_V3";

/** Customer-facing opportunity timing display statuses (exactly four). */
export const NEED_DATE_CUSTOMER_DISPLAY = Object.freeze({
  MATCHES_NEED_DATE: "MATCHES_NEED_DATE",
  POTENTIAL_MATCH: "POTENTIAL_MATCH",
  NOT_YET_ASSESSED: "NOT_YET_ASSESSED",
  NO_CURRENT_MATCH: "NO_CURRENT_MATCH",
});

export const NEED_DATE_CUSTOMER_DISPLAY_ICON = Object.freeze({
  [NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE]: "calendar-check",
  [NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH]: "calendar-clock",
  [NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED]: "calendar-question",
  [NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH]: "calendar-x",
});

export const NEED_DATE_CUSTOMER_DISPLAY_COLOR_CLASS = Object.freeze({
  [NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE]: "gdi-need-date-icon--matches",
  [NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH]: "gdi-need-date-icon--potential",
  [NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED]: "gdi-need-date-icon--not-assessed",
  [NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH]: "gdi-need-date-icon--no-match",
});

export const NEED_DATE_CUSTOMER_DISPLAY_LABEL = Object.freeze({
  [NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE]: "Matches a Confirmed Need Date",
  [NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH]: "Potential Match",
  [NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED]: "Not Yet Assessed",
  [NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH]: "No Current Match",
});

export const NEED_DATE_CUSTOMER_DISPLAY_COLOR_TOKEN = Object.freeze({
  [NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE]: "var(--system--green-300)",
  [NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH]: "var(--secondary--color-5)",
  // V4: light blue-gray vs muted slate so the two gray statuses stay distinct.
  [NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED]:
    "var(--gdi-need-date-not-assessed, #94A3B8)",
  [NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH]:
    "var(--gdi-need-date-no-match, #64748B)",
});

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

function hasReliableOpportunityDemandDates(opportunity = {}) {
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

function formatMonthList(labels = []) {
  const labs = [...new Set((labels || []).filter(Boolean).map(text))];
  if (!labs.length) return null;
  if (labs.length === 1) return labs[0];
  if (labs.length === 2) return `${labs[0]} and ${labs[1]}`;
  return `${labs.slice(0, -1).join(", ")}, and ${labs[labs.length - 1]}`;
}

/**
 * Credible opportunity-specific timing thesis (not hotel market estimate alone).
 */
export function hasOpportunitySpecificTimingThesis(opportunity = {}, underlying = {}) {
  const flex = upper(
    underlying.flexibilityClassification ||
      opportunity.flexibilityClassification ||
      "",
  );
  if (
    flex &&
    flex !== "UNKNOWN" &&
    flex !== "FIXED_VERIFIED" &&
    flex !== "NONE" &&
    flex !== "NOT_APPLICABLE"
  ) {
    return true;
  }
  if (underlying.demotedFromVerifiedBecause === "MARKET_ESTIMATED_PERIOD") {
    return true;
  }
  const reason = text(underlying.reason);
  if (/flexibility|tentative|ongoing_account|estimated_period_timing_supported/i.test(reason)) {
    return true;
  }
  const timingState = text(underlying.indicatorState || underlying.state);
  if (
    timingState === UNDERLYING.POTENTIALLY_FLEXIBLE ||
    timingState === "POTENTIAL_MATCH"
  ) {
    return true;
  }
  return false;
}

function notYetAssessedReason(opportunity = {}, hotelStatus = "", underlying = {}) {
  const hotelMissing =
    upper(hotelStatus) === "NOT_ESTABLISHED" ||
    underlying.state === UNDERLYING.HOTEL_NEED_DATES_NOT_ESTABLISHED ||
    /hotel_need_dates_not_established/i.test(text(underlying.reason));
  const oppDatesMissing = !hasReliableOpportunityDemandDates(opportunity);
  if (hotelMissing && oppDatesMissing) {
    return {
      reasonCode: "both_missing",
      explanation: "More information is needed to assess timing.",
    };
  }
  if (hotelMissing) {
    return {
      reasonCode: "hotel_need_dates_missing",
      explanation:
        "The hotel has not provided need dates. This opportunity may still be worth pursuing.",
    };
  }
  if (oppDatesMissing || /timing_unresolved|research|unconfirmed/i.test(text(underlying.reason))) {
    return {
      reasonCode: "opportunity_timing_missing",
      explanation: "Opportunity timing requires further research.",
    };
  }
  if (upper(hotelStatus) === "MARKET_ESTIMATED") {
    return {
      reasonCode: "market_estimated_no_timing_thesis",
      explanation:
        "The hotel has market-estimated need periods only. Opportunity timing has not been confirmed against them.",
    };
  }
  return {
    reasonCode: "insufficient_evidence",
    explanation: "More information is needed to assess timing.",
  };
}

function potentialMatchBasis(underlying = {}) {
  if (underlying.demotedFromVerifiedBecause === "MARKET_ESTIMATED_PERIOD") {
    return "market_estimate";
  }
  const flex = upper(underlying.flexibilityClassification || "");
  if (/FLEXIBLE|RECURRING/.test(flex)) return "flexible_timing";
  if (/tentative|unconfirmed/i.test(text(underlying.dateEvidenceStatus))) {
    return "tentative_dates";
  }
  if (/estimated/i.test(text(underlying.reason))) return "market_estimate";
  if (flex) return "flexible_timing";
  return "timing_thesis";
}

/**
 * Map underlying intelligence → one of four customer display statuses.
 */
export function mapUnderlyingNeedDateToCustomerDisplayV3({
  opportunity = {},
  hotelNeedDateConfig = {},
  underlyingIndicator = null,
} = {}) {
  const underlying =
    underlyingIndicator && typeof underlyingIndicator === "object"
      ? underlyingIndicator
      : {};
  const underlyingState = text(
    underlying.state || underlying.indicatorState || "",
  );
  const hotelStatus = upper(
    hotelNeedDateConfig.status || underlying.hotelNeedDateStatus || "NOT_ESTABLISHED",
  );
  const months = formatMonthList(underlying.displayMonthLabels);

  let display = NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED;
  let potentialBasis = null;
  let assessMeta = null;

  if (
    underlyingState === UNDERLYING.VERIFIED_DATE_MATCH ||
    underlyingState === "MATCHES_NEED_DATE"
  ) {
    // Never green against market-estimated hotels.
    if (hotelStatus === "MARKET_ESTIMATED") {
      display = NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH;
      potentialBasis = "market_estimate";
    } else {
      display = NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE;
    }
  } else if (
    underlyingState === UNDERLYING.POTENTIALLY_FLEXIBLE ||
    underlyingState === "POTENTIAL_MATCH"
  ) {
    display = NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH;
    potentialBasis = potentialMatchBasis(underlying);
  } else if (
    underlyingState === UNDERLYING.NO_DATE_MATCH ||
    underlyingState === "NO_CURRENT_MATCH"
  ) {
    display = NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH;
  } else if (underlyingState === UNDERLYING.MARKET_NEED_DATES_ESTIMATED) {
    // Market estimate alone ≠ Potential Match for every opportunity.
    if (hasOpportunitySpecificTimingThesis(opportunity, underlying)) {
      display = NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH;
      potentialBasis = "market_estimate";
    } else {
      display = NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED;
      assessMeta = notYetAssessedReason(opportunity, hotelStatus, underlying);
    }
  } else {
    // TIMING_RESEARCH_REQUIRED, HOTEL_NEED_DATES_NOT_ESTABLISHED, unknown → Not yet assessed
    display = NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED;
    assessMeta = notYetAssessedReason(opportunity, hotelStatus, underlying);
  }

  const displayLabel = NEED_DATE_CUSTOMER_DISPLAY_LABEL[display];
  let tooltipTitle = displayLabel;
  let tooltipExplanation = "";
  let tooltipText = displayLabel;

  if (display === NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE) {
    tooltipExplanation = months
      ? `This opportunity overlaps with a hotel-confirmed need period (${months}).`
      : "This opportunity overlaps with a hotel-confirmed need period.";
    tooltipText = `${tooltipTitle}. ${tooltipExplanation}`;
  } else if (display === NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH) {
    const basisNote =
      potentialBasis === "flexible_timing"
        ? "Basis: flexible timing."
        : potentialBasis === "tentative_dates"
          ? "Basis: tentative opportunity dates."
          : potentialBasis === "market_estimate"
            ? "Basis: market estimate — not hotel confirmed."
            : "Confirmation is needed.";
    tooltipExplanation = `The timing could work, but confirmation is needed. ${basisNote}`;
    tooltipText = `${tooltipTitle}. ${tooltipExplanation}`;
  } else if (display === NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH) {
    tooltipExplanation =
      "The known opportunity dates do not overlap with the hotel's confirmed need periods. This does not mean the opportunity is unsuitable.";
    tooltipText = `${tooltipTitle}. ${tooltipExplanation}`;
  } else {
    assessMeta =
      assessMeta || notYetAssessedReason(opportunity, hotelStatus, underlying);
    tooltipExplanation = assessMeta.explanation;
    tooltipText = `${tooltipTitle}. ${tooltipExplanation}`;
  }

  return {
    version: GDI_NEED_DATE_CUSTOMER_PRESENTATION_V3,
    state: display,
    displayStatus: display,
    icon: NEED_DATE_CUSTOMER_DISPLAY_ICON[display],
    colorClass: NEED_DATE_CUSTOMER_DISPLAY_COLOR_CLASS[display],
    colorToken: NEED_DATE_CUSTOMER_DISPLAY_COLOR_TOKEN[display],
    displayLabel,
    accessibleLabel: displayLabel,
    tooltipTitle,
    tooltipExplanation,
    tooltipText,
    visible: true,
    underlyingState: underlyingState || null,
    potentialMatchBasis: potentialBasis,
    notYetAssessedReasonCode: assessMeta?.reasonCode || null,
    displayMonthLabels: underlying.displayMonthLabels || [],
    hotelNeedDateStatus: hotelStatus || null,
    verifiedHotelNeedDateMatch:
      display === NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE,
    timingDoesNotImplyAccessibility: true,
  };
}

/**
 * Apply V3 customer presentation onto an opportunity that already has an
 * underlying needDateIndicator / tileNeedDatePreview.
 */
export function applyCustomerNeedDatePresentationV3(opportunity = {}, opts = {}) {
  const underlying =
    opts.underlyingIndicator ||
    opportunity.needDateIndicator ||
    opportunity.tileNeedDatePreview ||
    null;
  const customer = mapUnderlyingNeedDateToCustomerDisplayV3({
    opportunity,
    hotelNeedDateConfig: opts.hotelNeedDateConfig || {
      status: underlying?.hotelNeedDateStatus || "NOT_ESTABLISHED",
    },
    underlyingIndicator: underlying,
  });

  const priorPreview = opportunity.tileNeedDatePreview || {};
  return {
    ...opportunity,
    needDateIndicator: {
      ...customer,
      // Keep diagnostic underlying fields for gates / audits.
      underlyingState: customer.underlyingState,
      reason: underlying?.reason || null,
      hotelNeedDateStatus: customer.hotelNeedDateStatus,
    },
    tileNeedDatePreview: {
      ...priorPreview,
      // Customer-facing fields for any consumer still reading tile preview.
      indicatorState: customer.state,
      state: customer.state,
      icon: customer.icon,
      colorClass: customer.colorClass,
      colorToken: customer.colorToken,
      visible: true,
      tooltipText: customer.tooltipText,
      accessibleLabel: customer.accessibleLabel,
      displayLabel: customer.displayLabel,
      underlyingState: customer.underlyingState,
      customerPresentationVersion: GDI_NEED_DATE_CUSTOMER_PRESENTATION_V3,
      // Preserve prior timing metadata.
      displayMonthLabels:
        priorPreview.displayMonthLabels || customer.displayMonthLabels || [],
      reason: priorPreview.reason || underlying?.reason || null,
      version: priorPreview.version || priorPreview.contract || customer.version,
    },
  };
}

/** Deterministic gate for the four customer display states. */
export function runNeedDateCustomerPresentationV3Gate() {
  const failures = [];
  const cases = [
    {
      id: "verified_green",
      opportunity: { eventStartDate: "2027-01-14", eventEndDate: "2027-01-16" },
      hotelNeedDateConfig: { status: "HOTEL_CONFIRMED", confirmedPeriodCount: 8 },
      underlyingIndicator: {
        state: UNDERLYING.VERIFIED_DATE_MATCH,
        displayMonthLabels: ["January 2027"],
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE,
      expectIcon: "calendar-check",
    },
    {
      id: "flexible_potential",
      opportunity: {},
      hotelNeedDateConfig: { status: "HOTEL_CONFIRMED", confirmedPeriodCount: 8 },
      underlyingIndicator: {
        state: UNDERLYING.POTENTIALLY_FLEXIBLE,
        flexibilityClassification: "POTENTIALLY_FLEXIBLE",
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH,
      expectIcon: "calendar-clock",
    },
    {
      id: "research_not_assessed",
      opportunity: {},
      hotelNeedDateConfig: { status: "HOTEL_CONFIRMED", confirmedPeriodCount: 8 },
      underlyingIndicator: {
        state: UNDERLYING.TIMING_RESEARCH_REQUIRED,
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED,
      expectIcon: "calendar-question",
    },
    {
      id: "no_match",
      opportunity: { eventStartDate: "2028-06-10", eventEndDate: "2028-06-12" },
      hotelNeedDateConfig: { status: "HOTEL_CONFIRMED", confirmedPeriodCount: 8 },
      underlyingIndicator: {
        state: UNDERLYING.NO_DATE_MATCH,
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH,
      expectIcon: "calendar-x",
    },
    {
      id: "spice_not_established",
      opportunity: { title: "CARPHA" },
      hotelNeedDateConfig: { status: "NOT_ESTABLISHED" },
      underlyingIndicator: {
        state: UNDERLYING.HOTEL_NEED_DATES_NOT_ESTABLISHED,
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED,
      expectIcon: "calendar-question",
    },
    {
      id: "waterstone_estimate_alone_not_potential",
      opportunity: { eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" },
      hotelNeedDateConfig: {
        status: "MARKET_ESTIMATED",
        estimatedPeriodCount: 4,
      },
      underlyingIndicator: {
        state: UNDERLYING.MARKET_NEED_DATES_ESTIMATED,
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED,
      expectIcon: "calendar-question",
    },
    {
      id: "waterstone_estimate_with_timing_thesis",
      opportunity: { eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" },
      hotelNeedDateConfig: {
        status: "MARKET_ESTIMATED",
        estimatedPeriodCount: 4,
      },
      underlyingIndicator: {
        state: UNDERLYING.POTENTIALLY_FLEXIBLE,
        demotedFromVerifiedBecause: "MARKET_ESTIMATED_PERIOD",
        flexibilityClassification: "POTENTIALLY_FLEXIBLE",
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH,
      expectIcon: "calendar-clock",
    },
    {
      id: "never_verified_on_estimated",
      opportunity: { eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" },
      hotelNeedDateConfig: { status: "MARKET_ESTIMATED", estimatedPeriodCount: 4 },
      underlyingIndicator: {
        state: UNDERLYING.VERIFIED_DATE_MATCH,
        demotedFromVerifiedBecause: "MARKET_ESTIMATED_PERIOD",
      },
      expect: NEED_DATE_CUSTOMER_DISPLAY.POTENTIAL_MATCH,
      expectIcon: "calendar-clock",
    },
  ];

  const seen = new Set();
  for (const c of cases) {
    const out = mapUnderlyingNeedDateToCustomerDisplayV3(c);
    if (out.state !== c.expect) {
      failures.push({
        id: c.id,
        detail: "state_mismatch",
        got: out.state,
        expect: c.expect,
      });
    }
    if (out.icon !== c.expectIcon) {
      failures.push({
        id: c.id,
        detail: "icon_mismatch",
        got: out.icon,
        expect: c.expectIcon,
      });
    }
    if (!out.displayLabel || !out.tooltipText || out.visible !== true) {
      failures.push({ id: c.id, detail: "missing_customer_fields", got: out });
    }
    if (out.colorClass === "gdi-need-date-icon--research") {
      failures.push({ id: c.id, detail: "purple_research_class_forbidden" });
    }
    seen.add(out.state);
  }

  for (const s of Object.values(NEED_DATE_CUSTOMER_DISPLAY)) {
    if (!seen.has(s)) {
      failures.push({ id: "coverage", detail: `missing_display_${s}` });
    }
  }

  return {
    pass: failures.length === 0,
    failures,
    statesCovered: [...seen].sort(),
    caseCount: cases.length,
  };
}
