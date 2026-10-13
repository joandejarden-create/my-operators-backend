/**
 * Future-cycle evidence states + Watchlist preservation helpers.
 * REUSABLE_PRODUCT_LOGIC — no hotel-name hardcodes.
 *
 * Prior-year host / recurring annual events with unpublished next cycle
 * must land WATCHLIST / FUTURE_CYCLE, not hard DISQUALIFIED, when market
 * relevance remains plausible.
 */

import {
  BOOKING_WINDOW,
  DEMAND_STATUS,
  OPPORTUNITY_TYPE,
  REACTIVATION_SIGNAL,
  VENUE_SOURCING_STATUS,
} from "./claim-types.js";

/** Canonical future-cycle evidence states (REUSABLE_PRODUCT_LOGIC). */
export const FUTURE_CYCLE_EVIDENCE_STATE = Object.freeze({
  CURRENT_FUTURE_CYCLE_CONFIRMED: "CURRENT_FUTURE_CYCLE_CONFIRMED",
  RECURRING_CYCLE_STRONG: "RECURRING_CYCLE_STRONG",
  PRIOR_YEAR_HOST_KNOWN: "PRIOR_YEAR_HOST_KNOWN",
  FUTURE_CYCLE_EXPECTED: "FUTURE_CYCLE_EXPECTED",
  FUTURE_CYCLE_UNCONFIRMED: "FUTURE_CYCLE_UNCONFIRMED",
  NO_FUTURE_CYCLE_EVIDENCE: "NO_FUTURE_CYCLE_EVIDENCE",
});

export const FUTURE_CYCLE_EVIDENCE_STATE_LABEL = Object.freeze({
  CURRENT_FUTURE_CYCLE_CONFIRMED: "Current / future cycle confirmed",
  RECURRING_CYCLE_STRONG: "Strong recurring-cycle evidence",
  PRIOR_YEAR_HOST_KNOWN: "Prior-year host known",
  FUTURE_CYCLE_EXPECTED: "Future cycle expected",
  FUTURE_CYCLE_UNCONFIRMED: "Future cycle unconfirmed",
  NO_FUTURE_CYCLE_EVIDENCE: "No future-cycle evidence",
});

const TERMINAL_NEGATIVE =
  /discontinued|cancelled permanently|no longer (held|operating)|ceased|defunct|moved permanently|relocated permanently|ended permanently|no recurrence|no future cycle|one[- ]time|single offsite|not recurring/i;

function textBlob(opportunity = {}) {
  return [
    opportunity.title,
    opportunity.whyNow,
    opportunity.hotelOpportunityThesis,
    opportunity.bethesdaWinThesis,
    opportunity.summaryWhat,
    opportunity.summaryWhyMatters,
    opportunity.summaryWhyHotel,
    opportunity.venueSourcingRationale,
    opportunity.bookingWindowRationale,
    opportunity.demandStatus,
    opportunity.opportunityType,
    ...(opportunity.labels || []),
    ...(opportunity.disqualifyReasons || []),
  ]
    .filter(Boolean)
    .map(String)
    .join(" \n ");
}

/**
 * Detect reusable future-cycle evidence from opportunity research fields.
 * @returns {{
 *   state: string,
 *   stateLabel: string,
 *   preserveAsWatch: boolean,
 *   signals: string[],
 *   terminalNegative: boolean,
 *   whyMonitor: string|null
 * }}
 */
export function detectFutureCycleEvidence(opportunity = {}) {
  const t = textBlob(opportunity);
  const signals = [];
  const terminalNegative = TERMINAL_NEGATIVE.test(t);

  const explicitType =
    opportunity.opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE ||
    /future[_ ]cycle/i.test(String(opportunity.opportunityType || ""));
  if (explicitType) signals.push("explicit_opportunity_type_future_cycle");

  const demandRecurring =
    opportunity.demandStatus === DEMAND_STATUS.RECURRING_PREDICTED ||
    opportunity.demandStatus === DEMAND_STATUS.SPECULATIVE_WATCH;
  if (demandRecurring) signals.push(`demand_status_${opportunity.demandStatus}`);

  const bookingWatch =
    opportunity.bookingWindowStatus === BOOKING_WINDOW.WATCH ||
    opportunity.bookingWindowStatus === BOOKING_WINDOW.TOO_EARLY;
  if (bookingWatch) signals.push(`booking_window_${opportunity.bookingWindowStatus}`);

  const recurringText =
    !terminalNegative &&
    /recurring|annual(ly)?|each year|every year|year[- ]over[- ]year|next (fall|spring|summer|winter|year|cycle)|future (fall|spring|cycle|year)|(?:next|upcoming|following)\s+20(2[7-9]|3\d)\b/i.test(
      t
    );
  if (recurringText) signals.push("recurring_or_next_cycle_language");

  const unpublishedNext =
    /not yet (announced|published|confirmed)|next cycle (not|un)|dates? (not|tbd|unknown)|cycle unpublished|sourcing for 20(2[7-9]|3\d)|20(2[7-9]|3\d)\s*sourcing|sourcing opens|actionable window for .{0,40}(next|future)|effectively closed.{0,80}(next|future|recurring)|learn whether .{0,40}closed/i.test(
      t
    );
  if (unpublishedNext) signals.push("next_cycle_unpublished");

  const priorHost =
    /prior[- ]year (host|hotel|venue)|previous(ly)? (hosted|host hotel)|hosted (at|by) .*(hotel|marriott|hilton|hyatt)|historical host|last year('s)? (host|venue)|already uses .{0,40}hotels?|hotel(s)? precedent|marriott[- ]family precedent|uses times square hotels|prior (host|venue|hotel) known/i.test(
      t
    ) ||
    opportunity.reactivationSignal === REACTIVATION_SIGNAL.PRIOR_HOTEL_OPPORTUNITY ||
    opportunity.reactivationSignal === REACTIVATION_SIGNAL.PRIOR_MARRIOTT_OPPORTUNITY ||
    opportunity.reactivationSignal === REACTIVATION_SIGNAL.PRIOR_COMPETITOR_USAGE ||
    opportunity.reactivationSignal === REACTIVATION_SIGNAL.PRIOR_MARKET_PRESENCE;
  if (priorHost) signals.push("prior_year_host_or_reactivation");

  const currentCycleClosedLanguage =
    /20(2[4-6]) is effectively closed|current cycle closed|this year('s)? (host|venue) (selected|locked)|already (selected|placed|decided) for (this|current|20(2[4-6]))/i.test(
      t
    );
  if (currentCycleClosedLanguage) signals.push("current_cycle_closed_language");

  const venueClosedCycle =
    opportunity.venueSourcingStatus === VENUE_SOURCING_STATUS.CURRENT_CYCLE_CLOSED ||
    currentCycleClosedLanguage;
  if (venueClosedCycle && !currentCycleClosedLanguage) {
    signals.push("current_cycle_closed_language");
  }

  let state = FUTURE_CYCLE_EVIDENCE_STATE.NO_FUTURE_CYCLE_EVIDENCE;
  if (explicitType && (recurringText || unpublishedNext || priorHost || venueClosedCycle)) {
    state = FUTURE_CYCLE_EVIDENCE_STATE.CURRENT_FUTURE_CYCLE_CONFIRMED;
  } else if (recurringText && (priorHost || unpublishedNext || demandRecurring || venueClosedCycle)) {
    state = FUTURE_CYCLE_EVIDENCE_STATE.RECURRING_CYCLE_STRONG;
  } else if (priorHost && (recurringText || unpublishedNext || bookingWatch || venueClosedCycle)) {
    state = FUTURE_CYCLE_EVIDENCE_STATE.PRIOR_YEAR_HOST_KNOWN;
  } else if (demandRecurring || bookingWatch || unpublishedNext || venueClosedCycle) {
    state = FUTURE_CYCLE_EVIDENCE_STATE.FUTURE_CYCLE_EXPECTED;
  } else if (recurringText || explicitType) {
    state = FUTURE_CYCLE_EVIDENCE_STATE.FUTURE_CYCLE_UNCONFIRMED;
  }

  const preserveAsWatch =
    !terminalNegative &&
    state !== FUTURE_CYCLE_EVIDENCE_STATE.NO_FUTURE_CYCLE_EVIDENCE &&
    (state === FUTURE_CYCLE_EVIDENCE_STATE.CURRENT_FUTURE_CYCLE_CONFIRMED ||
      state === FUTURE_CYCLE_EVIDENCE_STATE.RECURRING_CYCLE_STRONG ||
      state === FUTURE_CYCLE_EVIDENCE_STATE.PRIOR_YEAR_HOST_KNOWN ||
      (state === FUTURE_CYCLE_EVIDENCE_STATE.FUTURE_CYCLE_EXPECTED &&
        (priorHost || recurringText || bookingWatch || venueClosedCycle)) ||
      // Placed/no-overflow + recurring language alone is enough to monitor, not pursue
      (state === FUTURE_CYCLE_EVIDENCE_STATE.FUTURE_CYCLE_UNCONFIRMED &&
        recurringText &&
        (opportunity.venueSourcingStatus ===
          VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE ||
          opportunity.venueSourcingStatus === VENUE_SOURCING_STATUS.FULLY_PLACED ||
          opportunity.venueSourcingStatus === VENUE_SOURCING_STATUS.CURRENT_CYCLE_CLOSED)));

  const whyMonitor = preserveAsWatch
    ? [
        "Recurring / future-cycle pattern evidenced",
        priorHost ? "prior host or reactivation signal known" : null,
        unpublishedNext ? "next cycle not yet published" : null,
        "monitor — not open current sourcing",
      ]
        .filter(Boolean)
        .join("; ")
    : null;

  return {
    state,
    stateLabel: FUTURE_CYCLE_EVIDENCE_STATE_LABEL[state],
    preserveAsWatch,
    signals,
    terminalNegative,
    whyMonitor,
  };
}

/**
 * True when a placed / no-overflow venue should still be retained as FUTURE_CYCLE watch.
 */
export function shouldPreservePlacedVenueAsFutureCycleWatch({
  opportunity = {},
  reactivationSignal = null,
  bookingWindowStatus = null,
} = {}) {
  const merged = {
    ...opportunity,
    reactivationSignal: reactivationSignal || opportunity.reactivationSignal,
    bookingWindowStatus: bookingWindowStatus || opportunity.bookingWindowStatus,
  };
  const evidence = detectFutureCycleEvidence(merged);
  if (evidence.preserveAsWatch) return { preserve: true, evidence };

  if (
    reactivationSignal === REACTIVATION_SIGNAL.PRIOR_HOTEL_OPPORTUNITY ||
    reactivationSignal === REACTIVATION_SIGNAL.PRIOR_MARRIOTT_OPPORTUNITY ||
    bookingWindowStatus === BOOKING_WINDOW.TOO_EARLY ||
    bookingWindowStatus === BOOKING_WINDOW.WATCH
  ) {
    return {
      preserve: true,
      evidence: {
        ...evidence,
        state: FUTURE_CYCLE_EVIDENCE_STATE.FUTURE_CYCLE_EXPECTED,
        preserveAsWatch: true,
        signals: [...evidence.signals, "legacy_reactivation_or_booking_watch"],
      },
    };
  }

  return { preserve: false, evidence };
}
