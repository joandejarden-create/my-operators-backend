/**
 * HIF V6 — Commercial Timing Model (founder 2026-11-21).
 * WHEN a franchise/management/operating decision may become commercially actionable.
 * Expiration alone ≠ commercial availability. Do not invent notice periods.
 */

export const HIF_V6_COMMERCIAL_TIMING_MODEL = "hif-v6-commercial-timing-model-v6";

/** Mutually exclusive, deterministic time horizons relative to asOf. */
export const TIMING_HORIZON = Object.freeze({
  NOW_ACTIVE_PROCESS: "NOW_ACTIVE_PROCESS",
  M0_6: "0_6_MONTHS",
  M6_12: "6_12_MONTHS",
  M12_24: "12_24_MONTHS",
  M24_36: "24_36_MONTHS",
  Y3_5: "3_5_YEARS",
  Y5_PLUS: "5_PLUS_YEARS",
  HISTORICAL_SUPERSEDED: "HISTORICAL_SUPERSEDED",
  UNKNOWN: "UNKNOWN",
});

export const TIMING_HORIZON_ORDER = Object.freeze([
  TIMING_HORIZON.NOW_ACTIVE_PROCESS,
  TIMING_HORIZON.M0_6,
  TIMING_HORIZON.M6_12,
  TIMING_HORIZON.M12_24,
  TIMING_HORIZON.M24_36,
  TIMING_HORIZON.Y3_5,
  TIMING_HORIZON.Y5_PLUS,
  TIMING_HORIZON.HISTORICAL_SUPERSEDED,
  TIMING_HORIZON.UNKNOWN,
]);

/** How the next commercial decision window was derived. */
export const DECISION_WINDOW_KIND = Object.freeze({
  CONFIRMED_DECISION_WINDOW: "CONFIRMED_DECISION_WINDOW",
  EVIDENCE_INFERRED_WINDOW: "EVIDENCE_INFERRED_WINDOW",
  EXPIRATION_ONLY_PROXY: "EXPIRATION_ONLY_PROXY",
  TIMING_UNKNOWN: "TIMING_UNKNOWN",
});

export function parseIsoDate(value) {
  if (!value) return null;
  const s = String(value).trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  const d = new Date(`${s}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function monthsBetween(asOfDate, eventDate) {
  const a = parseIsoDate(asOfDate);
  const e = parseIsoDate(eventDate);
  if (!a || !e) return null;
  const months =
    (e.getUTCFullYear() - a.getUTCFullYear()) * 12 + (e.getUTCMonth() - a.getUTCMonth());
  const dayAdj = e.getUTCDate() < a.getUTCDate() ? -1 : 0;
  return months + dayAdj;
}

/**
 * Deterministic horizon from months-to-event.
 * Historical if months < 0. NOW only when explicit active-process flag.
 */
export function classifyTimingHorizon(monthsToEvent, { activeProcess = false } = {}) {
  if (activeProcess) return TIMING_HORIZON.NOW_ACTIVE_PROCESS;
  if (monthsToEvent === null || monthsToEvent === undefined || Number.isNaN(monthsToEvent)) {
    return TIMING_HORIZON.UNKNOWN;
  }
  const m = Number(monthsToEvent);
  if (m < 0) return TIMING_HORIZON.HISTORICAL_SUPERSEDED;
  if (m < 6) return TIMING_HORIZON.M0_6;
  if (m < 12) return TIMING_HORIZON.M6_12;
  if (m < 24) return TIMING_HORIZON.M12_24;
  if (m < 36) return TIMING_HORIZON.M24_36;
  if (m < 60) return TIMING_HORIZON.Y3_5;
  return TIMING_HORIZON.Y5_PLUS;
}

/**
 * Derive NEXT_COMMERCIAL_DECISION_WINDOW without inventing notice periods.
 */
export function deriveNextCommercialDecisionWindow(row = {}, asOf) {
  const noticeDeadline = row.noticeDeadline || row.nonrenewalNoticeDeadline || row.verifiedNoticeDeadline || null;
  const renewalWindowStart = row.renewalNegotiationStart || row.renewalWindowStart || null;
  const terminationEffective = row.terminationEffectiveDate || row.terminationDate || null;
  const activeTrigger =
    row.activeCommercialTrigger ||
    row.confirmedFranchiseNonrenewal ||
    row.confirmedManagerDeparture ||
    row.activeConversionProcess ||
    null;
  const expiration =
    row.expirationDate ||
    row.primaryDate ||
    row.currentEffectiveExpiration ||
    row.verifiedEndDate ||
    row.futureExpirationDate ||
    null;

  if (noticeDeadline && parseIsoDate(noticeDeadline)) {
    const months = monthsBetween(asOf, noticeDeadline);
    return {
      nextCommercialDecisionDate: noticeDeadline,
      monthsToDecision: months,
      timingHorizon: classifyTimingHorizon(months),
      decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
      decisionBasis: "VERIFIED_NOTICE_DEADLINE",
      rawExpirationDate: expiration,
      note: "Confirmed notice/nonrenewal deadline — higher commercial certainty than expiration alone",
    };
  }

  if (terminationEffective && parseIsoDate(terminationEffective)) {
    const months = monthsBetween(asOf, terminationEffective);
    return {
      nextCommercialDecisionDate: terminationEffective,
      monthsToDecision: months,
      timingHorizon: classifyTimingHorizon(months, {
        activeProcess: months !== null && months <= 0,
      }),
      decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
      decisionBasis: "VERIFIED_TERMINATION_EFFECTIVE",
      rawExpirationDate: expiration,
      note: "Verified termination effective date",
    };
  }

  if (renewalWindowStart && parseIsoDate(renewalWindowStart)) {
    const months = monthsBetween(asOf, renewalWindowStart);
    return {
      nextCommercialDecisionDate: renewalWindowStart,
      monthsToDecision: months,
      timingHorizon: classifyTimingHorizon(months),
      decisionWindowKind: DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW,
      decisionBasis: "VERIFIED_RENEWAL_NEGOTIATION_PERIOD",
      rawExpirationDate: expiration,
      note: "Renewal negotiation window from source — not an invented notice period",
    };
  }

  if (activeTrigger) {
    return {
      nextCommercialDecisionDate: asOf,
      monthsToDecision: 0,
      timingHorizon: TIMING_HORIZON.NOW_ACTIVE_PROCESS,
      decisionWindowKind: DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW,
      decisionBasis: "ACTIVE_COMMERCIAL_TRIGGER",
      rawExpirationDate: expiration,
      note: String(activeTrigger).slice(0, 200),
    };
  }

  if (expiration && parseIsoDate(expiration)) {
    const months = monthsBetween(asOf, expiration);
    return {
      nextCommercialDecisionDate: expiration,
      monthsToDecision: months,
      timingHorizon: classifyTimingHorizon(months),
      decisionWindowKind: DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
      decisionBasis: "CONTRACT_EXPIRATION_ONLY",
      rawExpirationDate: expiration,
      note: "Expiration is a proxy only — not automatic commercial availability or confirmed decision window",
    };
  }

  return {
    nextCommercialDecisionDate: null,
    monthsToDecision: null,
    timingHorizon: TIMING_HORIZON.UNKNOWN,
    decisionWindowKind: DECISION_WINDOW_KIND.TIMING_UNKNOWN,
    decisionBasis: "NO_SUPPORTED_DATE",
    rawExpirationDate: null,
    note: "No verified notice, termination, renewal window, or expiration date",
  };
}

export function emptyHorizonCounts() {
  const o = {};
  for (const h of TIMING_HORIZON_ORDER) o[h] = 0;
  return o;
}
