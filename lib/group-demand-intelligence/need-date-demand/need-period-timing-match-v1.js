/**
 * Timing overlap of opportunity demand dates vs hotel need period.
 * Uses universal GDI timing intelligence; never treats sourcing-only as confirmed overlap.
 */

import {
  resolveUniversalTiming,
  TIMING_TYPE,
} from "../universal-timing-intelligence-v1.js";
import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_TIMING_OVERLAP,
  DEMAND_DATE_KIND,
} from "./need-date-demand-contract-v1.js";
import { intervalsOverlap } from "./need-period-normalize-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function parseIsoDay(raw) {
  const s = text(raw).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  const d = new Date(`${s}T00:00:00.000Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function classifyDemandDateKind(opportunity = {}, timing = {}) {
  const blob = [
    opportunity.opportunityType,
    opportunity.demandFamily,
    opportunity.researchSegment,
    opportunity.detailLayout,
    timing.timingNotes,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();

  if (
    opportunity.sourcingWindowOnly === true ||
    opportunity.dateKind === "SOURCING_PLANNING" ||
    /\bsourcing|rfp window|proposal deadline|bid deadline\b/i.test(blob)
  ) {
    return DEMAND_DATE_KIND.SOURCING_PLANNING;
  }
  if (
    opportunity.dateKind === "CATERING" ||
    /\bcatering|breakfast|lunch|dinner|banquet\b/i.test(blob)
  ) {
    return DEMAND_DATE_KIND.CATERING;
  }
  if (
    opportunity.dateKind === "MEETING" ||
    /\bday meeting|meeting space|board meeting\b/i.test(blob)
  ) {
    return DEMAND_DATE_KIND.MEETING;
  }
  if (
    opportunity.dateKind === "PROJECT_DEPLOYMENT" ||
    /\bdeployment|mobilization|project team\b/i.test(blob)
  ) {
    return DEMAND_DATE_KIND.PROJECT_DEPLOYMENT;
  }
  if (
    opportunity.dateKind === "GUEST_ARRIVAL_DEPARTURE" ||
    /\barrival|departure|room.?night|stay dates\b/i.test(blob)
  ) {
    return DEMAND_DATE_KIND.GUEST_ARRIVAL_DEPARTURE;
  }
  if (timing.timingType === TIMING_TYPE.ONGOING) {
    return DEMAND_DATE_KIND.UNKNOWN;
  }
  if (text(opportunity.eventStartDate) || text(opportunity.eventName)) {
    return DEMAND_DATE_KIND.EVENT;
  }
  return DEMAND_DATE_KIND.UNKNOWN;
}

function demandIntervalFromTiming(opportunity, timing) {
  const start =
    text(timing.startDate).slice(0, 10) ||
    text(opportunity.eventStartDate).slice(0, 10) ||
    text(opportunity.demandStartDate).slice(0, 10) ||
    null;
  const end =
    text(timing.endDate).slice(0, 10) ||
    text(opportunity.eventEndDate).slice(0, 10) ||
    text(opportunity.demandEndDate).slice(0, 10) ||
    start;

  if (timing.timingType === TIMING_TYPE.MONTH_ONLY && timing.year && timing.month) {
    const y = timing.year;
    const m = timing.month;
    const s = `${y}-${String(m).padStart(2, "0")}-01`;
    const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
    const e = `${y}-${String(m).padStart(2, "0")}-${String(last).padStart(2, "0")}`;
    return {
      startDate: s,
      endDate: e,
      precision: "MONTH",
      confirmed: Boolean(timing.dateEvidence?.length || timing.hasVerifiedUpcomingDate),
    };
  }

  if (start && parseIsoDay(start)) {
    return {
      startDate: start,
      endDate: end && parseIsoDay(end) ? end : start,
      precision: timing.precision || "DAY",
      confirmed: Boolean(
        timing.hasVerifiedUpcomingDate ||
          timing.dateStatus === "VERIFIED_UPCOMING" ||
          timing.dateStatus === "VERIFIED_ACTIVE" ||
          (Array.isArray(timing.dateEvidence) && timing.dateEvidence.length > 0),
      ),
    };
  }

  return null;
}

/**
 * @param {object} needPeriod normalized need period
 * @param {object} opportunity GDI opportunity
 */
export function evaluateNeedPeriodTimingOverlap(needPeriod, opportunity = {}) {
  const periodStart = text(needPeriod?.startDate).slice(0, 10);
  const periodEnd = text(needPeriod?.endDate).slice(0, 10);
  const timing = resolveUniversalTiming(opportunity);
  const dateKind = classifyDemandDateKind(opportunity, timing);
  const demand = demandIntervalFromTiming(opportunity, timing);

  const base = {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    needPeriodId: needPeriod?.needPeriodId || null,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    demandDateKind: dateKind,
    canonicalTimingType: timing?.timingType || null,
    demandStartDate: demand?.startDate || null,
    demandEndDate: demand?.endDate || null,
    needStartDate: periodStart || null,
    needEndDate: periodEnd || null,
  };

  if (!periodStart || !periodEnd) {
    return {
      ...base,
      overlapStatus: NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED,
      reason: "need_period_dates_missing",
      confirmedRevenueTiming: false,
    };
  }

  // Sourcing / planning dates in-period are NOT confirmed hotel revenue overlap.
  if (dateKind === DEMAND_DATE_KIND.SOURCING_PLANNING) {
    const sourcingOverlaps =
      demand &&
      intervalsOverlap(demand.startDate, demand.endDate, periodStart, periodEnd);
    return {
      ...base,
      overlapStatus: sourcingOverlaps
        ? NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED
        : demand
          ? NEED_TIMING_OVERLAP.OUTSIDE_PERIOD
          : NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED,
      reason: sourcingOverlaps
        ? "sourcing_window_in_period_does_not_prove_revenue_dates"
        : "sourcing_or_unknown_timing",
      confirmedRevenueTiming: false,
    };
  }

  if (timing?.timingType === TIMING_TYPE.ONGOING) {
    return {
      ...base,
      overlapStatus: NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL,
      reason: "ongoing_account_without_verified_month_demand",
      confirmedRevenueTiming: false,
    };
  }

  if (!demand) {
    return {
      ...base,
      overlapStatus: NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED,
      reason: "no_recoverable_demand_dates",
      confirmedRevenueTiming: false,
    };
  }

  const overlaps = intervalsOverlap(
    demand.startDate,
    demand.endDate,
    periodStart,
    periodEnd,
  );

  if (!overlaps) {
    return {
      ...base,
      overlapStatus: NEED_TIMING_OVERLAP.OUTSIDE_PERIOD,
      reason: "demand_dates_outside_need_period",
      confirmedRevenueTiming: false,
    };
  }

  if (demand.confirmed && demand.precision !== "YEAR") {
    return {
      ...base,
      overlapStatus: NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP,
      reason: "verified_demand_dates_overlap_need_period",
      confirmedRevenueTiming: true,
    };
  }

  if (demand.precision === "MONTH" || demand.precision === "YEAR") {
    return {
      ...base,
      overlapStatus: NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP,
      reason: "partial_date_precision_overlap",
      confirmedRevenueTiming: false,
    };
  }

  return {
    ...base,
    overlapStatus: NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP,
    reason: "overlap_without_full_verification",
    confirmedRevenueTiming: false,
  };
}

export function isCustomerNeedDateMatch(overlapStatus) {
  return (
    overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP ||
    overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP
  );
}
