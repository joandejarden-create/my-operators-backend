/**
 * Conditional "Hotel Need-Date Match" drawer extension.
 * Attaches near Opportunity Overview — does NOT replace the ten canonical sections.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_TIMING_OVERLAP,
  NEED_TIMING_OVERLAP_LABEL,
  NEED_REVENUE_TYPE_LABEL,
  NEED_OUTPUT_CATEGORY,
} from "./need-date-demand-contract-v1.js";
import { isCustomerNeedDateMatch } from "./need-period-timing-match-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Build compact need-date match block for customer drawer.
 */
export function buildNeedDateMatchDrawerComponent({
  needPeriod = null,
  timingMatch = null,
  association = null,
  opportunity = null,
} = {}) {
  if (!needPeriod?.needPeriodId || !timingMatch) return null;
  if (
    timingMatch.overlapStatus === NEED_TIMING_OVERLAP.OUTSIDE_PERIOD &&
    !association
  ) {
    return null;
  }

  const overlap = timingMatch.overlapStatus;
  const confirmedClaim =
    overlap === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP;

  const revenueLabels = (needPeriod.revenueNeed || [])
    .map((r) => NEED_REVENUE_TYPE_LABEL[r] || r)
    .filter(Boolean);

  const why = buildWhyMatch(needPeriod, timingMatch, opportunity);
  const uncertainty = buildUncertainty(timingMatch);
  const salesAction = buildSuggestedSalesAction(needPeriod, timingMatch);

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    componentId: "hotel_need_date_match",
    title: "Hotel Need-Date Match",
    placement: "near_opportunity_overview",
    replacesTenSections: false,
    needPeriodId: needPeriod.needPeriodId,
    needPeriodLabel: needPeriod.periodLabel,
    needPeriodRange: `${needPeriod.startDate} → ${needPeriod.endDate}`,
    revenueTarget: revenueLabels,
    timingOverlapStatus: overlap,
    timingOverlapLabel: NEED_TIMING_OVERLAP_LABEL[overlap] || overlap,
    confirmedNeedDateMatch: confirmedClaim,
    verifiedDemandDates: {
      start: timingMatch.demandStartDate || null,
      end: timingMatch.demandEndDate || null,
      demandDateKind: timingMatch.demandDateKind || null,
    },
    whyThisMatches: why,
    keyTimingUncertainty: uncertainty,
    suggestedSalesAction: salesAction,
    outputCategory:
      association?.outputCategory || NEED_OUTPUT_CATEGORY.EXISTING_MATCH,
    classificationLine: buildClassificationLine(needPeriod, timingMatch),
  };
}

function buildWhyMatch(needPeriod, timing, opportunity) {
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP) {
    return `Verified demand dates (${timing.demandStartDate}${
      timing.demandEndDate && timing.demandEndDate !== timing.demandStartDate
        ? `–${timing.demandEndDate}`
        : ""
    }) overlap the hotel need period ${needPeriod.periodLabel}.`;
  }
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP) {
    return `Source-backed timing evidence suggests demand during ${needPeriod.periodLabel}, but day-level confirmation is incomplete.`;
  }
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL) {
    return `Ongoing account/partnership could generate ${needPeriod.periodLabel} business, but specific month demand is not yet verified.`;
  }
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED) {
    return `Potential relevance to ${needPeriod.periodLabel}, but need-period timing is not established from evidence.`;
  }
  return text(opportunity?.title)
    ? `Reviewed against need period ${needPeriod.periodLabel}.`
    : null;
}

function buildUncertainty(timing) {
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP) {
    return null;
  }
  if (timing.demandDateKind === "SOURCING_PLANNING") {
    return "Sourcing/planning activity in-period does not prove guestroom or meeting revenue dates.";
  }
  return timing.reason || "Timing evidence incomplete for a confirmed need-date match.";
}

function buildSuggestedSalesAction(needPeriod, timing) {
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP) {
    return `Qualify capture for ${needPeriod.periodLabel}: confirm remaining inventory, buyer path, and a concrete next step tied to the verified demand dates.`;
  }
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP) {
    return `Verify exact demand dates for ${needPeriod.periodLabel} before committing hotel sales capacity.`;
  }
  if (timing.overlapStatus === NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL) {
    return `Ask whether this account has concrete ${needPeriod.periodLabel} travel/meeting demand; do not treat the partnership alone as a confirmed fill.`;
  }
  return `Clarify whether any demand lands in ${needPeriod.periodLabel} before pursuing as a need-date fill.`;
}

function buildClassificationLine(needPeriod, timing) {
  const label = NEED_TIMING_OVERLAP_LABEL[timing.overlapStatus] || "Need-date reviewed";
  return `Need dates · ${needPeriod.periodLabel} · ${label}`;
}

/**
 * Attach component onto a sanitized opportunity when associations exist.
 */
export function attachNeedDateMatchToOpportunity(opportunity = {}, opts = {}) {
  const associations = opts.associations || opportunity.needDateAssociations || [];
  if (!Array.isArray(associations) || !associations.length) {
    return opportunity;
  }
  const components = [];
  for (const assoc of associations) {
    const block = buildNeedDateMatchDrawerComponent({
      needPeriod: assoc.needPeriod,
      timingMatch: assoc.timing || assoc.timingMatch,
      association: assoc,
      opportunity,
    });
    if (block) components.push(block);
  }
  if (!components.length) return opportunity;

  const primary = components[0];
  return {
    ...opportunity,
    needDateMatchV1: primary,
    needDateMatchComponents: components,
    needDateClassificationLine: primary.classificationLine,
    // Preserve event timing on tile — do not overwrite with need month
    _needDateDoesNotOverrideEventTiming: true,
  };
}

export function opportunityMatchesNeedDatesFilter(opportunity = {}) {
  const status =
    opportunity.needDateMatchV1?.timingOverlapStatus ||
    opportunity.needDateAssociations?.[0]?.timing?.overlapStatus;
  return Boolean(status && isCustomerNeedDateMatch(status));
}
