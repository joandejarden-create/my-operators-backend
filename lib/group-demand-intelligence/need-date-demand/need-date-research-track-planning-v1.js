/**
 * Need-Date research tracks V1 — planning only (zero paid).
 *
 * A. DATE_SPECIFIC_DEMAND — events/programmes with timing evidence for need months.
 * B. ACCOUNT_RELATIONSHIP_DEMAND — planners, venues, accounts with plausible future hotel need.
 *
 * Both may enter Research Continuation.
 * Only DATE_SPECIFIC with adequate timing counts as verified need-date matches.
 * Account opportunities are not discarded solely for missing dates.
 */

import {
  NEED_DATE_RESEARCH_STRATEGY,
  NEED_DATE_STRATEGY_META,
} from "./need-date-demand-contract-v1.js";

export const NEED_DATE_RESEARCH_TRACK = Object.freeze({
  DATE_SPECIFIC_DEMAND: "DATE_SPECIFIC_DEMAND",
  ACCOUNT_RELATIONSHIP_DEMAND: "ACCOUNT_RELATIONSHIP_DEMAND",
});

export const NEED_DATE_DEMAND_CLASS = Object.freeze({
  DIRECT_ADDRESSABLE_DEMAND: "DIRECT_ADDRESSABLE_DEMAND",
  OPEN_OR_UNCONFIRMED_SOURCING: "OPEN_OR_UNCONFIRMED_SOURCING",
  PARTNERSHIP_OR_REFERRAL_DEMAND: "PARTNERSHIP_OR_REFERRAL_DEMAND",
  EVIDENCED_OVERFLOW_OPPORTUNITY: "EVIDENCED_OVERFLOW_OPPORTUNITY",
  SPECULATIVE_OVERFLOW: "SPECULATIVE_OVERFLOW",
  COMPETITOR_COMMITTED: "COMPETITOR_COMMITTED",
});

/** Strategies that primarily surface dated programmes when run for need months. */
export const DATE_SPECIFIC_STRATEGY_IDS = Object.freeze([
  NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS,
  NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES,
  NEED_DATE_RESEARCH_STRATEGY.EDUCATION_SCIENTIFIC,
  NEED_DATE_RESEARCH_STRATEGY.SPORTS_ENTERTAINMENT,
  NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS,
  NEED_DATE_RESEARCH_STRATEGY.GOVERNMENT_PUBLIC_SECTOR,
  NEED_DATE_RESEARCH_STRATEGY.CORPORATE_CELEBRATIONS,
  NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS,
]);

/** Strategies that primarily surface accounts / intermediaries / partnerships. */
export const ACCOUNT_RELATIONSHIP_STRATEGY_IDS = Object.freeze([
  NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES,
  NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS,
  NEED_DATE_RESEARCH_STRATEGY.PREFERRED_LODGING,
  NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE,
  NEED_DATE_RESEARCH_STRATEGY.PROJECTS_INFRASTRUCTURE,
  NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY,
]);

/**
 * Classify a candidate for commercial accessibility (offline / post-research).
 * Speculative overflow is never sales-ready.
 */
export function classifyNeedDateDemandAccessibility({
  competingHostNamed = false,
  competingHotelBlockNamed = false,
  overflowEvidence = false,
  sourcingOpenEvidence = false,
  identifiableBuyer = false,
  timingAdequateForNeedMonth = false,
  partnershipPath = false,
} = {}) {
  if (competingHostNamed || competingHotelBlockNamed) {
    if (overflowEvidence) {
      return {
        class: NEED_DATE_DEMAND_CLASS.EVIDENCED_OVERFLOW_OPPORTUNITY,
        salesReadyEligible: timingAdequateForNeedMonth && identifiableBuyer,
        note: "Competing host exists; overflow supported by evidence.",
      };
    }
    return {
      class: NEED_DATE_DEMAND_CLASS.COMPETITOR_COMMITTED,
      secondaryClass: NEED_DATE_DEMAND_CLASS.SPECULATIVE_OVERFLOW,
      salesReadyEligible: false,
      note: "Primary business committed elsewhere; overflow not evidenced.",
    };
  }
  if (partnershipPath && !timingAdequateForNeedMonth) {
    return {
      class: NEED_DATE_DEMAND_CLASS.PARTNERSHIP_OR_REFERRAL_DEMAND,
      salesReadyEligible: false,
      researchContinuationEligible: true,
      note: "Relationship demand — continue research; not a verified need-date match yet.",
    };
  }
  if (timingAdequateForNeedMonth && identifiableBuyer && sourcingOpenEvidence) {
    return {
      class: NEED_DATE_DEMAND_CLASS.DIRECT_ADDRESSABLE_DEMAND,
      salesReadyEligible: true,
      note: "Direct demand with buyer and open/contestable sourcing.",
    };
  }
  if (timingAdequateForNeedMonth && identifiableBuyer) {
    return {
      class: NEED_DATE_DEMAND_CLASS.OPEN_OR_UNCONFIRMED_SOURCING,
      salesReadyEligible: false,
      researchContinuationEligible: true,
      note: "Unconfirmed sourcing is not available inventory.",
    };
  }
  return {
    class: NEED_DATE_DEMAND_CLASS.OPEN_OR_UNCONFIRMED_SOURCING,
    salesReadyEligible: false,
    researchContinuationEligible: true,
    note: "Insufficient evidence for direct sales-ready classification.",
  };
}

/**
 * Build dual-track research plan for a need-date campaign (no spend).
 */
export function buildNeedDateDualTrackResearchPlan({
  strategies = [],
  includeWeddingSocial = true,
} = {}) {
  const byId = new Map((strategies || []).map((s) => [s.strategyId, s]));

  const mapTrack = (ids, track) =>
    ids
      .map((id) => {
        const row = byId.get(id) || {
          strategyId: id,
          workflowNumber: NEED_DATE_STRATEGY_META[id]?.number,
          powershell: NEED_DATE_STRATEGY_META[id]?.powershell,
          relevance: "UNKNOWN",
        };
        return {
          track,
          strategyId: id,
          workflowNumber: row.workflowNumber,
          powershell: row.powershell,
          relevance: row.relevance,
          pursueInContinuation: row.pursueInContinuation,
          countsAsVerifiedNeedDateMatchOnlyWithTiming: track === NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
          mayEnterResearchContinuationWithoutDates:
            track === NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
        };
      })
      .filter((r) => includeWeddingSocial || r.strategyId !== NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS);

  return {
    version: "gdi-need-date-research-track-planning-v1",
    tracks: {
      [NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND]: {
        label: "Date-specific demand",
        rule: "Verified need-date match requires adequate timing evidence for the target month(s).",
        strategies: mapTrack(DATE_SPECIFIC_STRATEGY_IDS, NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND),
      },
      [NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND]: {
        label: "Account / relationship demand",
        rule: "Promising accounts, planners and partnerships may enter Research Continuation without established dates; they do not count toward verified need-date matches until timing is adequate.",
        strategies: mapTrack(
          ACCOUNT_RELATIONSHIP_STRATEGY_IDS,
          NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
        ),
      },
    },
    weddingSocialNote:
      "Weddings & Social Groups = canonical segment #8 (Invoke-GdiSocialSpecialEvents.ps1). Corporate Celebrations discovery is NOT wedding research. Preferred Lodging / Professional Event Planners are Segment-8 subtypes (PS #13/#14), not additional canonical segments.",
    overflowRule:
      "Speculative overflow is Watch-only. Do not classify competing-hotel event pages as sales-ready without evidenced overflow or open contestable sourcing.",
    paidExecution: false,
  };
}
