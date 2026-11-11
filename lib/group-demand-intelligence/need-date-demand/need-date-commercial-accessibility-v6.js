/**
 * Shared commercial-accessibility assessment for Need-Date Deep Discovery V6.
 */

import {
  NEED_DATE_V6_ACCESSIBILITY,
  NEED_DATE_V6_SALES_READY_ACCESSIBILITY,
  NEED_DATE_V6_CONTINUATION_ACCESSIBILITY,
  NEED_DATE_V6_WATCH_ONLY_ACCESSIBILITY,
} from "./need-date-deep-discovery-v6-contract.js";

/**
 * Demand-owner-first accessibility classification.
 * Competing host alone ≠ available opportunity.
 */
export function assessCommercialAccessibilityV6({
  competingHostNamed = false,
  competingHotelBlockNamed = false,
  overflowEvidence = false,
  sourcingOpenOrContestableEvidence = false,
  identifiableBuyerOrReferral = false,
  timingAdequateForNeedMonth = false,
  verifiedPartnershipPath = false,
  demandOwnerVerified = false,
} = {}) {
  const competitorLocked = competingHostNamed || competingHotelBlockNamed;

  if (competitorLocked) {
    if (overflowEvidence && identifiableBuyerOrReferral) {
      return {
        class: NEED_DATE_V6_ACCESSIBILITY.EVIDENCED_OVERFLOW,
        salesReadyEligible:
          timingAdequateForNeedMonth &&
          NEED_DATE_V6_SALES_READY_ACCESSIBILITY.includes(
            NEED_DATE_V6_ACCESSIBILITY.EVIDENCED_OVERFLOW,
          ),
        researchContinuationEligible: true,
        note: "Competing host exists; overflow supported by evidence.",
      };
    }
    return {
      class: NEED_DATE_V6_ACCESSIBILITY.COMPETITOR_COMMITTED,
      secondaryClass: NEED_DATE_V6_ACCESSIBILITY.SPECULATIVE_OVERFLOW,
      salesReadyEligible: false,
      researchContinuationEligible: false,
      watchOnly: true,
      note: "Primary business committed elsewhere; speculative overflow is Watch-only.",
    };
  }

  if (verifiedPartnershipPath && !timingAdequateForNeedMonth) {
    return {
      class: NEED_DATE_V6_ACCESSIBILITY.TIMING_UNCONFIRMED_RELATIONSHIP,
      salesReadyEligible: false,
      researchContinuationEligible: true,
      countsTowardVerifiedNeedMonthYield: false,
      note: "Relationship demand — Continuation only until timing is adequate.",
    };
  }

  if (verifiedPartnershipPath && timingAdequateForNeedMonth && identifiableBuyerOrReferral) {
    return {
      class: NEED_DATE_V6_ACCESSIBILITY.VERIFIED_PARTNERSHIP_REFERRAL,
      salesReadyEligible: true,
      researchContinuationEligible: true,
      note: "Verified partnership/referral path with timing.",
    };
  }

  if (
    demandOwnerVerified &&
    timingAdequateForNeedMonth &&
    identifiableBuyerOrReferral &&
    sourcingOpenOrContestableEvidence
  ) {
    return {
      class: NEED_DATE_V6_ACCESSIBILITY.DIRECT_ADDRESSABLE_DEMAND,
      salesReadyEligible: true,
      researchContinuationEligible: true,
      note: "Direct demand with buyer and open/contestable sourcing.",
    };
  }

  if (demandOwnerVerified && timingAdequateForNeedMonth && identifiableBuyerOrReferral) {
    return {
      class: NEED_DATE_V6_ACCESSIBILITY.CREDIBLY_CONTESTABLE_SOURCING,
      salesReadyEligible: true,
      researchContinuationEligible: true,
      note: "Sourcing contestable with evidence — keep uncertainty explicit; not assumed available inventory.",
    };
  }

  if (verifiedPartnershipPath || (demandOwnerVerified && identifiableBuyerOrReferral)) {
    return {
      class: NEED_DATE_V6_ACCESSIBILITY.TIMING_UNCONFIRMED_RELATIONSHIP,
      salesReadyEligible: false,
      researchContinuationEligible: true,
      countsTowardVerifiedNeedMonthYield: false,
      note: "Promising relationship/account — timing investigation required.",
    };
  }

  return {
    class: NEED_DATE_V6_ACCESSIBILITY.SPECULATIVE_OVERFLOW,
    salesReadyEligible: false,
    researchContinuationEligible: false,
    watchOnly: true,
    note: "Insufficient demand-owner / accessibility evidence.",
  };
}

export function isSalesReadyAccessibility(cls) {
  return NEED_DATE_V6_SALES_READY_ACCESSIBILITY.includes(cls);
}

export function isWatchOnlyAccessibility(cls) {
  return NEED_DATE_V6_WATCH_ONLY_ACCESSIBILITY.includes(cls);
}

export function isContinuationAccessibility(cls) {
  return NEED_DATE_V6_CONTINUATION_ACCESSIBILITY.includes(cls);
}
