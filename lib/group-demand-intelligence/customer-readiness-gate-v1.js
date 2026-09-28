/**
 * Global GDI customer-readiness gate (hotel-agnostic).
 * Converges identity/timing/entity + summary quality + WHO research attempted.
 */

import { isCustomerSurfaceActiveEligible } from "./customer-surface-revalidation-v1.js";
import {
  SUMMARY_QUALITY,
  evaluateGdiSummaryQuality,
  SUMMARY_INSUFFICIENT,
} from "./opportunity-summary-v1.js";
import {
  whoResearchAttempted,
  classifyWhoHowPath,
  WHO_PATH_CLASS,
} from "./opportunity-who-resolution-v1.js";

export const READINESS_HOLD = Object.freeze({
  READY: "READY",
  HELD_FOR_SUMMARY: "HELD_FOR_SUMMARY",
  HELD_FOR_WHO_RESEARCH: "HELD_FOR_WHO_RESEARCH",
  HELD_FOR_EVIDENCE: "HELD_FOR_EVIDENCE",
  DQ: "DQ",
});

/**
 * Consolidated readiness for customer-visible promotion.
 * Unknown-after-research is valid; not-researched is not.
 */
export function isGdiCustomerOpportunityReady(opp = {}, opts = {}) {
  const failed = [];
  const holds = [];

  if (!isCustomerSurfaceActiveEligible(opp, opts)) {
    failed.push("surface_eligibility");
    return {
      ok: false,
      state: READINESS_HOLD.DQ,
      failed,
      holds,
      summaryQuality: evaluateGdiSummaryQuality(opp).quality,
      whoPathClass: classifyWhoHowPath(opp).pathClass,
    };
  }

  if (!(opp.title || opp.opportunityName)) failed.push("title");
  if (!opp.organizationName && !opp.company) failed.push("organization");

  const hasSource =
    opp.officialSource ||
    opp.discoverySource ||
    (Array.isArray(opp.sources) && opp.sources.some((s) => s?.url || typeof s === "string"));
  if (!hasSource) {
    failed.push("source");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }

  const summaryEval = evaluateGdiSummaryQuality(opp);
  if (
    summaryEval.quality === SUMMARY_QUALITY.THIN ||
    summaryEval.quality === SUMMARY_QUALITY.INVALID ||
    opp.summaryInsufficient === true
  ) {
    failed.push("summary_quality");
    holds.push(READINESS_HOLD.HELD_FOR_SUMMARY);
  }

  if (!whoResearchAttempted(opp)) {
    failed.push("who_research_not_attempted");
    holds.push(READINESS_HOLD.HELD_FOR_WHO_RESEARCH);
  }

  // Hotel fit / why-now / action — required for promotion depth; unknown-after-research OK if stamped
  if (opp.hotelFitScore == null && opp.hotelFitScore !== 0 && !opp.summaryWhyHotel && !opp.fitExplanation) {
    failed.push("hotel_fit");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }
  if (!opp.whyNow && !opp.cardWhyNowLine) {
    failed.push("why_now");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }
  if (!opp.recommendedAction && !opp.recommendedNextStep) {
    failed.push("recommended_action");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }

  const who = classifyWhoHowPath(opp);
  const ok = failed.length === 0;
  let state = READINESS_HOLD.READY;
  if (!ok) {
    if (holds.includes(READINESS_HOLD.HELD_FOR_SUMMARY)) state = READINESS_HOLD.HELD_FOR_SUMMARY;
    else if (holds.includes(READINESS_HOLD.HELD_FOR_WHO_RESEARCH)) {
      state = READINESS_HOLD.HELD_FOR_WHO_RESEARCH;
    } else state = READINESS_HOLD.HELD_FOR_EVIDENCE;
  }

  return {
    ok,
    state,
    failed,
    holds: [...new Set(holds)],
    summaryQuality: summaryEval.quality,
    whoPathClass: who.pathClass,
    summaryInsufficientCode: SUMMARY_INSUFFICIENT,
  };
}

/**
 * Soft gate for existing bag rows: surface eligibility + summary not INVALID.
 * THIN existing rows may remain visible during backfill window if already promoted,
 * but new promotions must use isGdiCustomerOpportunityReady.
 */
export function passesCustomerSummaryGate(opp = {}) {
  const q = evaluateGdiSummaryQuality(opp).quality;
  return q === SUMMARY_QUALITY.STRONG || q === SUMMARY_QUALITY.ADEQUATE;
}

export { SUMMARY_QUALITY, WHO_PATH_CLASS };
