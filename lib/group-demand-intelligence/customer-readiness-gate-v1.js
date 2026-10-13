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
import { meetsReadyContactRequirement } from "./buyer-contact-path-taxonomy-v1.js";
import { meetsReadyAccountRequirement } from "./account-quality-taxonomy-v1.js";

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

  // Commercial contact path — SOURCE_PAGE / GENERAL_ORG alone are not READY
  const contactReq = meetsReadyContactRequirement(opp);
  if (!contactReq.ok) {
    failed.push("buyer_contact_path_insufficient");
    holds.push(READINESS_HOLD.HELD_FOR_WHO_RESEARCH);
  }

  // Account quality — venue/generator/contact shells are not READY sales targets
  const accountReq = meetsReadyAccountRequirement(opp);
  if (!accountReq.ok) {
    failed.push("account_quality_insufficient");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }

  // Cautionary Why Now copy may only veto when it maps to a REAL missing structured pillar.
  // Structured canonical evidence wins over overcautious templates (final-mile contract).
  const whyNowBlob = `${opp.whyNow || ""} ${opp.cardWhyNowLine || ""}`;
  const whyNowCautionary =
    /qualify (?:buyer\/)?housing before selling as ready|verify whether this is an opportunity|buyer unknown|no usable contact path|confirm lodging path and buyer function before outreach|confirm lodging evidence and buyer-function path before customer-ready/i.test(
      whyNowBlob
    );
  if (whyNowCautionary) {
    const lodgingStructured =
      opp.travelingEntityProven === true ||
      Boolean(opp.hotelMotionClass && !/^(NONE|UNCONFIRMED|UNKNOWN)$/i.test(String(opp.hotelMotionClass))) ||
      /WEAK|STRONG|CREDIBLE|VERIFIED/i.test(String(opp.housingStatus || "")) ||
      (typeof opp.lodgingEvidence === "string" &&
        opp.lodgingEvidence.length > 8 &&
        !/^(unknown|none|missing|n\/?a)$/i.test(opp.lodgingEvidence.trim()));
    const buyerStructured = contactReq.ok === true;
    const travelingStructured =
      opp.travelingEntityProven === true ||
      Boolean(opp.groupMotionType && opp.groupMotionEvidence);
    // Only veto when copy correctly signals a still-missing pillar
    if (!lodgingStructured || !buyerStructured || !travelingStructured) {
      failed.push("why_now_contradicts_ready");
      holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
    }
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
    buyerContactPathClass: contactReq.class,
    buyerContactPathReason: contactReq.reason,
    accountQualityClass: accountReq.class,
    accountQualityReason: accountReq.reason,
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

/**
 * Maturity Funnel V1 — ACTIONABLE ≡ strict Ready gate.
 * Alias preserved for callers; do not lower this bar.
 */
export function isGdiMaturityActionable(opp = {}, opts = {}) {
  return isGdiCustomerOpportunityReady(opp, opts);
}

export { SUMMARY_QUALITY, WHO_PATH_CLASS };
