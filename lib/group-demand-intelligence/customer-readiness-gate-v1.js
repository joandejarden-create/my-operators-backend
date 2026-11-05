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
import { extractContactSurface } from "./prospect-publication-eligibility-v1.js";

/**
 * Same nested-contact contract as eligibility — Ready must not ignore contact.*.
 */
function withNormalizedResearchContact(opp = {}) {
  const s = extractContactSurface(opp);
  if (!s.email && !s.phone && !s.contactUrl && !s.person) return opp;
  return {
    ...opp,
    primaryContactEmail: opp.primaryContactEmail || s.email || undefined,
    primaryContactPhone: opp.primaryContactPhone || s.phone || undefined,
    contactEmail: opp.contactEmail || s.email || undefined,
    contactUrl: opp.contactUrl || s.contactUrl || undefined,
    publicContactPath: opp.publicContactPath || s.contactUrl || undefined,
    organizationContactUrl: opp.organizationContactUrl || s.contactUrl || undefined,
    primaryContact: {
      ...(opp.primaryContact || {}),
      name: opp.primaryContact?.name || s.person || undefined,
      email: opp.primaryContact?.email || s.email || undefined,
      phone: opp.primaryContact?.phone || s.phone || undefined,
      role: opp.primaryContact?.role || s.title || undefined,
      organizationContactRoute:
        opp.primaryContact?.organizationContactRoute || s.contactUrl || undefined,
    },
  };
}

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
  // Normalize nested E2D contact onto the same surface eligibility/Ready share.
  const bag = withNormalizedResearchContact(opp);

  if (!isCustomerSurfaceActiveEligible(bag, opts)) {
    failed.push("surface_eligibility");
    return {
      ok: false,
      state: READINESS_HOLD.DQ,
      failed,
      holds,
      summaryQuality: evaluateGdiSummaryQuality(bag).quality,
      whoPathClass: classifyWhoHowPath(bag).pathClass,
    };
  }

  if (!(bag.title || bag.opportunityName)) failed.push("title");
  if (!bag.organizationName && !bag.company) failed.push("organization");

  const hasSource =
    bag.officialSource ||
    bag.discoverySource ||
    (Array.isArray(bag.sources) && bag.sources.some((s) => s?.url || typeof s === "string"));
  if (!hasSource) {
    failed.push("source");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }

  const summaryEval = evaluateGdiSummaryQuality(bag);
  if (
    summaryEval.quality === SUMMARY_QUALITY.THIN ||
    summaryEval.quality === SUMMARY_QUALITY.INVALID ||
    bag.summaryInsufficient === true
  ) {
    failed.push("summary_quality");
    holds.push(READINESS_HOLD.HELD_FOR_SUMMARY);
  }

  if (!whoResearchAttempted(bag)) {
    failed.push("who_research_not_attempted");
    holds.push(READINESS_HOLD.HELD_FOR_WHO_RESEARCH);
  }

  // Commercial contact path — SOURCE_PAGE / GENERAL_ORG alone are not READY
  const contactReq = meetsReadyContactRequirement(bag);
  if (!contactReq.ok) {
    failed.push("buyer_contact_path_insufficient");
    holds.push(READINESS_HOLD.HELD_FOR_WHO_RESEARCH);
  }

  // Account quality — venue/generator/contact shells are not READY sales targets
  const accountReq = meetsReadyAccountRequirement(bag);
  if (!accountReq.ok) {
    failed.push("account_quality_insufficient");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }

  // Cautionary Why Now copy may only veto when it maps to a REAL missing structured pillar.
  // Structured canonical evidence wins over overcautious templates (final-mile contract).
  const whyNowBlob = `${bag.whyNow || ""} ${bag.cardWhyNowLine || ""}`;
  const whyNowCautionary =
    /qualify (?:buyer\/)?housing before selling as ready|verify whether this is an opportunity|buyer unknown|no usable contact path|confirm lodging path and buyer function before outreach|confirm lodging evidence and buyer-function path before customer-ready/i.test(
      whyNowBlob
    );
  if (whyNowCautionary) {
    const lodgingStructured =
      bag.travelingEntityProven === true ||
      Boolean(bag.hotelMotionClass && !/^(NONE|UNCONFIRMED|UNKNOWN)$/i.test(String(bag.hotelMotionClass))) ||
      /WEAK|STRONG|CREDIBLE|VERIFIED/i.test(String(bag.housingStatus || "")) ||
      (typeof bag.lodgingEvidence === "string" &&
        bag.lodgingEvidence.length > 8 &&
        !/^(unknown|none|missing|n\/?a)$/i.test(bag.lodgingEvidence.trim()));
    const buyerStructured = contactReq.ok === true;
    const travelingStructured =
      bag.travelingEntityProven === true ||
      Boolean(bag.groupMotionType && bag.groupMotionEvidence);
    // Only veto when copy correctly signals a still-missing pillar
    if (!lodgingStructured || !buyerStructured || !travelingStructured) {
      failed.push("why_now_contradicts_ready");
      holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
    }
  }

  // Hotel fit — same research fields as eligibility (whyThisHotel is equivalent
  // evidence when summaryWhyHotel / hotelFitRationale aliases were not persisted).
  if (
    bag.hotelFitScore == null &&
    bag.hotelFitScore !== 0 &&
    !bag.summaryWhyHotel &&
    !bag.fitExplanation &&
    !bag.hotelFitRationale &&
    !bag.whyThisHotel &&
    !bag.hotelOpportunityThesis
  ) {
    failed.push("hotel_fit");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }
  if (!bag.whyNow && !bag.cardWhyNowLine) {
    failed.push("why_now");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }
  if (!bag.recommendedAction && !bag.recommendedNextStep) {
    failed.push("recommended_action");
    holds.push(READINESS_HOLD.HELD_FOR_EVIDENCE);
  }

  const who = classifyWhoHowPath(bag);
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
