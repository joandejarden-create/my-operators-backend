/**
 * HIF V6 — First 100 commercial opportunities milestone gate.
 * Counts only evidence-backed near-term development opportunities.
 * Expiration-only Priority B proxies do NOT count.
 */

import { DECISION_WINDOW_KIND } from "./hif-v6-commercial-timing-model-v6.js";
import { COMMERCIAL_PRIORITY } from "./hif-v6-commercial-actionability-v6.js";
import { GEO_CLASS } from "./hif-v6-geographic-confidence-v6.js";

export const HIF_V6_FIRST_100_MILESTONE = "hif-v6-first-100-milestone-v6";
export const FIRST_100_TARGET = 100;

/**
 * Strict qualification for the first-100 near-term commercial milestone.
 * Requires credible prospective/active commercial decision — not expiration proxy alone.
 */
export function qualifiesForFirst100Milestone(row = {}) {
  const reasons = [];
  const fail = (code) => ({ qualifies: false, reasons: [...reasons, code] });

  if (!row.hotelName || String(row.hotelName).trim().length < 3) {
    return fail("MISSING_HOTEL_IDENTITY");
  }
  if (/maturity|turity date|related mortgage|loan number|cut-?off|appraised/i.test(String(row.hotelName))) {
    return fail("NOISY_HOTEL_IDENTITY");
  }

  const geoClass = row.geographicClass || row.geographicConfidence;
  if (geoClass === GEO_CLASS.UNRESOLVED) return fail("PHYSICAL_COUNTRY_UNRESOLVED");
  if (!row.physicalCountry && !row.hotelCountry) return fail("PHYSICAL_COUNTRY_MISSING");

  const agr = String(row.agreementType || row.agreementTypeConfidence || "");
  const list = String(row.commercialList || "");
  const franchiseOrMgmt =
    /FRANCHISE|MANAGEMENT|OPERATING/i.test(agr) ||
    /BRAND_CONVERSION_TARGETS|MANAGEMENT_CONTRACT_TARGETS/.test(list);
  if (!franchiseOrMgmt || row.agreementTypeResolved === false) {
    return fail("AGREEMENT_TYPE_NOT_EVIDENCE_SUPPORTED");
  }
  if (/UNRESOLVED|UNKNOWN|COMBINED|MULTIPLE/i.test(agr) && !/BRAND_CONVERSION_TARGETS|MANAGEMENT_CONTRACT_TARGETS/.test(list)) {
    return fail("AGREEMENT_TYPE_UNRESOLVED");
  }

  const windowKind = row.decisionWindowKind;
  if (windowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY) {
    return fail("EXPIRATION_ONLY_PROXY_EXCLUDED");
  }
  if (windowKind === DECISION_WINDOW_KIND.TIMING_UNKNOWN || !windowKind) {
    return fail("NO_CREDIBLE_DECISION_WINDOW");
  }
  if (
    windowKind !== DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW &&
    windowKind !== DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW
  ) {
    return fail("DECISION_WINDOW_NOT_CREDIBLE");
  }

  const months = row.monthsToDecision;
  if (months === null || months === undefined || months < 0 || months >= 24) {
    return fail("OUTSIDE_24M_WINDOW");
  }

  if (row.superseded === true || row.status === "HISTORICAL_OR_SUPERSEDED") {
    return fail("SUPERSEDED_OR_HISTORICAL");
  }
  if (row.commercialPriority === COMMERCIAL_PRIORITY.HOLD) {
    return fail("HOLD_INSUFFICIENT_EVIDENCE");
  }

  // Priority A always qualifies when above gates pass; Priority B only with confirmed/inferred window (not expiration)
  if (
    row.commercialPriority !== COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW &&
    row.commercialPriority !== COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH
  ) {
    return fail("PRIORITY_NOT_A_OR_QUALIFIED_B");
  }

  reasons.push("MILESTONE_QUALIFIED");
  return {
    qualifies: true,
    reasons,
    opportunityId:
      row.canonicalOpportunityId ||
      row.key ||
      `${String(row.hotelName).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim()}|${row.physicalCountry || row.hotelCountry}|${row.nextCommercialDecisionDate || row.expirationDate || "NODATE"}`,
  };
}

export function buildFirst100Scorecard(candidates = [], asOf) {
  const qualified = [];
  const rejected = [];
  const seen = new Set();

  for (const row of candidates) {
    const gate = qualifiesForFirst100Milestone(row);
    if (!gate.qualifies) {
      rejected.push({ hotelName: row.hotelName, key: row.key, reasons: gate.reasons });
      continue;
    }
    if (seen.has(gate.opportunityId)) {
      rejected.push({ hotelName: row.hotelName, key: row.key, reasons: ["DUPLICATE_OPPORTUNITY_ID"] });
      continue;
    }
    seen.add(gate.opportunityId);
    qualified.push({ ...row, ...gate });
  }

  const byCountry = {};
  const byTiming = {};
  const byType = {};
  const byDirection = {};
  for (const q of qualified) {
    const c = q.physicalCountry || q.hotelCountry || "UNATTRIBUTED";
    byCountry[c] = (byCountry[c] || 0) + 1;
    const t = q.timingHorizon || "UNKNOWN";
    byTiming[t] = (byTiming[t] || 0) + 1;
    const typ = q.commercialList || q.agreementType || "OTHER";
    byType[typ] = (byType[typ] || 0) + 1;
    const d = q.researchDirection || "OTHER_UNKNOWN";
    byDirection[d] = (byDirection[d] || 0) + 1;
  }

  return {
    asOf,
    target: FIRST_100_TARGET,
    qualifiedNearTermOpportunities: qualified.length,
    progressToward100: `${qualified.length}/${FIRST_100_TARGET}`,
    remaining: Math.max(0, FIRST_100_TARGET - qualified.length),
    note: "Expiration-only Priority B proxies are excluded from the milestone count",
    byCountry,
    byTiming,
    byOpportunityType: byType,
    byResearchDirection: byDirection,
    qualified,
    rejectedSample: rejected.slice(0, 25),
    rejectedCount: rejected.length,
  };
}
