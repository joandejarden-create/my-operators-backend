/**
 * Targeted next-best research — one action for promising near-misses.
 * Does not lower gates; only proposes / optionally executes a single research step.
 */

import { classifyEntityTruth } from "../entity-truth-gate-v1.js";
import { classifyActiveDate, ACTIVE_DATE_CLASS } from "../active-eligibility-v1.js";
import { classifyWhoHowPath, WHO_PATH_CLASS } from "../opportunity-who-resolution-v1.js";

export const RESEARCH_BLOCKER = Object.freeze({
  TIMING: "TIMING",
  LODGING: "LODGING",
  WHO: "WHO",
  CONTACT_PATH: "CONTACT_PATH",
  VENUE: "VENUE",
  OVERFLOW: "OVERFLOW",
  ORGANIZER: "ORGANIZER",
  NONE: "NONE",
});

function lodgingMissing(opp = {}) {
  const le = opp.lodgingEvidence || opp.lodging || null;
  if (!le) return true;
  if (typeof le === "object" && (le.roomBlockMentioned || le.housingPageFound)) return false;
  return true;
}

function timingWeak(opp = {}) {
  const d = classifyActiveDate(opp, {});
  if (d.activeDateClass === ACTIVE_DATE_CLASS.ACTIVE_FUTURE) return false;
  if (d.activeDateClass === ACTIVE_DATE_CLASS.ACTIVE_CURRENT) return false;
  if (d.activeDateClass === ACTIVE_DATE_CLASS.PAST_WITH_VALID_FUTURE_MOTION) return false;
  return true;
}

function whoWeak(opp = {}) {
  const w = classifyWhoHowPath(opp);
  return w.pathClass === WHO_PATH_CLASS.NOT_RESEARCHED;
}

function contactWeak(opp = {}) {
  const w = classifyWhoHowPath(opp);
  return (
    w.pathClass === WHO_PATH_CLASS.NOT_RESEARCHED ||
    w.pathClass === WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH
  );
}

function hotelFitOk(opp = {}) {
  const score = opp.hotelFitScore ?? opp.hotelFit;
  if (score != null && Number(score) >= 40) return true;
  const thesis = String(opp.hotelOpportunityThesis || opp.summaryWhyHotel || "").trim();
  return thesis.length >= 24;
}

function geoOk(opp = {}, marketTokens = []) {
  if (!marketTokens.length) return true;
  // Market-scoped discovery provenance counts (query was already place-bound)
  if (opp.discoveryMeta?.marketPlace || opp.eventLocationSummary || opp.destinationStatus) {
    const scoped = String(
      opp.discoveryMeta?.marketPlace || opp.eventLocationSummary || opp.destinationStatus || ""
    ).toLowerCase();
    if (marketTokens.some((t) => scoped.includes(String(t).toLowerCase()))) return true;
  }
  const blob = [
    opp.eventLocationSummary,
    opp.eventLocation,
    opp.destinationStatus,
    opp.venueStatus,
    opp.title,
    opp.organizationName,
    opp.officialSource,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");
  return marketTokens.some((t) => blob.includes(String(t).toLowerCase()));
}

/**
 * True when candidate is promising enough for exactly one next research step.
 */
export function isPromisingNearMiss(opp = {}, opts = {}) {
  const entity = classifyEntityTruth(opp);
  if (!entity.validEntity) return false;
  if (!hotelFitOk(opp)) return false;
  if (!geoOk(opp, opts.marketTokens || [])) return false;

  const blockers = [];
  if (timingWeak(opp)) blockers.push(RESEARCH_BLOCKER.TIMING);
  if (lodgingMissing(opp)) blockers.push(RESEARCH_BLOCKER.LODGING);
  if (whoWeak(opp)) blockers.push(RESEARCH_BLOCKER.WHO);
  else if (contactWeak(opp)) blockers.push(RESEARCH_BLOCKER.CONTACT_PATH);

  // Exactly one (or WHO+CONTACT counted as one family) — allow 1–2 related contact blockers
  const distinct = new Set(blockers.map((b) => (b === RESEARCH_BLOCKER.CONTACT_PATH ? RESEARCH_BLOCKER.WHO : b)));
  if (distinct.size === 0) return false;
  if (distinct.size > 1 && !(distinct.size === 2 && distinct.has(RESEARCH_BLOCKER.LODGING) && distinct.has(RESEARCH_BLOCKER.WHO))) {
    // Allow lodging+who as still promising; reject 3+ distinct families
    if (distinct.size >= 3) return false;
  }
  return true;
}

/**
 * @returns {{ blocker, exactQuestion, bestSourceFamily, expectedInformationGain, estimatedCostUsd, promotionCondition, query }}
 */
export function getGdiNextBestResearchAction(candidate = {}, opts = {}) {
  const opp = candidate.opportunity || candidate;
  const place = opts.marketPlace || opts.place || "";

  if (timingWeak(opp)) {
    return {
      blocker: RESEARCH_BLOCKER.TIMING,
      exactQuestion: `What is the next confirmed or expected cycle date/range for "${opp.title || opp.organizationName}"?`,
      bestSourceFamily: "OFFICIAL_EVENT_OR_ASSOCIATION_PAGE",
      expectedInformationGain: "HIGH",
      estimatedCostUsd: 0.05,
      promotionCondition: "CONFIRMED_EXACT|CONFIRMED_RANGE|CONFIRMED_MONTH|CONFIRMED_YEAR with evidence URL",
      query: `"${opp.organizationName || opp.title}" ${place} 2026 OR 2027 OR 2028 dates OR "next" OR annual`,
    };
  }
  if (lodgingMissing(opp)) {
    return {
      blocker: RESEARCH_BLOCKER.LODGING,
      exactQuestion: `Is there an official housing page, host hotel, room block, or accommodation tender for "${opp.title || opp.organizationName}"?`,
      bestSourceFamily: "HOUSING_OR_ACCOMMODATION_PAGE",
      expectedInformationGain: "HIGH",
      estimatedCostUsd: 0.05,
      promotionCondition: "roomBlockMentioned|housingPageFound stamped from official URL",
      query: `"${opp.organizationName || opp.title}" (housing OR accommodation OR "hotel block" OR "host hotel" OR "room block") ${place}`,
    };
  }
  if (whoWeak(opp) || contactWeak(opp)) {
    return {
      blocker: whoWeak(opp) ? RESEARCH_BLOCKER.WHO : RESEARCH_BLOCKER.CONTACT_PATH,
      exactQuestion: `Who is the meetings / housing / exhibitions contact for "${opp.organizationName || opp.title}", or what is the official contact path?`,
      bestSourceFamily: "ORGANIZER_CONTACT_OR_STAFF_PAGE",
      expectedInformationGain: "MEDIUM",
      estimatedCostUsd: 0.05,
      promotionCondition: "NAMED_*|FUNCTIONAL|ORG_PATH|PUBLIC_DATA_CEILING stamped",
      query: `"${opp.organizationName || opp.title}" (contact OR "meeting planner" OR exhibitions OR housing OR "commercial director")`,
    };
  }
  if (!opp.venueStatus || /^UNKNOWN$/i.test(String(opp.venueStatus))) {
    return {
      blocker: RESEARCH_BLOCKER.VENUE,
      exactQuestion: `Has a primary venue been selected for "${opp.title}"?`,
      bestSourceFamily: "OFFICIAL_EVENT_PAGE",
      expectedInformationGain: "MEDIUM",
      estimatedCostUsd: 0.04,
      promotionCondition: "venueStatus stamped; overflow path assessed separately",
      query: `"${opp.title || opp.organizationName}" venue OR location ${place} 2026 OR 2027`,
    };
  }
  return {
    blocker: RESEARCH_BLOCKER.NONE,
    exactQuestion: null,
    bestSourceFamily: null,
    expectedInformationGain: "NONE",
    estimatedCostUsd: 0,
    promotionCondition: null,
    query: null,
  };
}
