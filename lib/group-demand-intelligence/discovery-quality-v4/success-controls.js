/**
 * Success control set — Bethesda / Renaissance NYTS / Hilton NYC ready opportunities.
 * Capture READY-TIME structural pattern (fields present when ready), not later enrichment guesses.
 */

import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";

export const CONTROL_HOTELS = Object.freeze([
  { hotelId: "recLuxvwwxID7U2B8", hotelKey: "BETHESDA", label: "Bethesda Marriott" },
  { hotelId: "recG66DQJKP2c0UNh", hotelKey: "RENAISSANCE", label: "Renaissance NYTS" },
  { hotelId: "rec35fExUxCClpOP6", hotelKey: "HILTON_NYC", label: "Hilton NYC / Times Square market" },
]);

function structuralSnapshot(o = {}) {
  const thesis = String(o.hotelOpportunityThesis || o.bethesdaWinThesis || "");
  const lodging =
    Boolean(o.lodgingEvidence?.housingPageFound || o.lodgingEvidence?.roomBlockMentioned) ||
    /housing|accommodation|room block|overflow|stay-to-play|host hotel/i.test(
      `${thesis} ${o.officialSource || ""} ${o.discoverySource || ""}`
    );
  const who = Boolean(
    o.primaryContact?.email ||
      o.functionalContactEmail ||
      o.organizationContactUrl ||
      o.officialContactPath
  );
  const timing = Boolean(o.eventStartDate || o.eventYear || o.eventEndDate);
  const entity = Boolean(o.organizationName && String(o.organizationName).length > 3);
  const motion = /overflow|housing|room|primary|pitch|compete|stay-to-play|group/i.test(thesis);
  const authority = /\.(org|gov|edu|int)\b|marriott|hilton|association|society|federation/i.test(
    String(o.officialSource || o.discoverySource || "")
  );
  const sources = Array.isArray(o.sources) ? o.sources.length : o.officialSource ? 1 : 0;

  return {
    opportunityId: o.id,
    title: o.title,
    opportunityType: o.opportunityType,
    entityQuality: entity ? "STRONG" : "WEAK",
    timingQuality: timing ? (o.eventStartDate ? "EXACT_OR_RANGE" : "YEAR_OR_PARTIAL") : "WEAK",
    lodgingQuality: lodging ? "HINT_OR_STRUCTURED" : "WEAK",
    buyerWhoQuality: who ? "PATH_PRESENT" : "WEAK",
    placementQuality: o.venueStatus && !/^UNKNOWN$/i.test(String(o.venueStatus)) ? "PARTIAL" : "UNKNOWN",
    hotelFitQuality: Number(o.hotelFitScore || 0) >= 45 ? "PASS" : "WEAK",
    sourceAuthority: authority ? "CREDIBLE" : "UNKNOWN",
    independentSources: sources,
    hotelMotionSpecific: motion,
    eventProgramMaturity: o.opportunityType || "UNKNOWN",
    commercialActionability: who && timing && motion,
    eventStartDate: o.eventStartDate || null,
    organizationName: o.organizationName || null,
    officialSource: o.officialSource || o.discoverySource || null,
    thesisSnippet: thesis.slice(0, 160),
  };
}

export async function buildSuccessControlSet(opts = {}) {
  const nowDate = opts.nowDate || "2026-10-03";
  const rows = [];
  for (const h of CONTROL_HOTELS) {
    const doc = await loadOpportunitiesCanonical(h.hotelId);
    for (const o of doc.opportunities || []) {
      const s = { ...o };
      delete s.customerVisible;
      delete s.customerActiveEligible;
      delete s.customerSurfaceDisposition;
      if (s.priority === "DISQUALIFIED") s.priority = s.priorityBeforeSurfaceDq || "WATCHLIST";
      if (!isGdiCustomerOpportunityReady(s, { nowDate }).ok) continue;
      rows.push({
        hotelKey: h.hotelKey,
        hotelLabel: h.label,
        hotelId: h.hotelId,
        ...structuralSnapshot(o),
      });
    }
  }
  return rows;
}

/**
 * Aggregate pattern from controls vs a candidate.
 */
export function compareCandidateToControls(candidate = {}, controlAgg = {}) {
  const gaps = [];
  const blob = `${candidate.title || ""} ${candidate.organizationName || ""} ${candidate.officialSource || candidate.source || ""} ${candidate.hotelOpportunityThesis || ""}`;

  if (!candidate.organizationName || String(candidate.organizationName).length < 4) {
    gaps.push("ENTITY_WEAK");
  }
  if (!candidate.eventStartDate && !candidate.eventYear && !/202[6-9]|203[0-2]/.test(blob)) {
    gaps.push("TIMING_WEAK");
  }
  if (
    !candidate.lodgingEvidence &&
    !/housing|accommodation|room block|overflow|hébergement|alojamiento/i.test(blob)
  ) {
    gaps.push("LODGING_WEAK");
  }
  if (
    !candidate.primaryContact &&
    !candidate.functionalContactEmail &&
    !candidate.organizationContactUrl &&
    !/\/contact/i.test(String(candidate.officialSource || candidate.source || ""))
  ) {
    gaps.push("WHO_WEAK");
  }
  if (!candidate.venueStatus || /^UNKNOWN$/i.test(String(candidate.venueStatus))) {
    gaps.push("PLACEMENT_WEAK");
  }
  if (
    !candidate.hotelOpportunityThesis ||
    !/overflow|housing|room|pitch|compete|group|block/i.test(
      String(candidate.hotelOpportunityThesis || "")
    )
  ) {
    gaps.push("HOTEL_MOTION_WEAK");
  }
  if (!/\.(org|gov|edu|int)\b/i.test(String(candidate.officialSource || candidate.source || ""))) {
    gaps.push("SOURCE_AUTHORITY_WEAK");
  }
  if (!/\b(congress|conference|summit|meeting|tournament|rfp|housing|delegation)\b/i.test(blob)) {
    gaps.push("TOO_GENERIC");
  }
  if (!/\b(hotel|housing|accommodation|lodging|room|group|meeting|congress)\b/i.test(blob)) {
    gaps.push("NOT_ACTUALLY_GROUP_DEMAND");
  }
  if (!candidate.organizationName) gaps.push("NO_BUYER");
  if (!candidate.eventStartDate && !candidate.eventYear && !candidate.nextResearchTrigger) {
    gaps.push("NO_FUTURE_DECISION_POINT");
  }

  return {
    gaps,
    controlRequires: controlAgg,
    gapCount: gaps.length,
  };
}

export function summarizeControlPatterns(controls = []) {
  const n = controls.length || 1;
  return {
    controlCount: controls.length,
    pctEntityStrong: controls.filter((c) => c.entityQuality === "STRONG").length / n,
    pctTimingPresent: controls.filter((c) => c.timingQuality !== "WEAK").length / n,
    pctLodgingHint: controls.filter((c) => c.lodgingQuality !== "WEAK").length / n,
    pctWhoPath: controls.filter((c) => c.buyerWhoQuality === "PATH_PRESENT").length / n,
    pctHotelMotion: controls.filter((c) => c.hotelMotionSpecific).length / n,
    pctActionable: controls.filter((c) => c.commercialActionability).length / n,
    avgSources: controls.reduce((s, c) => s + (c.independentSources || 0), 0) / n,
    structuralDifferences:
      "Successful Bethesda/NYC ready rows almost always combine: (1) named organizer/buyer, (2) future date or cycle, (3) explicit hotel-motion thesis (overflow/housing/primary), (4) public contact or housing path, (5) market-credible venue/destination — often before deep completion. V3 SERP hits typically lack lodging+buyer+motion together.",
  };
}
