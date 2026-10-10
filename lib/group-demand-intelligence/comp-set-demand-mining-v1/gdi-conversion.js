/**
 * Convert validated competitive demand patterns → GDI candidates via canonical gates.
 * No manual promotion.
 */

import { isGdiResearchLeadWorthPursuing } from "../opportunity-discovery-v5/research-lead-gate.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { FIT_CLASS } from "./target-thesis.js";
import { resolveGdiDemandBuyer } from "../opportunity-discovery-v5/buyer-resolution.js";

/**
 * Pattern may become GDI candidate only with full structural requirements.
 */
export function patternEligibleForGdiCandidate(pattern = {}, thesis = {}, buyer = {}) {
  const missing = [];
  if (!pattern.organization) missing.push("ORGANIZATION");
  if (!pattern.historicHotels?.length && !pattern.historicHotels) missing.push("HISTORIC_COMP_USE");
  if (!pattern.nextExpectedCycle && !pattern.nextKnownCycle && !/ANNUAL|MULTI_YEAR|ROTAT/i.test(pattern.repeatCadence || "")) {
    missing.push("REPEAT_OR_FUTURE_THESIS");
  }
  if (!pattern.nextExpectedCycle && !pattern.nextKnownCycle && !pattern.nextDecisionWindow) {
    missing.push("FUTURE_DECISION_POINT");
  }
  if (!/LODGING|MENTIONED|PRESENT/i.test(String(pattern.lodgingPattern || pattern.roomBlock || ""))) {
    // still allow if evidence set has DIRECT/STRONG
    if (!/DIRECT_CONFIRMED|STRONG_ASSOCIATION/i.test(String(pattern.evidenceSet || ""))) {
      missing.push("PLAUSIBLE_LODGING_NEED");
    }
  }
  if (thesis.fitClass !== FIT_CLASS.STRONG_FIT && thesis.fitClass !== FIT_CLASS.PLAUSIBLE_FIT) {
    missing.push("TARGET_HOTEL_FIT");
  }
  if (!buyer.buyerEntity && !buyer.publicContactPath && !pattern.organizer) {
    missing.push("BUYER_ORGANIZER_PATH");
  }
  return { ok: missing.length === 0, missing };
}

export function patternToGdiDraft(pattern = {}, thesis = {}, buyer = {}, targetHotel = {}, sources = []) {
  const sourceUrl = String(pattern.sources || "").split("|")[0] || sources[0] || null;
  const lodgingHint = /LODGING|DIRECT|STRONG/i.test(
    `${pattern.lodgingPattern || ""} ${pattern.evidenceSet || ""}`
  );
  return {
    id: `gdi_comp_${pattern.patternId}`.slice(0, 96),
    title: pattern.eventProgram || `${pattern.organization} competitive demand`,
    organizationName: pattern.organization || buyer.buyerEntity,
    opportunityName: pattern.eventProgram || pattern.organization,
    opportunityType: lodgingHint ? "OVERFLOW_HOUSING" : "FUTURE_CYCLE",
    officialSource: sourceUrl,
    discoverySource: sourceUrl,
    sources: (String(pattern.sources || "").split("|").filter(Boolean) || []).map((url) => ({
      url,
      kind: "comp_set_demand_mining_v1",
    })),
    summaryWhat: thesis.fact || `Competitor-demand pattern for ${pattern.organization}`,
    hotelOpportunityThesis: [
      thesis.whyRelevant,
      thesis.whatTargetCouldWin,
      `Fit: ${thesis.fitClass}.`,
      thesis.inference ? `Inference: ${thesis.inference}` : "",
    ]
      .filter(Boolean)
      .join(" "),
    whyNow: thesis.nextSalesResearchAction,
    recommendedAction: thesis.nextSalesResearchAction,
    summaryWhyMatters: thesis.repeatFutureEvidence,
    summaryWhyHotel: targetHotel.hotelName || targetHotel.targetHotelName,
    hotelFitScore: thesis.fitClass === FIT_CLASS.STRONG_FIT
      ? Math.max(targetHotel.defaultFitScore || 50, 55)
      : targetHotel.defaultFitScore || 48,
    eventYear: String(pattern.nextKnownCycle || pattern.nextExpectedCycle || "").match(/^20\d{2}/)?.[0] || null,
    eventStartDate: /^\d{4}-\d{2}-\d{2}$/.test(String(pattern.nextKnownCycle || ""))
      ? pattern.nextKnownCycle
      : null,
    futureCycleEvidenceState: pattern.nextKnownCycle
      ? "CURRENT_FUTURE_CYCLE_CONFIRMED"
      : pattern.nextExpectedCycle
        ? "FUTURE_CYCLE_UNCONFIRMED"
        : null,
    lodgingEvidence: lodgingHint
      ? { housingPageFound: true, roomBlockMentioned: false, status: "WEAK" }
      : null,
    buyerEntity: buyer.buyerEntity,
    buyerType: buyer.buyerType,
    organizationContactUrl: buyer.publicContactPath,
    organizer: buyer.organizer || pattern.organizer,
    venueStatus: "Unknown",
    gdiDiscoveryVersion: "comp_set_demand_mining_v1",
    competitiveDemand: true,
    historicCompetitorHotels: pattern.historicHotels,
    supportSignals: ["COMPETITOR_HOTEL_USE", "BUYER_ORGANIZER_HINT"],
    engine: "COMPETITIVE",
    signalType: "COMPETITIVE",
  };
}

/**
 * Run canonical gates — never manually promote.
 */
export function classifyCompDemandGdiCandidate(draft = {}, opts = {}) {
  const lead = isGdiResearchLeadWorthPursuing(draft, {
    geoTokens: opts.geoTokens || [],
  });
  if (!lead.ok) {
    return {
      stage: "NOT_RESEARCH_LEAD",
      researchLead: false,
      customerReady: false,
      validFutureWatch: false,
      lead,
      ready: null,
      watch: null,
    };
  }

  const ready = isGdiCustomerOpportunityReady(draft, { nowDate: opts.nowDate || "2026-10-03" });
  const watch = isValidFutureWatch(draft, { nowDate: opts.nowDate || "2026-10-03" });
  const watchOk =
    watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH";

  return {
    stage: ready.ok
      ? "CUSTOMER_READY"
      : watchOk
        ? "VALID_FUTURE_WATCH"
        : "CANDIDATE_OR_RESEARCH_LEAD",
    researchLead: true,
    customerReady: ready.ok === true,
    validFutureWatch: watchOk && !ready.ok,
    lead,
    ready,
    watch,
  };
}

export function resolveBuyerForPattern(pattern = {}, trace = {}) {
  return resolveGdiDemandBuyer(
    {
      title: pattern.eventProgram,
      organizationName: pattern.organization,
      summaryWhat: pattern.lodgingPattern,
      officialSource: String(pattern.sources || "").split("|")[0],
    },
    {
      organizerName: pattern.organizer,
      organizationContactUrl: trace.organizationContactUrl,
      functionalContactEmail: trace.functionalContactEmail,
      sourceUrl: String(pattern.sources || "").split("|")[0],
    }
  );
}
