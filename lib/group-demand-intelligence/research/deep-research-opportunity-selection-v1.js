/**
 * Deep-research opportunity selection V1 — rank existing GDI opportunities
 * by expected commercial value of investigation (not missing-field count).
 * No new discovery. No paid calls.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { evaluateDeepResearchEligibility } from "./deep-research-eligibility-v1.js";
import { isConsumerEmailDomain } from "./commercial-spine-restoration-v41.js";

export const DEEP_RESEARCH_OPPORTUNITY_SELECTION_V1 = "gdi-deep-research-opportunity-selection-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const HOTELS_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels");

/** Prior test opportunities — may appear in corpus but must not be preferred solely for re-test. */
export const PRIOR_TEST_OPPORTUNITY_IDS = Object.freeze([
  "gdi_lmc_97fb727376abb83b", // Gaslamp
  "gdi_opp_potomac_memorial_2027",
  "gdi_opp_nado_ddaa_washcon_2028",
]);

/**
 * Locked principal offline validation cohort from commercial-improvement selection.
 * Buyer-intelligence offline cert prefers these IDs (not live ranker drift).
 */
export const BUYER_INTELLIGENCE_VALIDATION_COHORT_V1 = Object.freeze([
  "gdi_opp_alexandria_soccer_kickoff_2027",
  "gdi_lmc_4d2f10eab2d44499",
  "gdi_opp_aad_2028_overflow",
]);

export const PRIOR_TEST_ORG_PATTERNS = Object.freeze([
  /^Principal\.?\s*London/i,
  /Gaslamp Event Management/i,
  /Potomac Soccer Association/i,
  /National Association of Development Organizations/i,
]);

const HOTEL_DISPLAY_NAME_FALLBACKS = Object.freeze({
  recnPB3Q02NO8Ma1o: "Wyndham San Diego Bayside",
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recC2kbCaS48jFjFF: "Nobu Hotel London Portman Square",
  recgMYovrrZDJMqzX: "Waterstone Resort & Marina Boca Raton",
  recG66DQJKP2c0UNh: "Renaissance New York Times Square Hotel",
  rec35fExUxCClpOP6: "Hilton New York Times Square",
  recKRJjcPnb4tVDDS: "Spice Island Beach Resort",
  recdmo0CnhIZAwHGC: "The Westin Tampa Waterside",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelDisplayName(hotelId) {
  const profilePath = path.join(HOTELS_DIR, hotelId, "profile.json");
  if (fs.existsSync(profilePath)) {
    try {
      const profile = JSON.parse(fs.readFileSync(profilePath, "utf8"));
      const name = profile?.identity?.hotelName || profile?.hotelName || profile?.name;
      if (text(name)) return text(name);
    } catch { /* fallback */ }
  }
  return HOTEL_DISPLAY_NAME_FALLBACKS[hotelId] || hotelId;
}

function loadHotelOpportunities(hotelId) {
  const p = path.join(HOTELS_DIR, hotelId, "opportunities.json");
  if (!fs.existsSync(p)) return [];
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  return Array.isArray(raw) ? raw : (raw.opportunities || []);
}

export function listProductionHotelIds() {
  return fs.readdirSync(HOTELS_DIR).filter((d) => d.startsWith("rec"));
}

export function inferPathway(opportunity = {}) {
  const type = String(opportunity.opportunityType || "");
  const room = String(opportunity.roomDemandStatus || "");
  const venue = String(opportunity.venueSourcingStatus || "");
  const segment = String(opportunity.segment || opportunity.researchSegment || "");
  const thesis = text(opportunity.hotelOpportunityThesis || opportunity.summaryWhyHotel || "");
  const blob = `${type} ${room} ${venue} ${segment} ${thesis} ${opportunity.organizationName || ""}`;

  // Mega-events / named overflow against locked hosts → competitive future cycle
  if (/FIFA|World Cup|overflow if|already .+ (?:host|hotel)|Crystal Gateway|Marquis|headquarter hotel/i.test(blob)
    && /OVERFLOW|FUTURE|HOTEL_VENUE_TBD|PARTIALLY|PRIMARY_VENUE/i.test(`${type} ${room} ${venue}`)) {
    return "COMPETITIVE_FUTURE_CYCLE";
  }

  if (type === "CORPORATE_ACCOUNT_RELATIONSHIP"
    || (/PRIMARY_PURSUIT/i.test(type) && /DMC|incentive|planner|Local Meetings|corporate/i.test(segment + thesis))
    || (/Local Meetings|DMC|incentive|corporate headquarters|executive travel/i.test(segment)
      && !/OVERFLOW_HOUSING|VERIFIED_HOUSING/i.test(`${type} ${room}`))) {
    return "ACCOUNT_LED";
  }

  if (/OVERFLOW_HOUSING|VERIFIED_HOUSING|stay-?to-?play|tournament|housing\s*bureau|Sports\s*\/\s*Weekend/i.test(blob)
    && !/cannot win host|already Crystal/i.test(thesis)) {
    if (/PRIMARY_VENUE_SELECTED|OVERFLOW|RFP_ACTIVE|HOTEL_VENUE_TBD|PARTIALLY_PLACED|OPEN_UNRESOLVED/i.test(venue)
      || /VERIFIED_HOUSING/i.test(room)) {
      return "EVENT_LED";
    }
  }

  if (/FUTURE_CYCLE|OVERFLOW_ONLY|COMPETITIVE|PRIMARY_VENUE_SELECTED/i.test(`${type} ${room} ${venue}`)
    || /overflow if|future.?cycle|not yet named/i.test(thesis)) {
    return "COMPETITIVE_FUTURE_CYCLE";
  }

  if (/VERIFIED_HOUSING|OVERFLOW_HOUSING|Sports|Weekend|tournament/i.test(blob)) {
    return "EVENT_LED";
  }
  return "ACCOUNT_LED";
}

function isPriorTestOpportunity(opportunity = {}) {
  const id = opportunity.id || opportunity.opportunityId;
  if (PRIOR_TEST_OPPORTUNITY_IDS.includes(id)) return true;
  const org = opportunity.organizationName || "";
  return PRIOR_TEST_ORG_PATTERNS.some((re) => re.test(org));
}

function weakThesis(thesis) {
  return /none sufficient|not a current primary|monitor for a future|sponsorship\/relationship only|cannot host|unlikely primary|guestrooms demand is not yet evidenced/i.test(thesis);
}

function researchableGaps(opportunity = {}) {
  const gaps = [];
  const contact = opportunity.contactEmail || opportunity.primaryContactEmail || opportunity.contact?.businessEmail;
  if (!contact) gaps.push("No organization contact route — public WHO / org pages may establish one");
  else if (isConsumerEmailDomain(contact)) gaps.push("Consumer-domain email — seek org-domain route");
  else if (!opportunity.contactName && !opportunity.primaryContactName && !opportunity.contactIsNamedPerson) {
    gaps.push("General org route only — role-relevant named buyer unresolved");
  }
  if (/OPEN_UNRESOLVED|HOTEL_VENUE_TBD|RFP_ACTIVE|OVERFLOW_POSSIBLE|PARTIALLY_PLACED|UNKNOWN/i.test(String(opportunity.venueSourcingStatus || ""))) {
    gaps.push("Venue/housing sourcing status incomplete — research can resolve host/overflow/shortlist");
  }
  if (/VERIFIED_HOUSING|ESTIMATED_ROOM|OVERFLOW/i.test(String(opportunity.roomDemandStatus || ""))
    && !/named housing bureau|HBC list|official overflow/i.test(text(opportunity.hotelOpportunityThesis))) {
    gaps.push("Housing/room-demand mechanism known but partner/list membership unclear");
  }
  if (opportunity.officialSource || opportunity.contactUrl) {
    gaps.push("Official source available for autonomous follow-up");
  }
  if (/FUTURE_CYCLE|202[7-9]/i.test(String(opportunity.opportunityType || opportunity.eventStartDate || ""))
    && !opportunity.eventStartDate) {
    gaps.push("Future cycle without firm dates — dates/windows researchable");
  }
  return gaps;
}

function estimateInvestigationCostUsd(opportunity = {}, eligibility = {}) {
  const gaps = researchableGaps(opportunity).length;
  const base = eligibility.tier?.includes?.("TIER_3") ? 0.35 : 0.45;
  if (gaps <= 2) return Number((base * 0.7).toFixed(2));
  if (gaps >= 5) return Number(Math.min(0.55, base * 1.1).toFixed(2));
  return base;
}

/**
 * Score expected commercial value of deep research for one existing opportunity.
 */
export function scoreDeepResearchCandidate(opportunity = {}, { hotelId = null, hotelName = null } = {}) {
  const id = opportunity.id || opportunity.opportunityId;
  const org = opportunity.organizationName || "Unknown";
  const hid = hotelId || opportunity.hotelId;
  const hname = hotelName || loadHotelDisplayName(hid) || opportunity.hotelName || hid;
  const thesis = text(opportunity.hotelOpportunityThesis || opportunity.summaryWhyHotel || opportunity.whyThisHotel || "");
  const pathway = inferPathway(opportunity);
  const gaps = researchableGaps(opportunity);
  const priorTest = isPriorTestOpportunity(opportunity);

  const eligibility = evaluateDeepResearchEligibility({
    ...opportunity,
    hotelId: hid,
    hotelName: hname,
    unresolvedQuestions: gaps,
  }, { remainingBudgetUsd: 0.5, providersAvailable: true });

  let score = 0;
  const reasons = [];
  const risks = [];

  // Verified underlying activity
  if (/VERIFIED_HOUSING|HOUSING_PROGRAM/i.test(String(opportunity.roomDemandStatus || ""))) {
    score += 3;
    reasons.push("verified_housing_or_room_demand");
  } else if (/ESTIMATED_ROOM_DEMAND/i.test(String(opportunity.roomDemandStatus || ""))) {
    score += 1.5;
    reasons.push("estimated_room_demand");
  }
  if (/RFP_ACTIVE_SOURCING/i.test(String(opportunity.venueSourcingStatus || ""))) {
    score += 3;
    reasons.push("active_sourcing_window");
  } else if (/OVERFLOW_POSSIBLE|HOTEL_VENUE_TBD|PARTIALLY_PLACED|OPEN_UNRESOLVED/i.test(String(opportunity.venueSourcingStatus || ""))) {
    score += 2;
    reasons.push("open_or_partial_sourcing");
  } else if (/FULLY_PLACED|NO_OVERFLOW_EVIDENCE/i.test(String(opportunity.venueSourcingStatus || ""))) {
    score -= 2;
    reasons.push("fully_placed_or_no_overflow");
    risks.push("Primary venue may be locked — research must prove overflow path or exclude");
  }

  // Hotel fit
  if (thesis.length > 120 && !weakThesis(thesis)) {
    score += 2;
    reasons.push("credible_hotel_thesis");
  } else if (weakThesis(thesis)) {
    score -= 3;
    reasons.push("weak_or_monitor_only_thesis");
    risks.push("Thesis already signals low pursuit value");
  }
  if (/overflow|Beltway|Metro|waterfront|ballroom|stay-?to-?play|housing|Marylebone|incentive/i.test(thesis)) {
    score += 1;
    reasons.push("hotel_fit_language");
  }

  // Researchable unanswered questions (not mere missing fields)
  const researchable = gaps.filter((g) => !/missing field/i.test(g));
  score += Math.min(3, researchable.length * 0.75);
  if (researchable.length) reasons.push(`researchable_gaps:${researchable.length}`);

  // Contact gap that research could close (high VoI) vs already-complete contacts
  const contact = opportunity.contactEmail || opportunity.primaryContactEmail;
  if (!contact) {
    score += 1.5;
    reasons.push("contact_gap_researchable");
  } else if (isConsumerEmailDomain(contact)) {
    score += 1;
    reasons.push("consumer_email_needs_org_route");
  } else if (opportunity.contactName || opportunity.primaryContactName || opportunity.contactIsNamedPerson) {
    score -= 0.5;
    reasons.push("named_contact_already_present");
  }

  // Official source / independent path available
  if (opportunity.officialSource || opportunity.contactUrl) {
    score += 1;
    reasons.push("official_source_available");
  }
  if (opportunity.eventStartDate || opportunity.eventDateDisplay) {
    score += 0.5;
    reasons.push("dated_program");
  }

  // Revenue potential
  if (/GUESTROOM|HOUSING|OVERFLOW|meeting|banquet|ballroom|catering/i.test(thesis + String(opportunity.proposedHotelUse || ""))) {
    score += 1;
    reasons.push("guestroom_or_meeting_revenue_path");
  }
  if (/NOT_REQUIRED/i.test(String(opportunity.roomDemandStatus || "")) && /Local Meetings/i.test(String(opportunity.segment || ""))) {
    score += 0.5;
    reasons.push("meetings_catering_account_path");
  }

  // Eligibility gate
  if (!eligibility.eligible) {
    score -= 2;
    reasons.push(`eligibility:${eligibility.decision}`);
  } else {
    score += 1;
    reasons.push(`eligibility:${eligibility.decision}`);
  }

  // Do not boost prior test cases
  if (priorTest) {
    score -= 5;
    reasons.push("prior_test_opportunity_deprioritized");
  }

  // Unknown / placeholder org identity
  if (/^Unknown$/i.test(org) || /^Various\b/i.test(org) || /TBD|placeholder|multiple corporations/i.test(org)) {
    score -= 5;
    reasons.push("unresolved_or_generic_organization_identity");
    risks.push("Organization identity unresolved or generic — deep research cannot target a buyer");
  }

  // Mega-event accounts are rarely actionable at $0.45 public research for one hotel
  if (/^FIFA$/i.test(org) || /World Cup 2026/i.test(thesis + org)) {
    score -= 2;
    reasons.push("mega_event_low_hotel_level_voi");
    risks.push("World Cup housing is highly centralized — public research may not yield hotel-level buyer access");
  }

  const estimatedCostUsd = estimateInvestigationCostUsd(opportunity, eligibility);
  const expectedBenefit = score >= 8
    ? "HIGH — research likely to change outreach target, housing path, or pursue/exclude decision"
    : score >= 5
      ? "MEDIUM — research may clarify buyer route, sourcing status, or timing"
      : "LOW — limited commercial VoI relative to cost";

  const researchQuestions = [
    `Who owns hotel sourcing / housing / meeting venue selection at ${org}?`,
    pathway === "COMPETITIVE_FUTURE_CYCLE"
      ? `What is the current/next-cycle host or overflow status, and can ${hname} compete without inventing demand?`
      : pathway === "EVENT_LED"
        ? `Which housing bureau / stay-to-play partner / room-block path applies, and is ${hname} listed or listable?`
        : `What dated programmes or recurring meetings create lodging or meeting-space demand for ${hname}?`,
    `What independent or official sources corroborate group activity without inventing RFPs or room counts?`,
    ...researchable.slice(0, 2),
  ];

  return {
    version: DEEP_RESEARCH_OPPORTUNITY_SELECTION_V1,
    opportunityId: id,
    hotelId: hid,
    hotelName: hname,
    organizationName: org,
    pathway,
    opportunityType: opportunity.opportunityType || null,
    roomDemandStatus: opportunity.roomDemandStatus || null,
    venueSourcingStatus: opportunity.venueSourcingStatus || null,
    contactEmail: contact || null,
    officialSource: opportunity.officialSource || opportunity.contactUrl || null,
    eventStartDate: opportunity.eventStartDate || opportunity.eventDateDisplay || null,
    verifiedDemandSignal: thesis.slice(0, 320) || null,
    whyHotelCouldCompete: thesis.slice(0, 320) || null,
    whatRemainsUnknown: researchable,
    whyResearchCouldChangeSalesDecision: expectedBenefit,
    researchQuestions: [...new Set(researchQuestions)].slice(0, 6),
    contactIntelligenceGaps: researchable.filter((g) => /contact|buyer|email|route/i.test(g)),
    availableSourcePaths: [
      opportunity.officialSource || opportunity.contactUrl || null,
      contact ? `org contact ${contact}` : null,
      "organization team/leadership/housing pages",
      "independent press / housing bureau / association pages via Exa",
    ].filter(Boolean),
    expectedCommercialBenefit: expectedBenefit,
    researchRisks: risks.length ? risks : ["Public sources may not name housing/meeting decision-makers"],
    estimatedInvestigationCostUsd: estimatedCostUsd,
    score: Number(score.toFixed(2)),
    scoreReasons: reasons,
    eligibility,
    priorTestOpportunity: priorTest,
    commerciallyWeak: weakThesis(thesis) || score < 3,
    incompleteButPotentiallyStrong: score >= 5 && researchable.length >= 2 && !weakThesis(thesis),
  };
}

/**
 * Rank all production-hotel opportunities; optionally exclude prior tests from shortlist.
 */
export function rankDeepResearchCandidates({
  excludePriorTests = true,
  hotelIds = null,
  limit = 50,
} = {}) {
  const hotels = hotelIds || listProductionHotelIds();
  const ranked = [];
  for (const hotelId of hotels) {
    const opps = loadHotelOpportunities(hotelId);
    const hotelName = loadHotelDisplayName(hotelId);
    for (const opp of opps) {
      if (!opp?.id || !opp.organizationName) continue;
      const scored = scoreDeepResearchCandidate(opp, { hotelId, hotelName });
      if (excludePriorTests && scored.priorTestOpportunity) continue;
      ranked.push(scored);
    }
  }
  ranked.sort((a, b) => b.score - a.score || String(a.opportunityId).localeCompare(String(b.opportunityId)));
  return ranked.slice(0, limit);
}

/**
 * Select top 3 for next validation — prefer pathway diversity without forcing weak picks.
 */
export function selectShortlistForNextValidation({ preferPathwayDiversity = true, excludePriorTests = true } = {}) {
  const ranked = rankDeepResearchCandidates({ excludePriorTests, limit: 80 });
  const strong = ranked.filter((r) => r.incompleteButPotentiallyStrong || r.score >= 6);
  const pool = strong.length >= 3 ? strong : ranked.filter((r) => r.score >= 4.5);

  const picked = [];
  const usedPathways = new Set();
  const usedHotels = new Set();

  const tryPick = (pred) => {
    for (const c of pool) {
      if (picked.some((p) => p.opportunityId === c.opportunityId)) continue;
      if (!pred(c)) continue;
      picked.push(c);
      usedPathways.add(c.pathway);
      usedHotels.add(c.hotelId);
      return true;
    }
    return false;
  };

  if (preferPathwayDiversity) {
    tryPick((c) => c.pathway === "EVENT_LED" && c.score >= 6);
    tryPick((c) => c.pathway === "ACCOUNT_LED"
      && c.score >= 5.5
      && !/FIFA|World Cup/i.test(c.organizationName || "")
      && (/CORPORATE_ACCOUNT|PRIMARY_PURSUIT|Local Meetings|DMC|Corporate Headquarters/i.test(
        `${c.opportunityType || ""} ${c.verifiedDemandSignal || ""}`,
      ) || /account|meetings|banquet|incentive|corporate/i.test(c.verifiedDemandSignal || "")));
    tryPick((c) => c.pathway === "COMPETITIVE_FUTURE_CYCLE" && c.score >= 6);
  }

  // Fill remaining by score; prefer different hotels when possible
  for (const c of pool) {
    if (picked.length >= 3) break;
    if (picked.some((p) => p.opportunityId === c.opportunityId)) continue;
    if (usedHotels.has(c.hotelId) && picked.length < 2) {
      // allow same hotel if needed later
    }
    if (usedHotels.has(c.hotelId) && pool.some((x) =>
      !picked.some((p) => p.opportunityId === x.opportunityId)
      && !usedHotels.has(x.hotelId)
      && x.score >= c.score - 1.5)) {
      continue;
    }
    picked.push(c);
    usedPathways.add(c.pathway);
    usedHotels.add(c.hotelId);
  }

  while (picked.length < 3) {
    const next = ranked.find((c) => !picked.some((p) => p.opportunityId === c.opportunityId));
    if (!next) break;
    picked.push(next);
  }

  return {
    version: DEEP_RESEARCH_OPPORTUNITY_SELECTION_V1,
    selectedAt: new Date().toISOString(),
    selectionMethod: "score_expected_commercial_voi_then_pathway_diversity",
    hardcodedWinners: false,
    excludePriorTests,
    shortlist: picked.slice(0, 3),
    pathwayCoverage: [...usedPathways],
    rankingHead: ranked.slice(0, 10).map((r) => ({
      opportunityId: r.opportunityId,
      hotelId: r.hotelId,
      organizationName: r.organizationName,
      pathway: r.pathway,
      score: r.score,
    })),
  };
}
