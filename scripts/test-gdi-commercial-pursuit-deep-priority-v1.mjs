#!/usr/bin/env node
/**
 * Offline validation: commercial pursuit strategies + deep-research prioritization.
 *
 * Nine founder scenarios — no paid provider calls.
 */
import assert from "node:assert/strict";
import {
  COMMERCIAL_PURSUIT_STRATEGY,
  PURSUIT_SOURCING_STATUS,
  SCREEN_DECISION,
  lowCostCommercialScreen,
  scoreDeepResearchInvestment,
  enrichOpportunityWithPursuitStrategy,
} from "../lib/group-demand-intelligence/research/commercial-pursuit-strategy-v1.js";
import {
  evaluateDeepResearchEligibility,
  ELIGIBILITY_DECISION,
} from "../lib/group-demand-intelligence/research/deep-research-eligibility-v1.js";
import {
  scoreOpportunityForFairQueue,
  buildSegmentHotelFairDeepResearchQueue,
} from "../lib/group-demand-intelligence/workflow/gdi-segment-hotel-fair-research-scheduler-v1.js";
import { qualifyCommercialProspectV3 } from "../lib/group-demand-intelligence/research/commercial-qualification-v3.js";

const HOTEL = "recLuxvwwxID7U2B8";
const CTX = { remainingBudgetUsd: 0.5, providersAvailable: true };

function base(over = {}) {
  return {
    id: "test_opp",
    hotelId: HOTEL,
    organizationName: "Test Organization",
    researchSegment: "associations_conferences_events",
    hotelOpportunityThesis:
      "Bethesda Marriott is within metro catchment for Mid-Atlantic association meetings with plausible guestroom and catering demand.",
    evidence: [{ url: "https://example.org/event", claim: "annual conference announced" }],
    seedUrls: ["https://example.org/event"],
    geoFit: { geoFitClass: "IN_CATCHMENT" },
    ...over,
  };
}

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS  ${name}`);
}

// 1) Unconfirmed hotel with real demand → OPEN_HOTEL_SOURCING, eligible
check("unconfirmed_hotel_real_demand_open_sourcing", () => {
  const opp = base({
    id: "open_1",
    venueSourcingStatus: "REGISTRATION_OPEN_HOTEL_UNANNOUNCED",
    internalDisposition: "PRE_SOURCING_OPPORTUNITY",
    hotelOpportunityThesis:
      "Registration is open for the 2027 Mid-Atlantic Association Conference; hotel selection is not yet announced. Bethesda Marriott is geographically suitable for a DC-metro room block and banquet.",
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(screen.strategy, COMMERCIAL_PURSUIT_STRATEGY.OPEN_HOTEL_SOURCING);
  assert.equal(screen.sourcingStatus, PURSUIT_SOURCING_STATUS.OPEN_UNCONFIRMED);
  assert.equal(screen.decision, SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH);
  assert.equal(screen.unknownHotelSelectionIsNegative, false);
  const elig = evaluateDeepResearchEligibility(opp, CTX);
  assert.equal(elig.eligible, true);
  assert.equal(elig.decision, ELIGIBILITY_DECISION.ELIGIBLE_TIER_2);
  assert.ok(elig.reasons.includes("open_hotel_selection_is_not_a_negative_signal"));
  assert.ok(elig.deepResearchInvestmentScore > 40);
});

// 2) Competitor-hosted current + credible future rebid → FUTURE_CYCLE contestable
check("competitor_hosted_future_rebid_contestable", () => {
  const opp = base({
    id: "disp_1",
    organizationName: "Regional Tech Alliance",
    hotelOpportunityThesis:
      "Annual conference hosted at the Hilton Washington DC for 2024–2026. Event rotates venues every few years; next cycle rebid window expected. Bethesda Marriott can compete for a future cycle guestroom block.",
    internalDisposition: "COMPETITIVE_FUTURE_CYCLE",
    namedHostHotel: "Hilton Washington DC",
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(screen.strategy, COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT);
  assert.equal(
    screen.sourcingStatus,
    PURSUIT_SOURCING_STATUS.FUTURE_CYCLE_POTENTIALLY_CONTESTABLE
  );
  assert.equal(screen.futureCycleOpportunity, true);
  assert.equal(screen.decision, SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH);
  const elig = evaluateDeepResearchEligibility(opp, CTX);
  assert.equal(elig.eligible, true);
  assert.equal(elig.competitorHostIsPermanentDisqualification, false);
});

// 3) Competitor-hosted with no viable Bethesda fit → reject
check("competitor_hosted_no_bethesda_fit_reject", () => {
  const opp = base({
    id: "nofit_1",
    organizationName: "Pacific Coast Builders",
    hotelOpportunityThesis:
      "Hosted at the San Francisco Marriott Marquis. No Bethesda fit; cannot compete; wrong geography out of market for Mid-Atlantic hotel.",
    geoFit: { geoFitClass: "OUT_OF_MARKET_MISMATCH" },
    namedHostHotel: "San Francisco Marriott Marquis",
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(screen.decision, SCREEN_DECISION.REJECT_IRRELEVANT);
  assert.equal(screen.strategy, COMMERCIAL_PURSUIT_STRATEGY.NOT_PURSUABLE);
  const elig = evaluateDeepResearchEligibility(opp, CTX);
  assert.equal(elig.eligible, false);
  assert.equal(elig.decision, ELIGIBILITY_DECISION.NOT_ELIGIBLE);
});

// 4) Recurring local catering meeting → MEETING_AND_CATERING_DISPLACEMENT
check("recurring_local_catering_meeting", () => {
  const opp = base({
    id: "lcm_1",
    researchSegment: "local_catering_day_meetings",
    organizationName: "Bethesda Chamber Chapter",
    hotelOpportunityThesis:
      "Monthly chapter luncheon and awards dinner currently held at a local club; recurring catering and day-meeting demand Bethesda Marriott banquet team could host.",
    opportunityType: "LOCAL_DAY_MEETING",
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(
    screen.strategy,
    COMMERCIAL_PURSUIT_STRATEGY.MEETING_AND_CATERING_DISPLACEMENT
  );
  assert.equal(screen.decision, SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH);
  const elig = evaluateDeepResearchEligibility(opp, CTX);
  assert.equal(elig.eligible, true);
});

// 5) Venue without guestrooms (marketplace / day venue) stays continuation or not auto guestroom-qualified
check("venue_without_guestrooms_not_forced_guestroom", () => {
  const opp = base({
    id: "venue_1",
    organizationName: "Local Gallery Space",
    hotelOpportunityThesis: "Private venue lists meeting space only.",
    evidence: [{ url: "https://example.org/gallery" }],
    researchSegment: "preferred_lodging_partnerships",
  });
  // Partnership path may advance; commercial qualification must not invent guestroom RFP
  const q = qualifyCommercialProspectV3({
    opportunity: opp,
    targetHotel: "Bethesda Marriott",
    acceptedClaims: [{ text: "gallery rental for day meetings", url: "https://example.org/gallery" }],
  });
  assert.ok(q.commercialPursuit);
  assert.equal(q.commercialPursuit.requiresActiveRfp, false);
  assert.equal(q.separationsPreserved.pursuitStrategySeparateFromMaturityAndPriority, true);
});

// 6) Event planner with repeated referral potential
check("event_planner_referral_partnership", () => {
  const opp = base({
    id: "pep_1",
    researchSegment: "professional_event_planners",
    organizationName: "Capital Event Partners DMC",
    hotelOpportunityThesis:
      "Destination management and event planner repeatedly places Mid-Atlantic corporate groups; preferred lodging and referral relationship with Bethesda Marriott is commercially plausible.",
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(
    screen.strategy,
    COMMERCIAL_PURSUIT_STRATEGY.PARTNERSHIP_AND_REFERRAL_DEVELOPMENT
  );
  assert.equal(screen.sourcingStatus, PURSUIT_SOURCING_STATUS.PARTNERSHIP_PIPELINE);
  assert.equal(screen.decision, SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH);
});

// 7) Future edition with no announced date — do not fabricate dates; FUTURE_CYCLE_DEVELOPMENT
check("future_edition_no_announced_date", () => {
  const opp = base({
    id: "fut_1",
    organizationName: "National Policy Forum",
    hotelOpportunityThesis:
      "Biennial forum historically rotates Mid-Atlantic cities. Next edition destination and hotel sourcing not yet published; recurring future cycle development for Bethesda Marriott.",
    internalDisposition: "COMPETITIVE_FUTURE_CYCLE",
    eventStartDate: null,
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(screen.strategy, COMMERCIAL_PURSUIT_STRATEGY.FUTURE_CYCLE_DEVELOPMENT);
  assert.equal(screen.futureCycleOpportunity, true);
  assert.equal(opp.eventStartDate, null); // never fabricate
  const enriched = enrichOpportunityWithPursuitStrategy(opp);
  assert.equal(enriched.futureCycleOpportunity, true);
  assert.ok(!enriched.fabricatedEventDate);
});

// 8) Duplicate event via another segment — fair queue dedupes by opportunity id
check("duplicate_event_across_segments_deduped", () => {
  const shared = {
    organizationName: "AFCEA Bethesda Chapter",
    hotelOpportunityThesis:
      "Annual symposium; hotel selection open pending; Bethesda Marriott suitable for room block.",
    venueSourcingStatus: "REGISTRATION_OPEN_HOTEL_UNANNOUNCED",
    evidence: [{ url: "https://example.org/afcea" }],
    seedUrls: ["https://example.org/afcea"],
    geoFit: { geoFitClass: "IN_CATCHMENT" },
    hotelId: HOTEL,
  };
  const a = { ...shared, id: "dup_afcea", researchSegment: "associations_conferences_events" };
  const b = { ...shared, id: "dup_afcea", researchSegment: "government_public_sector" }; // same id
  const schedule = buildSegmentHotelFairDeepResearchQueue([a, b], {
    hotelIds: [HOTEL],
    segmentIds: [
      "associations_conferences_events",
      "government_public_sector",
    ],
  });
  const ids = schedule.queue.map((o) => o.id);
  assert.equal(ids.filter((id) => id === "dup_afcea").length, 1);
  assert.equal(schedule.totalQueued, 1);
});

// 9) Hotel-specific need-date match boosts investment score
check("need_date_match_boosts_investment", () => {
  const opp = base({
    id: "need_1",
    venueSourcingStatus: "REGISTRATION_OPEN_HOTEL_UNANNOUNCED",
    hotelOpportunityThesis:
      "Hotel selection not yet announced for Q2 association meeting with guestroom block need near Bethesda Metro.",
  });
  const without = scoreDeepResearchInvestment(opp, { ...CTX, needDateMatch: false });
  const withMatch = scoreDeepResearchInvestment(opp, { ...CTX, needDateMatch: true });
  assert.ok(withMatch.score > without.score);
  assert.ok(withMatch.notes.includes("need_date_match"));
  const fair = scoreOpportunityForFairQueue({ ...opp, needDateMatch: true });
  assert.ok(fair > scoreOpportunityForFairQueue({ ...opp, needDateMatch: false }));
});

// Extra: CURRENT_HOST_COMMITTED ≠ FUTURE_CYCLE_POTENTIALLY_CONTESTABLE
check("current_host_committed_distinct_from_future_cycle", () => {
  const opp = base({
    id: "commit_1",
    hotelOpportunityThesis:
      "2026 program hosted at the Omni Shoreham Hotel in Washington DC. Current host commitment only; no stated next-year sourcing opening.",
    namedHostHotel: "Omni Shoreham Hotel",
  });
  const screen = lowCostCommercialScreen(opp);
  assert.equal(screen.strategy, COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT);
  assert.equal(screen.sourcingStatus, PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED);
  assert.equal(screen.futureCycleOpportunity, false);
  // Still may deep-research to resolve future-cycle uncertainty — not auto EXCLUDE
  assert.equal(screen.decision, SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH);
  assert.ok(
    screen.reasons.includes("deep_research_to_resolve_future_cycle_contestability")
  );
});

// Extra: speculative without evidence → RESEARCH_CONTINUATION (not inflate eligible)
check("speculative_without_evidence_stays_continuation", () => {
  const opp = {
    id: "spec_1",
    hotelId: HOTEL,
    organizationName: "Vague Corp",
    researchSegment: "scaling_companies",
    hotelOpportunityThesis: "Maybe rooms?",
    evidence: [],
    seedUrls: [],
  };
  const screen = lowCostCommercialScreen(opp);
  assert.equal(screen.decision, SCREEN_DECISION.RESEARCH_CONTINUATION);
  const elig = evaluateDeepResearchEligibility(opp, CTX);
  assert.equal(elig.eligible, false);
});

console.log(`\nOK  ${passed} commercial-pursuit deep-priority checks passed (offline, $0)`);
