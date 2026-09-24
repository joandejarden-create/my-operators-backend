#!/usr/bin/env node
/**
 * Future-cycle Watch gating regressions — REUSABLE_PRODUCT_LOGIC.
 * No hotel-name hardcodes in assertions (synthetic titles only).
 */

import assert from "node:assert/strict";
import {
  buildOpportunity,
  deriveOpportunityType,
  computeOpportunityQualification,
  classifyPriority,
  detectFutureCycleEvidence,
  FUTURE_CYCLE_EVIDENCE_STATE,
  PRIORITY,
  OPPORTUNITY_TYPE,
  VENUE_SOURCING_STATUS,
  BOOKING_WINDOW,
  normalizeResearchProviderResult,
  shouldEscalateToParallel,
  resolveResearchProviderPath,
} from "../lib/group-demand-intelligence/index.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("fc1_prior_host_recurring_unpublished_next_is_watchlist", () => {
  const opp = buildOpportunity({
    hotelId: "gdi_hotel_synthetic_alpha",
    title: "Annual Industry Festival — next fall cycle unpublished",
    organizationName: "Regional Arts Council",
    venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
    roomDemandStatus: "OVERFLOW_ONLY",
    demandTerritoryFit: "TERRITORY_CORE",
    bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
    whyNow:
      "Recurring annual event; prior-year host hotel known; next fall cycle not yet announced — monitor for future housing.",
    hotelOpportunityThesis:
      "Future fall-cycle housing / overflow conversation once the next cycle publishes venue and room needs.",
    estimatedPeakRooms: 60,
    fitComponents: {
      physicalFit: 70,
      geographyFit: 80,
      timing: 55,
      commercialValue: 65,
      historicalFit: 60,
      competitiveAccessibility: 55,
      contactability: 40,
    },
    confidenceInput: {
      sourceAuthority: 60,
      independentSourceCount: 2,
      directness: 55,
      recency: 50,
      verifiedFieldRatio: 0.4,
      firstPartyShare: 40,
    },
  });
  assert.equal(opp.opportunityType, OPPORTUNITY_TYPE.FUTURE_CYCLE);
  assert.equal(opp.priority, PRIORITY.WATCHLIST);
  assert.notEqual(opp.opportunityQualification, "CLOSED");
  assert.ok(opp.futureCycleEvidenceState);
  assert.ok(opp.whyMonitor);
});

check("fc2_one_time_no_recurrence_stays_dq", () => {
  const opp = buildOpportunity({
    hotelId: "gdi_hotel_synthetic_alpha",
    title: "One-time Corporate Offsite 2025",
    organizationName: "Acme Corp",
    venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
    roomDemandStatus: "UNKNOWN",
    demandTerritoryFit: "TERRITORY_CORE",
    whyNow: "Single offsite already hosted elsewhere with no recurrence evidence.",
    hotelOpportunityThesis: "No future cycle evidence; primary venue already selected.",
    fitComponents: {
      physicalFit: 50,
      geographyFit: 50,
      timing: 40,
      commercialValue: 40,
      historicalFit: 30,
      competitiveAccessibility: 30,
      contactability: 20,
    },
    confidenceInput: {
      sourceAuthority: 40,
      independentSourceCount: 1,
      directness: 40,
      recency: 40,
      verifiedFieldRatio: 0.2,
      firstPartyShare: 20,
    },
  });
  assert.equal(opp.opportunityType, OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED);
  assert.equal(opp.priority, PRIORITY.DISQUALIFIED);
});

check("fc3_recurring_moved_permanently_out_of_market_dq", () => {
  const ev = detectFutureCycleEvidence({
    title: "Annual Meeting",
    whyNow: "Recurring annual meeting moved permanently to Las Vegas; discontinued in this market.",
  });
  assert.equal(ev.terminalNegative, true);
  assert.equal(ev.preserveAsWatch, false);
});

check("fc4_future_cycle_confirmed_venue_tbd_qualifies", () => {
  const opp = buildOpportunity({
    hotelId: "gdi_hotel_synthetic_alpha",
    title: "2028 Association Congress — venue TBD",
    organizationName: "National Association",
    venueSourcingStatus: VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
    roomDemandStatus: "ESTIMATED_ROOM_DEMAND",
    estimatedPeakRooms: 90,
    demandTerritoryFit: "TERRITORY_CORE",
    bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
    whyNow: "2028 congress announced; hotel venue still TBD with active sourcing window.",
    hotelOpportunityThesis:
      "Open primary pursuit — venue TBD with estimated mid-size peak rooms inside capture band for this hotel.",
    primaryContact: { name: "Jordan Lee", role: "Meetings Director", sourceUrl: "https://example.org" },
    fitComponents: {
      physicalFit: 72,
      geographyFit: 80,
      timing: 70,
      commercialValue: 70,
      historicalFit: 50,
      competitiveAccessibility: 60,
      contactability: 55,
    },
    confidenceInput: {
      sourceAuthority: 70,
      independentSourceCount: 3,
      directness: 65,
      recency: 70,
      verifiedFieldRatio: 0.5,
      firstPartyShare: 55,
    },
    evidence: [{ field: "source", sourceUrl: "https://example.org", claimKind: "FACT" }],
  });
  assert.equal(opp.opportunityType, OPPORTUNITY_TYPE.PRIMARY_PURSUIT);
  assert.ok(
    opp.priority === PRIORITY.HIGH || opp.priority === PRIORITY.MEDIUM,
    `expected High/Medium, got ${opp.priority}`
  );
});

check("fc5_historical_host_does_not_imply_open_sourcing", () => {
  const type = deriveOpportunityType({
    venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
    roomDemandStatus: "OVERFLOW_ONLY",
    reactivationSignal: "PRIOR_COMPETITOR_USAGE",
    bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
    opportunity: {
      whyNow: "Recurring annual; prior-year host known; next cycle not yet announced.",
      hotelOpportunityThesis: "Future-cycle housing watch only.",
    },
  });
  assert.equal(type.opportunityType, OPPORTUNITY_TYPE.FUTURE_CYCLE);
  const qual = computeOpportunityQualification({
    venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
    opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
    roomDemandStatus: "OVERFLOW_ONLY",
    eventLocationStatus: "VERIFIED_CITY",
    contactQuality: "NAMED_PERSON",
    hotelOpportunityThesis: "Future-cycle housing watch only for next published cycle.",
    evidenceConfidence: 50,
    demandTerritoryFit: "TERRITORY_CORE",
  });
  assert.equal(qual.opportunityQualification, "MODERATE");
  const pri = classifyPriority({
    hotelFitScore: 71,
    evidenceConfidence: 57,
    bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
    opportunityQualification: qual.opportunityQualification,
    opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
    venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
  });
  assert.equal(pri.priority, PRIORITY.WATCHLIST);
});

check("fc6_strong_evidence_state_is_recurring_or_prior_host", () => {
  const ev = detectFutureCycleEvidence({
    whyNow: "Recurring annual festival; prior-year host hotel known; next cycle unpublished.",
    demandStatus: "RECURRING_PREDICTED",
  });
  assert.ok(
    [
      FUTURE_CYCLE_EVIDENCE_STATE.RECURRING_CYCLE_STRONG,
      FUTURE_CYCLE_EVIDENCE_STATE.PRIOR_YEAR_HOST_KNOWN,
      FUTURE_CYCLE_EVIDENCE_STATE.CURRENT_FUTURE_CYCLE_CONFIRMED,
    ].includes(ev.state),
    ev.state
  );
  assert.equal(ev.preserveAsWatch, true);
});

check("norm_webhound_and_native_share_schema", () => {
  const native = normalizeResearchProviderResult({
    provider: "NATIVE",
    rawOutput: {
      title: "Sample Event",
      organizationName: "Sample Org",
      venueSourcingStatus: "HOTEL_VENUE_TBD",
      estimatedPeakRooms: 40,
      whyNow: "Venue TBD",
      sources: ["https://example.org"],
    },
  });
  const wh = normalizeResearchProviderResult({
    provider: "WEBHOUND",
    rawOutput: {
      event: "Sample Event",
      organization: "Sample Org",
      venue: "Hotel / Venue TBD",
      rooms: 40,
      why_now: "Venue TBD",
      sources: ["https://example.org"],
    },
  });
  assert.equal(native.schemaVersion, wh.schemaVersion);
  assert.equal(native.venueStatus, "HOTEL_VENUE_TBD");
  assert.ok(
    ["HOTEL_VENUE_TBD", "OPEN_UNRESOLVED", "UNKNOWN"].includes(wh.venueStatus),
    `unexpected venueStatus ${wh.venueStatus}`
  );
  assert.equal(native.provider, "NATIVE");
  assert.equal(wh.provider, "WEBHOUND");
});

check("parallel_gate_requires_multiple_triggers", () => {
  const weak = shouldEscalateToParallel({
    nativeResult: { confidence: 40, sourceEvidence: [], venueStatus: "UNKNOWN" },
  });
  assert.equal(weak.escalate, true);
  const strong = shouldEscalateToParallel({
    nativeResult: {
      confidence: 80,
      sourceEvidence: [{ url: "https://marriott.com", authority: "official" }],
      venueStatus: "HOTEL_VENUE_TBD",
      contactEvidence: { name: "Alex" },
      researchCompleteness: { hasEvent: true, hasOrg: true },
      discoveryCount: 8,
    },
  });
  assert.equal(strong.escalate, false);
});

check("webhound_unavailable_path_never_requires_webhound", () => {
  process.env.WEBHOUND_UNAVAILABLE = "true";
  const path = resolveResearchProviderPath({
    webhoundUnavailable: true,
    nativeResult: { confidence: 30, sourceEvidence: [], venueStatus: "UNKNOWN" },
  });
  assert.equal(path.webhound, "UNAVAILABLE");
  assert.equal(path.audit.webhoundRequired, false);
  assert.ok(["NATIVE_ONLY", "NATIVE_PLUS_GATED_PARALLEL"].includes(path.audit.providerPath));
  delete process.env.WEBHOUND_UNAVAILABLE;
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nAll future-cycle / normalization / Parallel-gate regressions passed.");
