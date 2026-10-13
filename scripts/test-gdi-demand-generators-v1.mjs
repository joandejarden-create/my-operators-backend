/**
 * GDI Demand Generator Intelligence V1 — unit + fixture tests.
 * No live write / Surfe / PDL.
 */

import assert from "node:assert/strict";
import {
  buildDemandGeneratorEntity,
  buildProgramEntity,
  assertNoInventedFuture,
  computeDemandGeneratorId,
} from "../lib/group-demand-intelligence/demand-generators/entities.js";
import {
  evaluateGeneratorRelevance,
  buildHotelDemandGeneratorFit,
  organizationTypesForHotelProfile,
} from "../lib/group-demand-intelligence/demand-generators/fit-relevance.js";
import {
  qualifyGeneratorSignal,
  buildDemandGeneratorSignal,
  buildGeneratorDrivenOpportunityDraft,
} from "../lib/group-demand-intelligence/demand-generators/qualify-promote.js";
import {
  buildGeneratorDiscoveryQueries,
  buildGeneratorGuidedQueries,
  compareSearchEfficiency,
  measureIncrementalAtBats,
} from "../lib/group-demand-intelligence/demand-generators/discovery.js";
import { auditDemandGeneratorHotelSpecificLogic } from "../lib/group-demand-intelligence/demand-generators/hotel-specific-audit.js";
import {
  GENERATOR_RELEVANCE_CLASS,
  SIGNAL_STATUS,
  RECURRENCE_STATUS,
} from "../lib/group-demand-intelligence/demand-generators/constants.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS  ${name}`);
    passed += 1;
  } catch (err) {
    console.error(`FAIL  ${name}`);
    console.error(err);
    failed += 1;
  }
}

check("association_with_annual_program", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Example Association",
    organizationType: "ASSOCIATION",
    officialDomain: "example-assoc.org",
    website: "https://example-assoc.org/",
  });
  const p = buildProgramEntity({
    demandGeneratorId: g.demandGeneratorId,
    programName: "Annual Conference",
    programType: "ANNUAL_CONFERENCE",
    recurrenceStatus: "RECURRING_CONFIRMED",
  });
  assert.equal(p.recurrenceStatus, RECURRENCE_STATUS.RECURRING_CONFIRMED);
  assert.ok(p.seriesId.startsWith("dgsr_"));
});

check("training_provider_quarterly_sessions", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Acme Training",
    organizationType: "TRAINING_PROVIDER",
    officialDomain: "acmetraining.example",
  });
  const p = buildProgramEntity({
    demandGeneratorId: g.demandGeneratorId,
    programName: "Quarterly Leadership Academy",
    programType: "TRAINING_ACADEMY",
    recurrenceStatus: "RECURRING_CONFIRMED",
    recurrenceFrequency: "quarterly",
  });
  assert.equal(p.recurrenceFrequency, "quarterly");
});

check("government_agency_recurring_symposium", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Federal Research Agency",
    organizationType: "GOVERNMENT_AGENCY",
    officialDomain: "agency.gov",
  });
  const p = buildProgramEntity({
    demandGeneratorId: g.demandGeneratorId,
    programName: "Annual Symposium",
    programType: "SYMPOSIUM",
    recurrenceStatus: "RECURRING_CONFIRMED",
  });
  assert.equal(p.programType, "SYMPOSIUM");
});

check("contractor_one_off_award_only", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Contractor Co",
    organizationType: "GOVERNMENT_CONTRACTOR",
    officialDomain: "contractor.example",
  });
  const p = buildProgramEntity({
    demandGeneratorId: g.demandGeneratorId,
    programName: "Single award mobilization",
    programType: "PROJECT_DEPLOYMENT",
    recurrenceStatus: "ONE_TIME",
  });
  assert.equal(p.recurrenceStatus, "ONE_TIME");
});

check("university_recurring_residential", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "State University",
    organizationType: "UNIVERSITY",
    officialDomain: "stateu.edu",
  });
  const p = buildProgramEntity({
    demandGeneratorId: g.demandGeneratorId,
    programName: "Parents Weekend",
    programType: "UNIVERSITY_RESIDENTIAL",
    recurrenceStatus: "RECURRING_CONFIRMED",
  });
  assert.ok(p.programId);
});

check("sports_org_annual_tournament", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Metro Soccer Club",
    organizationType: "SPORTS_ORGANIZATION",
    officialDomain: "metrosoccer.example",
  });
  const p = buildProgramEntity({
    demandGeneratorId: g.demandGeneratorId,
    programName: "Spring Cup",
    programType: "SPORTS_TOURNAMENT_SERIES",
    recurrenceStatus: "RECURRING_CONFIRMED",
  });
  assert.equal(p.programType, "SPORTS_TOURNAMENT_SERIES");
});

check("consulting_firm_no_public_program", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Advisory Partners",
    organizationType: "CONSULTING_FIRM",
    officialDomain: "advisory.example",
  });
  assert.equal(g.organizationType, "CONSULTING_FIRM");
  const eval_ = evaluateGeneratorRelevance({
    generatorRelevance: "MEDIUM",
    recurrence: "UNKNOWN",
    marketRelevance: "MEDIUM",
    likelyTravelCreation: "MEDIUM",
    programObservability: "LOW",
    buyerAccessibility: "LOW",
    evidenceQuality: "LOW",
  });
  assert.ok(
    eval_.relevanceClass === GENERATOR_RELEVANCE_CLASS.WATCH_GENERATOR ||
      eval_.relevanceClass === GENERATOR_RELEVANCE_CLASS.LOW_VALUE_GENERATOR
  );
});

check("historical_recurring_no_future_not_invented", () => {
  const guard = assertNoInventedFuture({
    recurrenceStatus: "RECURRING_HISTORICAL",
    inventFromHistoryOnly: true,
  });
  assert.equal(guard.ok, false);
  assert.throws(() =>
    buildProgramEntity({
      demandGeneratorId: "dg_test",
      programName: "Old Series",
      recurrenceStatus: "RECURRING_HISTORICAL",
      confirmedFutureCycle: { year: 2027, invented: true },
    })
  );
});

check("confirmed_new_future_cycle_ok", () => {
  const p = buildProgramEntity({
    demandGeneratorId: "dg_test",
    programName: "Annual Meeting",
    recurrenceStatus: "RECURRING_CONFIRMED",
    confirmedFutureCycle: {
      year: 2027,
      startDate: "2027-06-01",
      invented: false,
      sourceUrl: "https://example.org/2027",
    },
  });
  assert.equal(p.confirmedFutureCycle.year, 2027);
});

check("same_generator_two_hotels_different_fit", () => {
  const id = computeDemandGeneratorId({
    organizationName: "Shared Org",
    officialDomain: "shared.org",
  });
  const a = buildHotelDemandGeneratorFit({
    hotelId: "hotel_a",
    demandGeneratorId: id,
    factors: {
      marketRelevance: "CORE",
      likelyTravelCreation: "HIGH",
      generatorRelevance: "HIGH",
      recurrence: "RECURRING_CONFIRMED",
      programObservability: "HIGH",
      buyerAccessibility: "HIGH",
      evidenceQuality: "HIGH",
    },
  });
  const b = buildHotelDemandGeneratorFit({
    hotelId: "hotel_b",
    demandGeneratorId: id,
    factors: {
      marketRelevance: "STRETCH",
      likelyTravelCreation: "LOW",
      generatorRelevance: "LOW",
      recurrence: "UNKNOWN",
      programObservability: "LOW",
      buyerAccessibility: "LOW",
      evidenceQuality: "LOW",
    },
  });
  assert.notEqual(a.fitId, b.fitId);
  assert.notEqual(a.generatorPriority, b.generatorPriority);
});

check("generator_irrelevant_third_hotel_low", () => {
  const fit = buildHotelDemandGeneratorFit({
    hotelId: "hotel_c",
    demandGeneratorId: "dg_x",
    factors: {
      marketRelevance: "OUTSIDE",
      likelyTravelCreation: "NONE",
      generatorRelevance: "LOW",
      recurrence: "ONE_TIME",
      programObservability: "LOW",
      buyerAccessibility: "LOW",
      evidenceQuality: "LOW",
    },
  });
  assert.equal(fit.relevanceClass, GENERATOR_RELEVANCE_CLASS.LOW_VALUE_GENERATOR);
});

check("trigger_without_lodging_thesis_not_true", () => {
  const q = qualifyGeneratorSignal({
    demandGeneratorId: "dg_1",
    triggerType: "FUTURE_DATES_ANNOUNCED",
    sourceUrl: "https://example.org/event",
    eventStartDate: "2027-09-01",
    marketRelevance: "CORE",
    productFit: "HIGH",
    recommendedAction: "Call organizer",
    lodgingDemandThesis: false,
    hotelDemandThesis: "Dates announced only",
  });
  assert.equal(q.trueActionable, false);
  assert.ok(q.failReasons.includes("NO_LODGING_THESIS"));
});

check("trigger_promoted_after_lodging_evidence", () => {
  const signal = buildDemandGeneratorSignal({
    demandGeneratorId: "dg_1",
    programId: "dgp_1",
    triggerType: "HOUSING_INFO_POSTED",
    title: "Annual Meeting 2027 — housing open",
    sourceUrl: "https://example.org/housing",
    eventStartDate: "2027-09-10",
    marketRelevance: "CORE",
    productFit: "HIGH",
    hotelFitOk: true,
    lodgingDemandThesis: true,
    hotelDemandThesis: "Official housing page posts nearby hotel room block",
    recommendedAction: "Contact housing bureau / sales",
  });
  assert.equal(signal.signalStatus, SIGNAL_STATUS.TRUE_ACTIONABLE);
  const draft = buildGeneratorDrivenOpportunityDraft(
    signal,
    { organizationName: "Example Association", demandGeneratorId: "dg_1" },
    { programId: "dgp_1", programType: "ANNUAL_CONFERENCE" },
    { hotelId: "hotel_a", fitId: "hdgf_1" }
  );
  assert.equal(draft.ok, true);
  assert.equal(draft.opportunity.newToHotel, undefined);
  assert.equal(draft.opportunity.newnessStatus, "NEW_TO_GDI");
  assert.equal(draft.opportunity.recurrenceContextLabel, "Recurring demand source");
});

check("generator_not_customer_facing_entity", () => {
  const g = buildDemandGeneratorEntity({
    organizationName: "Research Target Org",
    officialDomain: "research-target.org",
  });
  assert.equal(g.isCustomerFacing, false);
});

check("discovery_queries_from_hotel_profile_no_hardcode", () => {
  const profile = {
    demandTerritory: { label: "Market X" },
    commercialPriorities: {
      targetSegments: ["Associations", "Medical"],
      demandAnchorFocus: ["Anchor Institution"],
    },
  };
  const qs = buildGeneratorDiscoveryQueries(profile, { maxQueries: 10 });
  assert.ok(qs.length >= 3);
  assert.ok(qs.every((q) => q.query.includes("Market X") || /Anchor|Associations|Medical/i.test(q.query)));
  const guided = buildGeneratorGuidedQueries(
    { organizationName: "Org", officialDomain: "org.example" },
    { programName: "Annual" }
  );
  assert.ok(guided.some((q) => q.query.includes("site:org.example")));
});

check("efficiency_and_incremental_at_bats", () => {
  const eff = compareSearchEfficiency(
    { queries: 40, urls: 200, qualified: 4, trueActionable: 1 },
    { queries: 12, urls: 40, qualified: 5, trueActionable: 2 }
  );
  assert.equal(eff.guidedWinsOnTruePerQuery, true);
  const bats = measureIncrementalAtBats({
    generatorDrivenOpportunityIds: ["a", "b", "c"],
    genericSearchOpportunityIds: ["b", "d"],
  });
  assert.equal(bats.generatorDrivenNew, 3);
  assert.equal(bats.genericSearchOverlap, 1);
  assert.equal(bats.incrementalGeneratorAtBats, 2);
});

check("replication_profiles_different_org_type_mix", () => {
  const beth = loadHotelDemandConfig("recLuxvwwxID7U2B8");
  const ren = loadHotelDemandConfig("recG66DQJKP2c0UNh");
  const cam = loadHotelDemandConfig("recIwaP1etgx2g9nA");
  const tb = organizationTypesForHotelProfile(beth);
  const tr = organizationTypesForHotelProfile(ren);
  const tc = organizationTypesForHotelProfile(cam);
  assert.ok(tb.length > 0 && tr.length > 0 && tc.length > 0);
  // Expect not identical mixes across suburban medical vs urban vs resort
  const sig = (arr) => [...arr].sort().join("|");
  assert.notEqual(sig(tb), sig(tc));
});

check("hotel_specific_logic_audit_clean", () => {
  const audit = auditDemandGeneratorHotelSpecificLogic();
  assert.equal(audit.ok, true, JSON.stringify(audit.findings));
  assert.equal(audit.categories.HOTEL, "NO");
  assert.equal(audit.categories.PERSON, "NO");
});

console.log(`\nDemand Generator V1 tests: ${passed} pass, ${failed} fail`);
if (failed) process.exit(1);
