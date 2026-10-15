#!/usr/bin/env node
/**
 * Focused verification for Local Meetings & Catering + shared demand model.
 */
import assert from "node:assert/strict";
import {
  assessLocalMeetingsCateringApplicability,
  qualifyLocalMeetingsCateringCandidate,
  buildLocalMeetingsCateringOpportunityCard,
  LOCAL_CATERING_FAIL,
} from "../lib/group-demand-intelligence/workflow/local-meetings-catering-research-v1.js";
import {
  DEMAND_USE_CASE,
  GUESTROOM_STATUS,
  shouldMergeStandingAccountSignals,
  guestroomStatusForUseCases,
} from "../lib/group-demand-intelligence/workflow/shared-demand-model-v1.js";
import { qualifyScalingCompanyCandidate, SCALING_QUALIFY_FAIL } from "../lib/group-demand-intelligence/workflow/scaling-companies-research-v1.js";
import { loadHotelConfig } from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";

const may = loadHotelConfig("rec0eTEnCqlsGJJt9");
const sd = loadHotelConfig("recnPB3Q02NO8Ma1o");

// Hotel with meetings inventory is applicable
assert.equal(assessLocalMeetingsCateringApplicability(may).applicable, true);
assert.equal(assessLocalMeetingsCateringApplicability(sd).applicable, true);

// No-guestroom catering can qualify
const cateringOk = qualifyLocalMeetingsCateringCandidate(
  {
    organizationName: "Test Chapter",
    localConnection: "Downtown chapter near hotel",
    eventActivityEvidence: "Annual awards dinner with plated banquet",
    hotelFitRationale: "Ballroom capacity fits chapter gala",
    whyThisMatters: "Recurring local F&B account",
    recommendedAction: "Email events@test.org to introduce banquet sales",
    contactEmail: "events@test.org",
    guestroomStatus: GUESTROOM_STATUS.NOT_REQUIRED,
    demandUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING],
  },
  may
);
assert.equal(cateringOk.ok, true);
assert.equal(cateringOk.guestroomStatus, GUESTROOM_STATUS.NOT_REQUIRED);

const card = buildLocalMeetingsCateringOpportunityCard(
  {
    organizationName: "Test Chapter",
    localConnection: "Downtown chapter near hotel",
    eventActivityEvidence: "Annual awards dinner with plated banquet",
    hotelFitRationale: "Ballroom capacity fits chapter gala",
    whyThisMatters: "Recurring local F&B account",
    recommendedAction: "Email events@test.org",
    contactEmail: "events@test.org",
    guestroomStatus: GUESTROOM_STATUS.NOT_REQUIRED,
    demandUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING],
    publish: true,
  },
  { hotelId: may.hotelId, hotelConfig: may }
);
assert.equal(card.ok, true);
assert.equal(card.opportunity.cateringOnly, true);
assert.equal(card.opportunity.roomDemandStatus, "NOT_REQUIRED");

// Generic directory cannot qualify
const directory = qualifyLocalMeetingsCateringCandidate(
  {
    organizationName: "Random Co",
    localConnection: "Listed downtown",
    eventActivityEvidence: "Appears in chamber directory",
    hotelFitRationale: "Nearby",
    whyThisMatters: "Maybe meetings",
    recommendedAction: "Call them",
    contactEmail: "info@random.co",
    sourceUrl: "https://www.yelp.com/biz/random",
  },
  may
);
assert.ok(directory.failReasons.includes(LOCAL_CATERING_FAIL.DIRECTORY_ONLY));

// Nearby venue is not automatically a catering buyer
const venueBuyer = qualifyLocalMeetingsCateringCandidate(
  {
    organizationName: "Some Museum Venue",
    localConnection: "Nearby",
    eventActivityEvidence: "Hosts private events",
    hotelFitRationale: "Close",
    whyThisMatters: "Partnership?",
    recommendedAction: "Email",
    contactEmail: "events@museum.org",
    nearbyVenueAsBuyer: true,
  },
  may
);
assert.ok(venueBuyer.failReasons.includes(LOCAL_CATERING_FAIL.VENUE_AS_BUYER));

// Funding-only startup cannot qualify as scaling (no local ops expansion)
const fundingOnly = qualifyScalingCompanyCandidate(
  {
    organizationName: "FundedCo",
    datedGrowthEvidence: "Raised Series B",
    growthEvidenceDate: "2025-01-01",
    growthSignals: ["FUNDING_WITH_OPERATING_EXPANSION"],
    proposedHotelUse: "Maybe travel",
    hotelFitRationale: "Downtown hotel",
    sourceUrl: "https://techcrunch.com/fundedco",
  },
  may
);
assert.ok(
  fundingOnly.failReasons.includes(SCALING_QUALIFY_FAIL.FUNDING_ONLY) ||
    fundingOnly.failReasons.includes(SCALING_QUALIFY_FAIL.MISSING_LOCAL_CONNECTION)
);

// Growth + local + use + contact can qualify (scaling)
const scalingOk = qualifyScalingCompanyCandidate(
  {
    organizationName: "ExpandCo",
    localConnection: "Opened DC office at 14th St",
    datedGrowthEvidence: "Opened DC office Sep 2024; hiring 50",
    growthEvidenceDate: "2024-09-18",
    growthSignals: ["NEW_OFFICE_OR_LOCAL_EXPANSION"],
    operatingExpansionEvidence: true,
    proposedHotelUse: "Visiting employees and training",
    hotelFitRationale: "Walk to Mayflower",
    contactEmail: "ops@expand.co",
  },
  may
);
assert.equal(scalingOk.ok, true);

// Same standing account merges; distinct dated occasions do not
assert.equal(
  shouldMergeStandingAccountSignals({
    existing: {
      hotelId: "h1",
      organizationName: "Acme",
      relationshipGrain: "STANDING_ACCOUNT",
    },
    candidate: {
      hotelId: "h1",
      organizationName: "Acme",
      relationshipGrain: "STANDING_ACCOUNT",
    },
  }),
  true
);
assert.equal(
  shouldMergeStandingAccountSignals({
    existing: {
      hotelId: "h1",
      organizationName: "Acme",
      relationshipGrain: "DATED_OCCASION",
      eventStartDate: "2026-01-01",
    },
    candidate: {
      hotelId: "h1",
      organizationName: "Acme",
      relationshipGrain: "DATED_OCCASION",
      eventStartDate: "2026-06-01",
    },
  }),
  false
);

assert.equal(
  guestroomStatusForUseCases([DEMAND_USE_CASE.MEETINGS_AND_CATERING]),
  GUESTROOM_STATUS.NOT_REQUIRED
);

console.log("test-gdi-local-meetings-catering-research-v1: PASS");
