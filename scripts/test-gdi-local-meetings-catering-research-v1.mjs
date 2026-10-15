#!/usr/bin/env node
/**
 * Focused verification for Local Meetings & Catering + shared demand model.
 */
import assert from "node:assert/strict";
import {
  assessLocalMeetingsCateringApplicability,
  assessLocalMeetingsCateringConstraints,
  buildLocalMeetingsCateringCoverageSnapshot,
  buildLocalMeetingsCateringResearchQueries,
  qualifyLocalMeetingsCateringCandidate,
  buildLocalMeetingsCateringOpportunityCard,
  LOCAL_CATERING_FAIL,
  LMC_SUBSEGMENT,
  LMC_CONSTRAINT_STATUS,
  qualifyProfessionalPlannerCandidate,
  buildProfessionalPlannerOpportunityCard,
  PLANNER_RELATIONSHIP_STATUS,
  PLANNER_SPECIALTY,
} from "../lib/group-demand-intelligence/workflow/local-meetings-catering-research-v1.js";
import {
  DEMAND_USE_CASE,
  GUESTROOM_STATUS,
  RELATIONSHIP_GRAIN,
  shouldMergeStandingAccountSignals,
  guestroomStatusForUseCases,
} from "../lib/group-demand-intelligence/workflow/shared-demand-model-v1.js";
import { qualifyScalingCompanyCandidate, SCALING_QUALIFY_FAIL } from "../lib/group-demand-intelligence/workflow/scaling-companies-research-v1.js";
import {
  loadHotelConfig,
  buildHotelSegmentResearchPlan,
  RESEARCH_SEGMENT,
} from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";
import { buildHotelSegmentResearchBrief } from "../lib/group-demand-intelligence/workflow/hotel-segment-research-brief-v1.js";
import {
  createEmptyScorecardRow,
  attachLmcSubsegmentCoverage,
} from "../lib/group-demand-intelligence/workflow/research-scorecard-v1.js";

const may = loadHotelConfig("rec0eTEnCqlsGJJt9");
const sd = loadHotelConfig("recnPB3Q02NO8Ma1o");
const beth = loadHotelConfig("recLuxvwwxID7U2B8");

// Hotel with meetings inventory is applicable
const mayApp = assessLocalMeetingsCateringApplicability(may);
const sdApp = assessLocalMeetingsCateringApplicability(sd);
assert.equal(mayApp.applicable, true);
assert.equal(sdApp.applicable, true);
assert.ok((mayApp.applicableSubsegmentIds || []).length >= 4);
assert.ok(
  mayApp.constraints.certifiedKosherCatering ===
    LMC_CONSTRAINT_STATUS.UNKNOWN_NEEDS_VERIFICATION
);
assert.ok(
  assessLocalMeetingsCateringConstraints(may).notesForVerification.includes(
    "certified_kosher_catering_unknown"
  )
);
if (beth) {
  assert.equal(assessLocalMeetingsCateringApplicability(beth).applicable, true);
}

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
assert.equal(card.opportunity.guestroomDimensionsMode, "not_applicable");
assert.ok(card.opportunity.lmcSubsegment);
assert.equal(card.opportunity.persistPath, "demand_generator");

// PE preferred-lodging misroute rejected from LMC
const peMisroute = qualifyLocalMeetingsCateringCandidate(
  {
    organizationName: "Garden Venue",
    localConnection: "Nearby",
    eventActivityEvidence: "Hosts weddings",
    hotelFitRationale: "Overflow rooms",
    whyThisMatters: "Partner",
    recommendedAction: "Call",
    contactEmail: "sales@garden.org",
    preferredLodgingPartnership: true,
  },
  may
);
assert.ok(
  peMisroute.failReasons.includes(LOCAL_CATERING_FAIL.PE_VENUE_REFERRAL_MISROUTE)
);

// Religion / private-family inference rejected
const privateInfer = qualifyLocalMeetingsCateringCandidate(
  {
    organizationName: "Family Event",
    localConnection: "Local",
    eventActivityEvidence: "Celebration",
    hotelFitRationale: "Ballroom",
    whyThisMatters: "Social",
    recommendedAction: "Email",
    contactEmail: "events@example.org",
    inferredReligion: true,
  },
  may
);
assert.ok(
  privateInfer.failReasons.includes(
    LOCAL_CATERING_FAIL.PRIVATE_FAMILY_OR_RELIGION_INFERENCE
  )
);

// Planner path + org fallback warning
const plannerCard = buildLocalMeetingsCateringOpportunityCard(
  {
    organizationName: "Capitol Planners Co",
    localConnection: "DC-based planner",
    eventActivityEvidence: "Recurring corporate and social event planning portfolio",
    hotelFitRationale: "Mayflower ballroom and meeting rooms fit planner client mix",
    whyThisMatters: "Standing intermediary with recurring banquet demand",
    recommendedAction: "Ask which hotels they currently book for chapter dinners",
    contactUrl: "https://capitolplanners.example/contact",
    contactEmail: "info@capitolplanners.example",
    lmcSubsegment: LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES,
    guestroomStatus: GUESTROOM_STATUS.NOT_REQUIRED,
    publish: true,
  },
  { hotelId: may.hotelId, hotelConfig: may }
);
assert.equal(plannerCard.ok, true);
assert.equal(
  plannerCard.opportunity.lmcSubsegment,
  LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES
);
assert.equal(plannerCard.opportunity.persistPath, "demand_generator_not_pe_venue");
assert.equal(plannerCard.opportunity.relationshipGrain, RELATIONSHIP_GRAIN.STANDING_ACCOUNT);
assert.equal(plannerCard.qualification.contactIsOrgFallback, true);

// Research queries cover applicable subsegments; kosher note present
const queries = buildLocalMeetingsCateringResearchQueries(may);
assert.ok(queries.some((q) => q.subsegment === LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS));
assert.ok(queries.some((q) => q.intent === "constraint_verification"));

// Plan + brief carry subsegment coverage
const plan = buildHotelSegmentResearchPlan({
  hotelId: may.hotelId,
  hotelConfig: may,
  opportunities: [],
});
const lmcPlan = plan.segments.find(
  (s) => s.segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
);
assert.ok((lmcPlan.applicableLmcSubsegmentIds || []).length >= 1);
const brief = buildHotelSegmentResearchBrief({
  hotelId: may.hotelId,
  segmentId: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
  hotelConfig: may,
  segmentPlanRow: lmcPlan,
});
assert.ok(brief.lmcCoverageSnapshot);
assert.ok(Array.isArray(brief.researchQueries));

const coverage = buildLocalMeetingsCateringCoverageSnapshot(may);
const scoreRow = createEmptyScorecardRow({
  hotelId: may.hotelId,
  segmentId: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
});
attachLmcSubsegmentCoverage(scoreRow, coverage);
assert.ok(scoreRow.scope.lmcSubsegmentCoverage);
assert.ok(
  scoreRow.scope.coverageGaps.some((g) =>
    String(g).startsWith("lmc_subsegment_not_researched:")
  )
);

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

// Professional planner: directory-only fails; qualified planner is DG not PE venue
const plannerDir = qualifyProfessionalPlannerCandidate(
  {
    organizationName: "Top 10 Planners Inc",
    localConnection: "Listed downtown",
    eventActivityEvidence: "Appears on top planner list",
    hotelFitRationale: "Nearby",
    whyThisMatters: "Maybe",
    recommendedAction: "Call",
    contactEmail: "info@top10.example",
    directoryRankingOnly: true,
  },
  may
);
assert.ok(plannerDir.failReasons.includes(LOCAL_CATERING_FAIL.DIRECTORY_ONLY));

const plannerOk = buildProfessionalPlannerOpportunityCard(
  {
    organizationName: "Harbor Event Advisors",
    localConnection: "Active DC corporate and association programs",
    eventActivityEvidence:
      "Public portfolio shows 2024–2025 chapter dinners and corporate offsites in downtown DC",
    hotelFitRationale:
      "Mayflower ballroom and meeting rooms fit association dinner scale",
    whyThisMatters: "Standing intermediary with recurring banquet demand",
    recommendedAction:
      "Ask sales if they know Harbor Event Advisors; introduce banquet product",
    contactEmail: "hello@harborevents.example",
    contactName: "Jordan Lee",
    contactRole: "Principal planner",
    plannerSpecialties: [
      PLANNER_SPECIALTY.CORPORATE_MEETING_EVENT,
      PLANNER_SPECIALTY.ASSOCIATION_MANAGEMENT_MEETING,
    ],
    publish: true,
  },
  { hotelId: may.hotelId, hotelConfig: may }
);
assert.equal(plannerOk.ok, true);
assert.equal(
  plannerOk.opportunity.persistPath,
  "demand_generator_not_pe_venue"
);
assert.equal(
  plannerOk.qualification.relationshipStatus,
  PLANNER_RELATIONSHIP_STATUS.RELATIONSHIP_NOT_YET_CONFIRMED
);
assert.equal(
  plannerOk.opportunity.lmcSubsegment,
  LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES
);

console.log("test-gdi-local-meetings-catering-research-v1: PASS");
