#!/usr/bin/env node
/**
 * Focused regressions for hotel-fit presentation + narrative consistency.
 *   node scripts/test-gdi-hotel-fit-presentation-v1.mjs
 */
import assert from "node:assert/strict";
import {
  assessHotelFitForOpportunity,
  applyHotelFitAssessmentToOpportunity,
  computeOverallFromAssessedDimensions,
  DIMENSION_STATUS,
} from "../lib/group-demand-intelligence/workflow/hotel-fit-assessment-v1.js";
import {
  detectNarrativeConsistencyIssues,
  repairNarrativeForConsistency,
} from "../lib/group-demand-intelligence/workflow/hotel-fit-narrative-consistency-v1.js";
import { DEFAULT_HOTEL_FIT_WEIGHTS } from "../lib/group-demand-intelligence/scoring.js";

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// 1. Insufficient evidence dims mark overall provisional (do not silently inflate)
{
  const dims = {
    physicalFit: { status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE },
    geographyFit: { status: DIMENSION_STATUS.ASSESSED, score: 40 },
    timing: { status: DIMENSION_STATUS.ASSESSED, score: 60 },
    commercialValue: { status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE },
    historicalFit: { status: DIMENSION_STATUS.ASSESSED, score: 32 },
    competitiveAccessibility: { status: DIMENSION_STATUS.ASSESSED, score: 50 },
    contactability: { status: DIMENSION_STATUS.ASSESSED, score: 80 },
  };
  const overall = computeOverallFromAssessedDimensions(
    dims,
    DEFAULT_HOTEL_FIT_WEIGHTS
  );
  assert.equal(overall.numericScoreEmitted, true);
  assert.equal(overall.provisional, true);
  assert.ok(overall.calculationBasis.insufficientKeys.includes("physicalFit"));
  ok("provisional_when_material_gaps");
}

// 2. LMC prior Mayflower banquet → historical assessed, not N/A
{
  const opp = {
    id: "test_acec",
    hotelId: "rec0eTEnCqlsGJJt9",
    researchSegment: "local_meetings_catering",
    opportunityType: "LOCAL_MEETINGS_CATERING",
    title: "ACEC — Local Meetings",
    organizationName: "ACEC Metropolitan Washington",
    localConnection:
      "Metropolitan Washington engineering association; held 2024 Engineering Excellence Awards Gala at The Mayflower Hotel.",
    hotelOpportunityThesis:
      "Already selected Mayflower for flagship 2024 gala — strongest possible hotel-specific fit for ongoing chapter banquets.",
    contactEmail: "info@acec.org",
  };
  const a = assessHotelFitForOpportunity(opp, {
    hotelId: "rec0eTEnCqlsGJJt9",
  });
  assert.equal(a.dimensions.historicalFit.status, DIMENSION_STATUS.ASSESSED);
  assert.ok(a.dimensions.historicalFit.score >= 80);
  ok("lmc_history_feeds_historical_fit");
}

// 3. Anderson not-named repair
{
  const opp = {
    venueName: "Anderson House",
    organizationName: "Anderson House",
    hotelOpportunityThesis:
      "Anderson House (~0.6 mi from Mayflower) lists nearby Embassy Row hotels; Mayflower is not named — competitive referral opening for Connecticut Ave overnight.",
  };
  const repaired = repairNarrativeForConsistency(opp, {
    hotelName: "The Mayflower Hotel, Autograph Collection",
  });
  assert.match(
    repaired.opportunity.hotelOpportunityThesis,
    /bounded sales hypothesis/i
  );
  assert.doesNotMatch(
    repaired.opportunity.hotelOpportunityThesis,
    /competitive referral opening/i
  );
  ok("anderson_not_named_not_vacancy");
}

// 4. History claim vs N/A flagged
{
  const check = detectNarrativeConsistencyIssues(
    {
      hotelOpportunityThesis:
        "Named local association with proven Mayflower banquet history.",
    },
    {
      dimensions: {
        historicalFit: { status: DIMENSION_STATUS.NOT_APPLICABLE },
      },
    }
  );
  assert.ok(check.issues.some((i) => i.code === "history_claim_not_in_assessment"));
  ok("history_claim_consistency_check");
}

// 5. Apply stamps provisional + consistency bag
{
  const opp = {
    id: "test_stretch",
    hotelId: "rec0eTEnCqlsGJJt9",
    opportunityType: "OVERFLOW_HOUSING",
    title: "Arlington Conference",
    destinationStatus: "Arlington — hotel TBA; downtown DC stretch",
    venueHistoryNote: "WashCon historically Arlington-oriented",
    competitiveContextNote: "Arlington pattern historically",
    contactEmail: "planner@example.com",
    primaryContact: { name: "Jamie" },
  };
  const a = assessHotelFitForOpportunity(opp, {
    hotelId: "rec0eTEnCqlsGJJt9",
  });
  const next = applyHotelFitAssessmentToOpportunity(
    { ...opp, _reassessHotelFit: true },
    a
  );
  assert.equal(next.hotelFitProvisional, true);
  assert.ok(
    next.hotelFitOverallStatus === "provisional" ||
      next.hotelFitPosture === "EXPLORATORY_STRETCH"
  );
  assert.ok(next.hotelFitNarrativeConsistency);
  ok("stretch_marked_provisional");
}

console.log(`\n${passed} passed`);
