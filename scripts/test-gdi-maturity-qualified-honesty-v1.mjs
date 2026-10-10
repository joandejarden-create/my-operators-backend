/**
 * test-gdi-maturity-qualified-honesty-v1.mjs
 */
import assert from "node:assert/strict";
import {
  evaluateGdiMaturityQualified,
  TRAVELING_COHORT_TYPE,
  LODGING_CONTROL_HYPOTHESIS,
} from "../lib/group-demand-intelligence/gdi-maturity-qualified-v1.js";
import { ACCOUNT_QUALITY_CLASS } from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import { GDI_EVIDENCE_TYPE } from "../lib/group-demand-intelligence/gdi-evidence-taxonomy-v1.js";

function goodQualifiedBase() {
  return {
    title: "Red Bull — SailGP Team Partner",
    organizationName: "Red Bull",
    officialSource: "https://sailgp.com/teams/italy/",
    participationRole: "TEAM_PARTNER",
    accountQualityClass: ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
    hotelFitScore: 82,
    fitExplanation: "VIP hospitality fit",
    summaryWhat:
      "Red Bull is a published SailGP team partner with a traveling race-week hospitality cohort.",
    summaryWhyHotel: "Lifestyle brand + suite demand",
    whyNow: "Rome GP dates announced for 2027",
    recommendedAction: "Qualify team hospitality desk",
    evidenceItems: [
      {
        claimKind: "FACT",
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TEAM,
        sourceUrl: "https://sailgp.com/teams/italy/",
        excerpt: "Red Bull Italy SailGP Team",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.SPORTS_TEAM,
    travelingCohortSummary:
      "Race team plus sponsor VIP hospitality guests for the Rome GP weekend",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: "INFERRED",
        evidenceType: "INFERRED_TRAVEL",
        excerpt: "Race-week traveling team + hospitality",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 10,
    modeledRoomsMax: 25,
    modeledDemandBasis: "Modeled band — lodging not verified",
    modeledDemandDisclaimer: "Estimated 10–25 rooms. Modeled demand — lodging not verified.",
    lodgingVerified: false,
    headcountVerified: false,
    missingValidation: [
      "lodging_block_unverified",
      "headcount_unverified",
      "lodging_controller_unknown",
      "named_buyer_person_missing",
    ],
    peakRoomsClaimKind: "MODELED",
  };
}

// Happy QUALIFIED
{
  const r = evaluateGdiMaturityQualified(goodQualifiedBase());
  assert.equal(r.ok, true, r.failed.join(","));
  assert.ok(r.missingValidation.includes("lodging_block_unverified"));
}

// Modeled rooms cannot appear as confirmed
{
  const o = goodQualifiedBase();
  o.lodgingVerified = true;
  o.whyNow = "Red Bull needs 15 rooms";
  o.peakRoomsClaimKind = "FACT";
  const r = evaluateGdiMaturityQualified(o);
  assert.equal(r.ok, false);
  assert.ok(
    r.failed.some((f) =>
      /speculative_rooms|lodging_verified_without|modeled_rooms/.test(f)
    ),
    r.failed.join(",")
  );
}

// Cohort required
{
  const o = goodQualifiedBase();
  delete o.travelingCohortType;
  delete o.travelingCohortSummary;
  delete o.travelingCohortEvidence;
  const r = evaluateGdiMaturityQualified(o);
  assert.equal(r.ok, false);
  assert.ok(r.failed.includes("traveling_cohort_missing"));
}

// Lodging-control hypothesis required
{
  const o = goodQualifiedBase();
  delete o.lodgingControlHypothesis;
  const r = evaluateGdiMaturityQualified(o);
  assert.equal(r.ok, false);
  assert.ok(r.failed.includes("lodging_control_hypothesis_missing"));
}

// Venue/operator shells fail
{
  const o = goodQualifiedBase();
  o.organizationName = "Palexpo SA";
  o.participationRole = "VENUE_OPERATOR";
  o.title = "Palexpo SA (VENUE OPERATOR)";
  delete o.accountQualityClass;
  const r = evaluateGdiMaturityQualified(o);
  assert.equal(r.ok, false);
  assert.ok(
    r.failed.some((f) => /account_quality|shell:VENUE/.test(f)),
    r.failed.join(",")
  );
}

// Generator wrapper fails
{
  const o = goodQualifiedBase();
  o.organizationName = "Sponsors";
  o.participationRole = "ORGANIZER";
  delete o.accountQualityClass;
  const r = evaluateGdiMaturityQualified(o);
  assert.equal(r.ok, false);
}

// missingValidation required for unresolved lodging
{
  const o = goodQualifiedBase();
  o.missingValidation = [];
  const r = evaluateGdiMaturityQualified(o);
  // buildMissingValidation synthesizes entries — ok if synthesized
  assert.ok(r.missingValidation.includes("lodging_block_unverified"));
}

console.log("test-gdi-maturity-qualified-honesty-v1: PASS");
