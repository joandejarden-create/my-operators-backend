/**
 * test-gdi-maturity-transitions-v1.mjs
 * Assert SIGNAL → CANDIDATE → QUALIFIED → ACTIONABLE progression shapes.
 */
import assert from "node:assert/strict";
import {
  assignGdiMaturityState,
  GDI_MATURITY_STATE,
  maturityRank,
  canTransitionMaturity,
  applyGdiMaturityStamp,
  SALES_WORKFLOW_STATE,
} from "../lib/group-demand-intelligence/gdi-maturity-v1.js";
import {
  ACCOUNT_QUALITY_CLASS,
} from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import {
  TRAVELING_COHORT_TYPE,
  LODGING_CONTROL_HYPOTHESIS,
} from "../lib/group-demand-intelligence/gdi-maturity-qualified-v1.js";
import { GDI_EVIDENCE_TYPE } from "../lib/group-demand-intelligence/gdi-evidence-taxonomy-v1.js";

function baseNamed() {
  return {
    id: "gdi_opp_maturity_test_1",
    title: "Acme Corp — SailGP Sponsor Activation",
    organizationName: "Acme Corp",
    officialSource: "https://example-event.org/sponsors/acme",
    discoverySource: "https://example-event.org/sponsors/acme",
    participationRole: "SPONSOR",
    hotelFitScore: 78,
    fitExplanation: "Lifestyle brand activation fits boutique inventory",
    summaryWhat:
      "Acme Corp is a published sponsor for the destination event with a traveling hospitality cohort.",
    summaryWhyHotel: "Premium ADR tolerance and VIP reception space",
    whyNow: "Event dates published; outreach window open for next season",
    recommendedAction: "Qualify hospitality desk before outreach",
    accountQualityClass: ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
    evidenceItems: [
      {
        claimKind: "FACT",
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
        sourceUrl: "https://example-event.org/sponsors/acme",
        excerpt: "Acme listed as official sponsor",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.SPONSOR_ACTIVATION,
    travelingCohortSummary:
      "Brand activation team plus VIP client hospitality guests for race weekend",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: "INFERRED",
        evidenceType: "INFERRED_TRAVEL",
        sourceUrl: "https://example-event.org/sponsors/acme",
        excerpt: "Sponsor hospitality programs typically travel for event week",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Controller unknown; missingValidation explicit",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: "INFERRED",
        evidenceType: "INFERRED_LODGING_CONTROL",
        excerpt: "No published housing desk",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 8,
    modeledRoomsMax: 20,
    modeledDemandBasis: "Modeled sponsor hospitality band — lodging not verified",
    modeledDemandDisclaimer: "Estimated 8–20 rooms. Modeled demand — lodging not verified.",
    lodgingVerified: false,
    headcountVerified: false,
    missingValidation: [
      "lodging_block_unverified",
      "headcount_unverified",
      "lodging_controller_unknown",
      "named_buyer_person_missing",
    ],
    peakRoomsClaimKind: "MODELED",
    customerFacingState: "ACTIVE",
    eventStartDate: "2027-09-11",
    eventEndDate: "2027-09-12",
  };
}

// Transition legality
assert.equal(canTransitionMaturity("SIGNAL", "CANDIDATE"), true);
assert.equal(canTransitionMaturity("CANDIDATE", "QUALIFIED"), true);
assert.equal(canTransitionMaturity("QUALIFIED", "ACTIONABLE"), true);
assert.equal(maturityRank("ACTIONABLE") > maturityRank("QUALIFIED"), true);

// SIGNAL — generator
{
  const r = assignGdiMaturityState({
    id: "gen1",
    title: "SailGP Rome",
    isDemandGenerator: true,
    demandFamily: "DEMAND_GENERATOR",
  });
  assert.equal(r.gdiMaturityState, GDI_MATURITY_STATE.SIGNAL);
}

// SIGNAL — no named account
{
  const r = assignGdiMaturityState({ id: "x", title: "Event only" });
  assert.equal(r.gdiMaturityState, GDI_MATURITY_STATE.SIGNAL);
}

// CANDIDATE — named but missing cohort
{
  const o = baseNamed();
  delete o.travelingCohortType;
  delete o.travelingCohortSummary;
  delete o.travelingCohortEvidence;
  delete o.travelingCohortConfidence;
  o.accountQualityClass = ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL;
  const r = assignGdiMaturityState(o);
  assert.equal(r.gdiMaturityState, GDI_MATURITY_STATE.CANDIDATE);
  assert.ok(maturityRank(r.gdiMaturityState) >= maturityRank("CANDIDATE") - 1);
}

// QUALIFIED — full intermediate bar, not Ready
{
  const o = baseNamed();
  // Ensure Ready fails: no who research / buyer path
  o.publicContactPath = "https://example-event.org/sponsors/acme";
  const r = assignGdiMaturityState(o, { nowDate: "2026-10-05" });
  assert.equal(
    r.gdiMaturityState,
    GDI_MATURITY_STATE.QUALIFIED,
    r.gdiMaturityReason
  );
  assert.notEqual(r.salesWorkflowState, SALES_WORKFLOW_STATE.PURSUING);
  assert.equal(r.salesWorkflowState, SALES_WORKFLOW_STATE.UNTOUCHED);
}

// Stamp does not set PURSUING
{
  const { opportunity, evaluation } = applyGdiMaturityStamp(baseNamed(), {
    nowDate: "2026-10-05",
  });
  assert.equal(opportunity.gdiMaturityState, evaluation.gdiMaturityState);
  assert.equal(opportunity.salesWorkflowState, SALES_WORKFLOW_STATE.UNTOUCHED);
}

console.log("test-gdi-maturity-transitions-v1: PASS");
