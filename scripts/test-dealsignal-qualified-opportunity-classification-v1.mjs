#!/usr/bin/env node
/**
 * Permanent Qualified Opportunity classification — unit gate.
 */
import assert from "node:assert/strict";
import {
  classifyQualifiedOpportunity,
  applyQualifiedClassificationGuard,
  summarizeQualifiedYield,
  QUALIFIED_ASSESSMENT,
} from "../lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js";

let passed = 0;
function ok(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS ${name}`);
}

ok("AC Cartagena completed reflag is not Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "AC Hotel Cartagena",
    opportunityType: "BCT",
    commercialTrigger: "Completed reflagging to AC Hotels",
    decisionEvidence: "Property now operating as AC Hotel Cartagena after conversion completed",
    commercialStatus: "EMERGING",
    countsTowardFirst100: true,
    assessmentLabel: "QUALIFIED_ACTIONABLE",
  });
  assert.equal(g.isQualified, false);
  assert.equal(g.countsTowardFirst100, false);
  assert.ok(g.reasons.includes("COMPLETED_REFLAG_NOT_OPEN_QUALIFIED"));
});

ok("GHL/Sonesta MFA through 2034 is retained not Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "GHL Hoteles — Sonesta Master Franchise Agreement extension through 2034",
    opportunityType: "BCT",
    commercialTrigger: "Sonesta extended Master Franchise Agreement with GHL through 2034",
    decisionEvidence: "Retention of master franchise partnership",
    commercialStatus: "COMMITTED",
  });
  assert.equal(g.isQualified, false);
  assert.equal(g.assessment, QUALIFIED_ASSESSMENT.RETAINED_RENEWED);
});

ok("Already-appointed operator is not Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "Alliance six Real Inns",
    opportunityType: "MCT",
    commercialTrigger: "Aimbridge selected to manage six-hotel portfolio as voco",
    decisionEvidence: "Operator has been appointed; brand signed",
    commercialStatus: "EMERGING",
    engagementPriority: "ENGAGE_NOW",
  });
  assert.equal(g.isQualified, false);
});

ok("Executed management replacement is historical", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "Courtyard New York Midtown East",
    opportunityType: "MCT",
    commercialTrigger: "Completed manager transition to Highgate HMA effective 2026-02-03",
    decisionEvidence: "Historical completed management replacement — closed",
    stream: "HISTORICAL",
  });
  assert.equal(g.isQualified, false);
  assert.ok(
    g.assessment === QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED ||
      g.assessment === QUALIFIED_ASSESSMENT.COMMITTED_CLOSED,
  );
});

ok("Unresolved brand search with pathway is Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "Voir Beach Resort",
    opportunityType: "BCT",
    commercialStatus: "EMERGING",
    engagementPriority: "ENGAGE_NOW",
    decisionUnresolved: true,
    credibleCompetitivePathway: true,
    commercialTrigger: "Owner seeking brand partner; no hotel partner identified",
    decisionEvidence: "Competitive pathway for brand development; unresolved affiliation",
    brandPathway: { relevant: true },
  });
  assert.equal(g.isQualified, true);
  assert.equal(g.assessment, QUALIFIED_ASSESSMENT.QUALIFIED);
});

ok("Emerging without unresolved evidence stays Emerging/Early — not forced Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "FIHO Express conversion intent",
    opportunityType: "BCT",
    commercialStatus: "EMERGING",
    engagementPriority: "BUILD_RELATIONSHIP",
    commercialTrigger: "Stated portfolio intent without hotel-level list",
    decisionEvidence: "Emerging portfolio thesis",
  });
  assert.equal(g.isQualified, false);
  assert.ok(
    g.assessment === QUALIFIED_ASSESSMENT.EARLY_ENGAGEMENT ||
      g.assessment === QUALIFIED_ASSESSMENT.EMERGING_PROSPECTIVE,
  );
});

ok("Guard strips first100 and Qualified label from historical", () => {
  const out = applyQualifiedClassificationGuard({
    hotelName: "AC Hotel Cartagena completed reflagging",
    opportunityType: "BCT",
    commercialTrigger: "Completed reflagging",
    decisionEvidence: "Conversion complete; now operating as AC",
    countsTowardFirst100: true,
    assessmentLabel: "QUALIFIED_ACTIONABLE",
    commercialStatus: "EMERGING",
  });
  assert.equal(out.countsTowardFirst100, false);
  assert.notEqual(out.assessmentLabel, "QUALIFIED_ACTIONABLE");
});

ok("Scorecard does not inflate Qualified from historical Verified facts", () => {
  const s = summarizeQualifiedYield([
    {
      id: "hist",
      hotelName: "AC Hotel Cartagena",
      opportunityType: "BCT",
      commercialTrigger: "Completed reflagging",
      decisionEvidence: "now operating as AC",
      countsTowardFirst100: true,
      assessmentLabel: "QUALIFIED_ACTIONABLE",
    },
    {
      id: "open",
      hotelName: "Baha Mar fourth",
      opportunityType: "BCT",
      decisionUnresolved: true,
      credibleCompetitivePathway: true,
      commercialTrigger: "unnamed brand; seeking partner",
      decisionEvidence: "unresolved brand affiliation; competitive pathway",
      engagementPriority: "ENGAGE_NOW",
      brandPathway: { relevant: true },
    },
  ]);
  assert.equal(s.qualified, 1);
  assert.equal(s.rejectedInflationAttempts, 1);
});

ok("Franchise expiry / contractual watch alone is not Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "Residence Inn Holtsville",
    opportunityType: "BCT",
    commercialStatus: "EMERGING",
    engagementPriority: "ENGAGE_NOW",
    commercialTrigger: "Franchise expiration year 2026",
    decisionEvidence: "No affirmative owner-process evidence of brand RFP; contractual watch",
    brandPathway: { relevant: true },
  });
  assert.equal(g.isQualified, false);
  assert.ok(
    g.reasons.includes("CONTRACTUAL_FLEXIBILITY_ALONE_NOT_QUALIFIED") ||
      g.reasons.includes("DECISION_NOT_EVIDENCED_UNRESOLVED"),
  );
});

ok("Watchlist status is never Qualified", () => {
  const g = classifyQualifiedOpportunity({
    hotelName: "DRH at-will HMA portfolio",
    opportunityType: "MCT",
    commercialStatus: "WATCHLIST",
    decisionUnresolved: true,
    credibleCompetitivePathway: true,
    brandPathway: { relevant: true },
    commercialTrigger: "At-will near-term term ends",
    decisionEvidence: "Contractual flexibility",
  });
  assert.equal(g.isQualified, false);
  assert.equal(g.assessment, QUALIFIED_ASSESSMENT.WATCHLIST);
});

console.log(`\n${passed} tests passed`);
