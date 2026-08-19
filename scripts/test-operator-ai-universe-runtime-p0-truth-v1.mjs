/**
 * Test: Operator AI Universe Runtime + P0 Truth Expansion V1
 * Covers: route registration, universe payload, truth classification,
 * Arbor blocks, comparability, gap safety, prompt moat, brand regression.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { buildOperatorCustomerUniversePayload } from "../lib/ai-visibility/operator-intelligence/operator-customer-read-service.js";
import { extractOperatorCompetitiveGapCandidates, summarizeGapCandidates } from "../lib/ai-visibility/operator-intelligence/gaps.js";
import { loadCertifiedOperatorPresenceCorpus } from "../lib/ai-visibility/operator-intelligence/competitive-intelligence.js";
import { listScenarioEligibilityMatrix, summarizeScenarioEligibilityMatrix } from "../lib/ai-visibility/operator-intelligence/eligibility.js";
import { auditP0TruthExpansion, p0TruthExpansionSummary, P0_CAPABILITY_TRUTH } from "../lib/ai-visibility/operator-intelligence/p0-truth-expansion-v1.js";
import { OPERATOR_AI_UNIVERSE } from "../lib/ai-visibility/operator-intelligence/universe.js";
import { ARBOR_LODGING_ID } from "../lib/ai-visibility/operator-intelligence/comparability.js";

describe("Operator AI Universe Runtime", () => {
  it("universe payload returns 9 operators", () => {
    const payload = buildOperatorCustomerUniversePayload();
    assert.equal(payload.ok, true);
    assert.equal(payload.operators.length, 9);
  });

  it("universe payload has required customer-safe fields", () => {
    const payload = buildOperatorCustomerUniversePayload();
    for (const op of payload.operators) {
      assert.ok(op.operatorId, "missing operatorId");
      assert.ok(op.name, "missing name");
      assert.ok(op.slug, "missing slug");
    }
  });

  it("universe payload does not leak prompt data or truth diagnostics", () => {
    const text = JSON.stringify(buildOperatorCustomerUniversePayload());
    assert.ok(!text.includes("rawPrompt"), "rawPrompt leak");
    assert.ok(!text.includes("canonicalPrompt"), "canonicalPrompt leak");
    assert.ok(!text.includes("goldLabel"), "goldLabel leak");
    assert.ok(!text.includes("CORE_COMPARABLE_RELATIONSHIPS"), "comparability matrix leak");
  });
});

describe("P0 Truth Expansion — Luxury", () => {
  it("classifies all 9 operators", () => {
    const audit = auditP0TruthExpansion();
    assert.equal(audit.operators.length, 9);
    for (const op of audit.operators) {
      assert.ok(op.luxury, `missing luxury for ${op.name}`);
    }
  });

  it("Marriott/IHG/Hilton have PRODUCTION_VALIDATED luxury", () => {
    const truth = P0_CAPABILITY_TRUTH;
    assert.equal(truth.recGmiPhRt6hiayd9.luxuryCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.rec7IXYQYpKMYsrDl.luxuryCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.rec3Uwxe6ovpiokuN.luxuryCapability, "PRODUCTION_VALIDATED");
  });

  it("HE/Arbor/GHL/Brittain have INSUFFICIENT_TRUTH for luxury", () => {
    const truth = P0_CAPABILITY_TRUTH;
    assert.equal(truth.recWPKu5laVZxsvpn.luxuryCapability, "INSUFFICIENT_TRUTH");
    assert.equal(truth.recF5Z87OAqFgndoq.luxuryCapability, "INSUFFICIENT_TRUTH");
    assert.equal(truth.reciI2tYQBfMoMK9G.luxuryCapability, "INSUFFICIENT_TRUTH");
    assert.equal(truth.receHCdI6CEsJqdG4.luxuryCapability, "INSUFFICIENT_TRUTH");
  });
});

describe("P0 Truth Expansion — Resort", () => {
  it("Marriott/IHG/Hilton/Aimbridge/GHL/Brittain have PRODUCTION_VALIDATED resort", () => {
    const truth = P0_CAPABILITY_TRUTH;
    assert.equal(truth.recGmiPhRt6hiayd9.resortCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.rec7IXYQYpKMYsrDl.resortCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.rec3Uwxe6ovpiokuN.resortCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.recGWxIJqnYHkJZFD.resortCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.reciI2tYQBfMoMK9G.resortCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.receHCdI6CEsJqdG4.resortCapability, "PRODUCTION_VALIDATED");
  });

  it("Arbor has INSUFFICIENT_TRUTH for resort", () => {
    assert.equal(P0_CAPABILITY_TRUTH.recF5Z87OAqFgndoq.resortCapability, "INSUFFICIENT_TRUTH");
  });
});

describe("P0 Truth Expansion — Independent Hotel", () => {
  it("all TPMs have PRODUCTION_VALIDATED independent hotel", () => {
    const truth = P0_CAPABILITY_TRUTH;
    assert.equal(truth.recGWxIJqnYHkJZFD.independentHotelCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.recWPKu5laVZxsvpn.independentHotelCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.recF5Z87OAqFgndoq.independentHotelCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.receHCdI6CEsJqdG4.independentHotelCapability, "PRODUCTION_VALIDATED");
    assert.equal(truth.rec6UB6RpMKSs2tAo.independentHotelCapability, "PRODUCTION_VALIDATED");
  });

  it("brand-managed operators have SUPPORTED_BUT_NOT_PRODUCTION", () => {
    const truth = P0_CAPABILITY_TRUTH;
    assert.equal(truth.recGmiPhRt6hiayd9.independentHotelCapability, "SUPPORTED_BUT_NOT_PRODUCTION");
    assert.equal(truth.rec7IXYQYpKMYsrDl.independentHotelCapability, "SUPPORTED_BUT_NOT_PRODUCTION");
    assert.equal(truth.rec3Uwxe6ovpiokuN.independentHotelCapability, "SUPPORTED_BUT_NOT_PRODUCTION");
  });

  it("GHL has SUPPORTED_BUT_NOT_PRODUCTION (mixed model)", () => {
    assert.equal(P0_CAPABILITY_TRUTH.reciI2tYQBfMoMK9G.independentHotelCapability, "SUPPORTED_BUT_NOT_PRODUCTION");
  });
});

describe("P0 Truth Expansion — Arbor Safety", () => {
  it("Arbor competitive claims remain blocked", () => {
    const summary = p0TruthExpansionSummary();
    assert.equal(summary.arborStatus, "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE");
    assert.equal(summary.arborCompetitiveClaims, "BLOCKED");
  });
});

describe("P0 Truth Expansion — Gap Regression", () => {
  it("client-promoted gaps remain at 8", () => {
    const corpus = loadCertifiedOperatorPresenceCorpus();
    const candidates = extractOperatorCompetitiveGapCandidates(corpus.extractions);
    const promoted = candidates.filter((c) => c.clientPromoted);
    assert.equal(promoted.length, 8);
  });

  it("no new client-promoted gaps from P0 truth alone", () => {
    const summary = p0TruthExpansionSummary();
    assert.equal(summary.clientPromotedAfter, 8);
    assert.equal(summary.newClientPromoted, 0);
  });
});

describe("P0 Truth Expansion — Questions Missing Regression", () => {
  it("preserves 108 operator × scenario pairs", () => {
    const matrix = listScenarioEligibilityMatrix();
    assert.equal(matrix.length, 108);
  });

  it("preserves eligibility summary", () => {
    const summary = summarizeScenarioEligibilityMatrix();
    assert.equal(summary.totalOperatorScenarioPairs, 108);
    assert.equal(summary.eligible, 64);
    assert.equal(summary.conditionallyEligible, 31);
    assert.equal(summary.outOfScope, 12);
    assert.equal(summary.insufficientTruth, 1);
  });
});

describe("P0 Truth Expansion — No Provider Calls", () => {
  it("summary confirms zero spend", () => {
    const summary = p0TruthExpansionSummary();
    assert.equal(summary.providerCalls, 0);
    assert.equal(summary.spend, "$0");
  });
});

describe("P0 Truth Expansion — Scenario Policy Unchanged", () => {
  it("luxury/resort remain customer-ineligible for gaps", () => {
    const summary = p0TruthExpansionSummary();
    assert.ok(summary.policyBlockedScenarios.includes("luxury"));
    assert.ok(summary.policyBlockedScenarios.includes("resort"));
  });
});
