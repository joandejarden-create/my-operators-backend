#!/usr/bin/env node
/**
 * Regression gate for GDI 20-opp Phase 2 live-research PREPARATION.
 * Mocked providers only. Actual paid spend must be $0.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PREP_EXPERIMENT_ID,
  MAX_PER_CASE_USD,
  MAX_TOTAL_USD,
  FIVE_CASE_SELECTION,
  PROVIDER_ALLOWLIST,
  assertResearchAuthorization,
  createExperimentBudgetLedger,
  reserveCaseBudget,
  settleCaseBudget,
  remainingExperimentBudget,
  runIndependentEvidenceChallenger,
  scorePhase2Arm,
  loadFrozenCase,
  loadScoringBiasAudit,
  loadEligibilityFlipAudit,
  runFiveCasePrepExperiment,
  buildAuthorizedExecutionCommand,
} from "../lib/group-demand-intelligence/experiments/gdi-20opp-live-research-prep-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(FIVE_CASE_SELECTION.length, 5);
assert.equal(MAX_PER_CASE_USD, 4);
assert.equal(MAX_TOTAL_USD, 20);
assert.ok(PROVIDER_ALLOWLIST.livePublicZeroCost.includes("official_page"));
assert.ok(PROVIDER_ALLOWLIST.paidRequiresSeparateAuth.includes("exa"));

const slots = new Set(FIVE_CASE_SELECTION.map((c) => c.slot));
assert.ok(slots.has("incomplete_demand_evidence"));
assert.ok(slots.has("missing_verified_contact"));
assert.ok(slots.has("forward_looking_demand"));
assert.ok(slots.has("rejected_unsupported"));
assert.ok(slots.has("cross_hotel_booked"));

for (const c of FIVE_CASE_SELECTION) {
  const frozen = loadFrozenCase(c.opportunityId);
  assert.equal(frozen.opportunityId, c.opportunityId);
  assert.ok(frozen.frozenOpportunity);
}

const mockAuth = assertResearchAuthorization({ mode: "mock" });
assert.equal(mockAuth.ok, true);
assert.equal(mockAuth.hardCeilingUsd, 0);

const paidBlocked = assertResearchAuthorization({
  mode: "mock",
  authorizePaidResearch: true,
  paidAuthorizationId: "TEST_AUTH",
  env: {},
});
assert.equal(paidBlocked.authorizePaidResearch, false);

const ledger = createExperimentBudgetLedger({ hardCeilingUsd: 20, maxPerCaseUsd: 4 });
const r1 = reserveCaseBudget(ledger, { opportunityId: "a", quotedMaxUsd: 4 });
assert.equal(r1.ok, true);
settleCaseBudget(ledger, { opportunityId: "a", quotedMaxUsd: 4, actualCostUsd: 0 });
assert.equal(ledger.spentUsd, 0);
const r2 = reserveCaseBudget(ledger, { opportunityId: "a", quotedMaxUsd: 4.01 });
assert.equal(r2.ok, false);
assert.equal(r2.reason, "PER_CASE_CEILING");
assert.equal(remainingExperimentBudget(ledger), 20);

const challenger = runIndependentEvidenceChallenger({
  baselineOpportunity: { thesis: "maybe", evidence: [] },
  workingOpportunity: { thesis: "maybe", evidence: [] },
  recovery: { overlaysApplied: { demand: true, buyer: false }, category: "A_ALREADY_SUPPORTED" },
  retrievalRecords: [{ mocked: true, informationFound: true, retrievals: 0 }],
});
assert.equal(challenger.buckets.existingRecoveredEvidence.length, 1);
assert.equal(challenger.buckets.genuinelyNewVerifiedEvidence.length, 0);
assert.equal(challenger.buckets.mockSimulatedFindings.length, 1);

const scored = scorePhase2Arm({
  shared: { status: "RESEARCH_CONTINUATION", salesReady: false },
  challenger,
  costs: { actualPaidUsd: 0 },
  genuinelyNewVerifiedCount: 0,
  recoveryApplied: true,
});
assert.equal(scored.scores.newVerifiedFactualFindings, 0);
assert.equal(scored.diagnostics.recoveryOverlayAppliedDiagnostic, 1);

const bias = loadScoringBiasAudit();
assert.equal(bias.available, true);
const flip = loadEligibilityFlipAudit();
assert.equal(flip.available, true);
assert.equal(flip.opportunityId, "rb_gdi_disc_dc428c1bd4c4e412");

const { report, outDir } = await runFiveCasePrepExperiment({
  root: ROOT,
  mode: "mock",
  authorizeLivePublicResearch: false,
  authorizePaidResearch: false,
});
assert.equal(report.schema, PREP_EXPERIMENT_ID);
assert.equal(report.totals.cases, 5);
assert.equal(report.totals.actualPaidUsdTotal, 0);
assert.equal(report.gatedPaidExecution.status, "NOT_EXECUTED");
assert.equal(report.safety.productionWrites, false);
assert.equal(report.safety.airtableWrites, false);
assert.ok(fs.existsSync(path.join(outDir, "PREP_REPORT.md")));
assert.ok(fs.existsSync(path.join(outDir, "AUTHORIZATION_GATE.md")));
assert.ok(fs.existsSync(path.join(outDir, "FIVE_CASE_MANIFEST.json")));
assert.match(buildAuthorizedExecutionCommand(), /GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH/);

// Paid CLI refusal is covered by runner exit code; here ensure auth refuses paid.
const paidAuth = assertResearchAuthorization({
  authorizePaidResearch: true,
  paidAuthorizationId: "FOUNDER_TEST",
  env: { GDI_20OPP_LIVE_PAID_BUDGET_FOUNDER_AUTH: "FOUNDER_TEST" },
});
assert.equal(paidAuth.refused, true);
assert.equal(paidAuth.authorizePaidResearch, false);

console.log(
  JSON.stringify(
    {
      ok: true,
      experiment: PREP_EXPERIMENT_ID,
      mode: report.auth.mode,
      actualPaidUsdTotal: report.totals.actualPaidUsdTotal,
      fiveIds: FIVE_CASE_SELECTION.map((c) => c.opportunityId),
      scoringBiasDocumented: bias.available,
      eligibilityFlipId: flip.opportunityId,
    },
    null,
    2,
  ),
);
