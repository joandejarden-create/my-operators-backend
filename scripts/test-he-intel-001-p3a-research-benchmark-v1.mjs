#!/usr/bin/env node
/**
 * HE-INTEL-001 P3A — dry-run regression tests ($0 paid).
 */
import assert from "node:assert/strict";
import {
  BENCHMARK_CONTRACT_ID,
  BENCHMARK_ARM,
  BENCHMARK_MODE,
  CLAIM_ACCEPTANCE,
  EVIDENCE_LAYER,
  P3A_POLICY,
  writeGuaranteesP3A,
  assertP3APaidBudgetZero,
  loadP3ASixHotelCohort,
  P3A_SIX_HPC_IDS,
  buildProviderCapabilityMatrix,
  buildClaimEnvelope,
  rejectInferredContact,
  scoreClaims,
  computeCostEfficiency,
  createIsolatedDiscoveryBags,
  createBenchmarkCostLedger,
  assertBenchmarkArmAuthorized,
  proposedMinimumPaidBenchmarkBudget,
  runP3ABenchmarkDryRun,
  createBenchmarkStagingStore,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/index.js";

let passed = 0;
function pass(name) {
  console.log(`PASS ${name}`);
  passed += 1;
}

function test(name, fn) {
  fn();
  pass(name);
}

async function testAsync(name, fn) {
  await fn();
  pass(name);
}

test("p3a_contract_policy_zero_paid", () => {
  assert.equal(BENCHMARK_CONTRACT_ID, "HE_INTEL_001_RESEARCH_METHOD_BENCHMARK_V1");
  assert.equal(P3A_POLICY.paid_budget_usd, 0);
  assert.equal(P3A_POLICY.production_airtable_writes, false);
  assert.equal(P3A_POLICY.canonical_graph_writes, false);
  assert.equal(writeGuaranteesP3A().paid_providers, false);
  assert.equal(assertP3APaidBudgetZero({ paidBudgetUsd: 0 }).ok, true);
  assert.equal(assertP3APaidBudgetZero({ paidBudgetUsd: 1 }).ok, false);
  assert.equal(assertP3APaidBudgetZero({ paidProvidersEnabled: true }).ok, false);
});

test("frozen_six_hotel_cohort_exact_hpc_ids", () => {
  const cohort = loadP3ASixHotelCohort();
  assert.equal(cohort.ok, true);
  assert.equal(cohort.hotels.length, 6);
  assert.deepEqual(cohort.hpcIds, [...P3A_SIX_HPC_IDS]);
  assert.equal(cohort.fullPilotDistinctHotels, 15);
  assert.equal(cohort.fullPilotFounderExperiences, 16);
  assert.equal(cohort.hotels[4].hpcId, "recBJZwMvgbdG7y4F");
  assert.equal(cohort.hotels[4].dualHouse, true);
});

test("provider_capability_matrix_no_secrets", () => {
  const m = buildProviderCapabilityMatrix({ checkEnv: false });
  assert.ok(m.providers.length >= 6);
  const blob = JSON.stringify(m);
  assert.equal(/sk-[a-zA-Z0-9]{10,}/.test(blob), false);
  assert.equal(m.surfeExcluded, true);
  assert.equal(m.paidExecutionAuthorized, false);
});

test("unknown_without_evidence_not_accepted", () => {
  const c = buildClaimEnvelope({
    claimType: "OWNER_ENTITY",
    value: "Maybe Owner LLC",
    supportingSource: { url: null },
  });
  assert.equal(c[EVIDENCE_LAYER.ACCEPTANCE_DECISION], CLAIM_ACCEPTANCE.UNKNOWN);
});

test("inferred_email_rejected", () => {
  const c = rejectInferredContact({
    claimType: "PUBLIC_EMAIL",
    value: "guessed@example.com",
    inferredContact: true,
    supportingSource: { url: "https://example.com" },
  });
  assert.equal(c[EVIDENCE_LAYER.ACCEPTANCE_DECISION], CLAIM_ACCEPTANCE.REJECTED);
});

test("cross_arm_leakage_forbidden", () => {
  const bags = createIsolatedDiscoveryBags([
    BENCHMARK_ARM.EXA,
    BENCHMARK_ARM.PARALLEL_SEARCH,
  ]);
  bags.append(BENCHMARK_ARM.EXA, { claimType: "OWNER_ENTITY", value: "A" });
  const leak = bags.readOther(BENCHMARK_ARM.PARALLEL_SEARCH, BENCHMARK_ARM.EXA);
  assert.equal(leak.ok, false);
  assert.equal(leak.error, "CROSS_ARM_LEAKAGE_FORBIDDEN");
  assert.equal(bags.readOwn(BENCHMARK_ARM.EXA).length, 1);
});

test("cost_unknown_never_zeroed_in_efficiency", () => {
  const e = computeCostEfficiency({
    costUsd: null,
    costUsdStatus: "UNKNOWN",
    verifiedUsefulFacts: 3,
  });
  assert.equal(e.cost_usd_status, "UNKNOWN");
  assert.equal(e.cost_per_verified_useful_fact, null);
});

test("shared_budget_cannot_be_bypassed", () => {
  const ledger = createBenchmarkCostLedger({ allowPaid: false, runUsdCap: 0 });
  assert.equal(ledger.canParallelBypassSharedBudget(), false);
  const paidAttempt = ledger.recordUsage({
    provider: "EXA",
    operation: "search",
    costUsd: 0.004,
  });
  assert.equal(paidAttempt.ok, false);
});

test("paid_arms_refused_in_p3a", () => {
  const r = assertBenchmarkArmAuthorized(BENCHMARK_ARM.EXA, { paidBudgetUsd: 0 });
  assert.equal(r.ok, false);
  assert.equal(r.error, "P3A_PAID_ARM_REFUSED");
  const mock = assertBenchmarkArmAuthorized(BENCHMARK_ARM.MOCK, {});
  assert.equal(mock.ok, true);
});

test("staging_forbids_production_graph_write", () => {
  const store = createBenchmarkStagingStore({ memoryOnly: true, runId: "test" });
  const bad = store.assertNoProductionGraphWrite({ productionAirtableWrite: true });
  assert.equal(bad.ok, false);
  const ok = store.assertNoProductionGraphWrite({});
  assert.equal(ok.ok, true);
});

test("proposed_p3b_budget_not_authorized", () => {
  const b = proposedMinimumPaidBenchmarkBudget();
  assert.equal(b.authorized, false);
  assert.equal(b.hardCapUsd, 5);
  assert.ok(b.estimatedMaxUsd > 0);
});

test("rubric_scores_blind_safe", () => {
  const claims = [
    buildClaimEnvelope({
      claimType: "PROPERTY_IDENTITY",
      value: "X",
      supportingSource: { url: "https://example.com" },
      independentVerification: { ok: true, method: "t" },
      acceptanceDecision: CLAIM_ACCEPTANCE.ACCEPTED,
    }),
  ];
  const s = scoreClaims(claims);
  assert.ok(typeof s.normalized === "number");
  assert.ok("pass" in s);
});

await testAsync("dry_run_harness_six_hotels_two_modes", async () => {
  const report = await runP3ABenchmarkDryRun({
    memoryOnly: true,
    runId: "p3a_test_dry",
    checkEnv: false,
  });
  assert.equal(report.ok, true);
  assert.equal(report.cohort.hotels.length, 6);
  assert.equal(report.leakageViolations.length, 0);
  assert.equal(report.sharedBudgetBypassPossible, false);
  assert.equal(report.guarantees.paid_budget_usd, 0);
  assert.ok(report.dryRunExecuted > 0);
  assert.ok(report.adjudication.blindedPacketCount > 0);
  assert.equal(report.staging.graphWriteGate.ok, true);
  assert.ok(report.p3bExecutionPlan.steps.length >= 4);
  assert.equal(report.costLedger.runUsdStatus, "ZERO_AUTHORIZED");
  // Modes present
  const modes = new Set(report.adjudication.rows.map((r) => r.mode));
  assert.ok(modes.has(BENCHMARK_MODE.INDEPENDENT_DISCOVERY));
  assert.ok(modes.has(BENCHMARK_MODE.INCREMENTAL_PRODUCTION));
});

console.log(`\nOK he-intel-001-p3a-research-benchmark-v1 (${passed} assertions)`);
