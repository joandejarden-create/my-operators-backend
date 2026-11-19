#!/usr/bin/env node
/**
 * HE-INTEL-001 P3B-M1 readiness regressions — mocks only, $0 paid.
 */
import assert from "node:assert/strict";
import {
  P3B_M1_CONTRACT_ID,
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  resolveP3BM1FounderPaidAuthorization,
  loadP3BM1PilotCohort,
  P3B_M1_HPC_IDS,
  createM1SharedBudgetLedger,
  buildP3BM1PlannedOperations,
  buildProposedExecutionCommand,
  normalizeAndFilterNewIntelligence,
  runP3BM1ReadinessValidation,
  probeConfiguredProviderAvailability,
  buildM1GatewayAdapterMap,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";
import {
  createExecutionGateway,
  buildOperationId,
} from "../lib/hotel-intelligence/research-engine-v1/execution-gateway.js";
import { PROVIDER_IDS } from "../lib/hotel-intelligence/research-engine-v1/provider-interface.js";

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

test("m1_contract_caps_and_auth_gate", () => {
  assert.equal(P3B_M1_CONTRACT_ID, "HE_INTEL_001_P3B_M1_LIVE_BENCHMARK_V1");
  assert.equal(P3B_M1_HARD_CAP_USD, 5);
  assert.equal(P3B_M1_PER_HOTEL_CAP_USD, 2.5);
  assert.equal(resolveP3BM1FounderPaidAuthorization({}).ok, false);
  assert.equal(
    resolveP3BM1FounderPaidAuthorization({
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
      confirmPaidExecution: true,
      authorizedBudgetUsd: 5,
    }).ok,
    true
  );
});

test("m1_three_hotel_cohort_exact_ids", () => {
  const c = loadP3BM1PilotCohort();
  assert.equal(c.ok, true);
  assert.deepEqual(c.hpcIds, [...P3B_M1_HPC_IDS]);
  assert.equal(c.subsequentSixHotelCohort.hpcIds.length, 6);
  assert.equal(c.hotels[0].name.includes("Bethesda"), true);
});

test("planned_operations_within_caps", () => {
  const p = buildP3BM1PlannedOperations();
  assert.equal(p.authorized, false);
  assert.ok(p.totals.expectedMaxProviderUsd < P3B_M1_HARD_CAP_USD);
  assert.ok(p.hotelPlans.every((h) => h.withinPerHotelCap));
  assert.ok(buildProposedExecutionCommand().paidExecutionBlockedUntilAuth);
});

test("seed_facts_excluded_from_new_intel_score", () => {
  const scored = normalizeAndFilterNewIntelligence(
    [
      {
        claimType: "PROPERTY_IDENTITY",
        value: "Bethesda Marriott",
        url: "https://example.com/x",
        isSeedFact: true,
      },
      {
        claimType: "OWNER_ENTITY",
        value: "Some Owner LLC",
        url: "https://example.com/owner",
        independentVerification: { ok: true, method: "t" },
      },
    ],
    { seedPropertyNames: ["Bethesda Marriott"] }
  );
  assert.equal(scored.acceptedNew.length, 1);
  assert.equal(scored.acceptedNew[0].DISCOVERED_CLAIM.claimType, "OWNER_ENTITY");
});

test("inferred_contact_never_verified", () => {
  const scored = normalizeAndFilterNewIntelligence([
    {
      claimType: "PUBLIC_EMAIL",
      value: "ceo@guessed.com",
      inferredContact: true,
      url: "https://example.com",
    },
  ]);
  assert.equal(scored.acceptedNew.length, 0);
  assert.ok(scored.incorrectOrRejected.length >= 1);
});

test("availability_probe_no_secrets", () => {
  const a = probeConfiguredProviderAvailability({
    EXA_API_KEY: "secret-should-not-appear",
    PARALLEL_API_KEY: "parallel-secret",
  });
  const blob = JSON.stringify(a);
  assert.equal(blob.includes("secret-should-not-appear"), false);
  assert.equal(blob.includes("parallel-secret"), false);
  assert.equal(a.EXA.keyConfigured, true);
  assert.equal(a.SURFE.forbidden, true);
});

await testAsync("atomic_shared_budget_blocks_overspend", async () => {
  const ledger = createM1SharedBudgetLedger({ runUsdCap: 0.01, perHotelUsdCap: 0.01 });
  const gw = createExecutionGateway({
    ledger,
    adapters: buildM1GatewayAdapterMap({ useMockTransports: true }),
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
    },
  });
  const r = await gw.execute({
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    hotelId: "recLuxvwwxID7U2B8",
    params: { processor: "core" },
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "BUDGET_EXHAUSTED");
});

await testAsync("durable_operation_id_prevents_duplicate_spend", async () => {
  const ledger = createM1SharedBudgetLedger();
  const gw = createExecutionGateway({
    ledger,
    adapters: buildM1GatewayAdapterMap({ useMockTransports: true }),
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
    },
  });
  const opId = buildOperationId({
    caseId: "dup",
    hotelId: "recLuxvwwxID7U2B8",
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    params: { numResults: 8 },
  });
  const a = await gw.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId: "recLuxvwwxID7U2B8",
    caseId: "dup",
    operationId: opId,
    params: { numResults: 8 },
  });
  const b = await gw.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId: "recLuxvwwxID7U2B8",
    caseId: "dup",
    operationId: opId,
    params: { numResults: 8 },
  });
  assert.equal(a.ok, true);
  assert.equal(b.replayed, true);
  assert.equal(ledger.snapshot().costBreakdown.length, 1);
});

await testAsync("concurrent_reserves_respect_shared_cap", async () => {
  const ledger = createM1SharedBudgetLedger({ runUsdCap: 0.01, perHotelUsdCap: 2.5 });
  const gw = createExecutionGateway({
    ledger,
    adapters: buildM1GatewayAdapterMap({ useMockTransports: true }),
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
    },
  });
  const rs = await Promise.all(
    [0, 1, 2, 3].map((i) =>
      gw.execute({
        provider: PROVIDER_IDS.EXA,
        operation: "search_lite",
        hotelId: "recLuxvwwxID7U2B8",
        caseId: `c${i}`,
        params: { numResults: 8, i },
      })
    )
  );
  const okCount = rs.filter((r) => r.ok).length;
  const refused = rs.filter((r) => !r.ok).length;
  assert.ok(okCount <= 2);
  assert.ok(refused >= 1);
  assert.equal(ledger.snapshot().canParallelBypassSharedBudget, false);
});

await testAsync("full_readiness_validation_report", async () => {
  const report = await runP3BM1ReadinessValidation({ memoryOnly: true });
  assert.equal(report.ok, true, JSON.stringify(report.atomicBudget, null, 2));
  assert.equal(report.paidExecutionAuthorized, false);
  assert.equal(report.founderAuthorizationGate.ok, false);
  assert.equal(report.leakageViolations.length, 0);
  assert.equal(report.atomicBudget.resume.replayed, true);
  assert.equal(report.guarantees.production_airtable_writes, false);
  assert.ok(report.plannedOperations.totals.expectedMaxProviderUsd > 0);
});

console.log(`\nOK he-intel-001-p3b-m1-readiness-v1 (${passed} assertions)`);
