#!/usr/bin/env node
/**
 * HE-INTEL-001 P3B-M1 Live Activation regressions — $0 paid, no live network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  P3B_M1_CONTRACT_VERSION,
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  createDurableM1BudgetLedger,
  OP_STATUS,
  runPhaseASafetySuite,
  runP3BM1LiveActivationValidation,
  buildAuthorizedExecutionCommand,
  buildP3BM1PlannedOperations,
  buildLiveCapableTransports,
  scoreHotelMethodResult,
  factsToClassifierInput,
  loadP3BM1PilotCohort,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";
import {
  createExecutionGateway,
} from "../lib/hotel-intelligence/research-engine-v1/execution-gateway.js";
import { PROVIDER_IDS } from "../lib/hotel-intelligence/research-engine-v1/provider-interface.js";
import {
  OWNERSHIP_SUCCESS_TIER,
} from "../lib/hotel-intelligence/research-engine-v1/ownership-research-objective.js";

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

test("live_activation_contract_version_and_caps", () => {
  assert.equal(P3B_M1_CONTRACT_VERSION, "1.1.0-m1-live-activation");
  assert.equal(P3B_M1_HARD_CAP_USD, 5);
  assert.equal(P3B_M1_PER_HOTEL_CAP_USD, 2.5);
});

await testAsync("phase_a_durable_budget_and_multi_worker_safety", async () => {
  const suite = await runPhaseASafetySuite({});
  assert.equal(suite.ok, true, JSON.stringify(suite, null, 2));
  assert.equal(suite.durableRestart.statusAfterRestart, OP_STATUS.RESERVED);
  assert.ok(suite.concurrent.refusedBudget >= 1);
  assert.equal(suite.pendingReconcile.releaseBlocked, "PENDING_RECONCILE_NO_AUTO_RELEASE");
  assert.equal(suite.opScopedAuth.bypassThrown, true);
  assert.equal(suite.failClosedUnknownCost.ok, false);
  assert.equal(suite.noDuplicateSpend.resumeReplayed, true);
});

await testAsync("durable_ledger_file_persist_across_instances", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-dur-"));
  const persistPath = path.join(dir, "a.json");
  const a = createDurableM1BudgetLedger({ persistPath, memoryOnly: false, runUsdCap: 5 });
  a.reserve({
    operation_id: "op_x",
    hotel_id: "recLuxvwwxID7U2B8",
    provider: "EXA",
    operation: "search_lite",
    cost_usd: 1.25,
    cost_usd_status: "ESTIMATED",
  });
  const b = createDurableM1BudgetLedger({ persistPath, memoryOnly: false, runUsdCap: 5 });
  const op = b.getOperation("op_x");
  assert.equal(op.status, OP_STATUS.RESERVED);
  assert.equal(op.reserved_usd, 1.25);
  const over = b.reserve({
    operation_id: "op_over",
    hotel_id: "recLuxvwwxID7U2B8",
    provider: "EXA",
    operation: "search_lite",
    cost_usd: 4,
    cost_usd_status: "ESTIMATED",
  });
  assert.equal(over.ok, false);
});

await testAsync("op_scoped_auth_not_shared_boolean", async () => {
  const ledger = createDurableM1BudgetLedger({ memoryOnly: true });
  let otherOpSawAuth = false;
  const gwRef = { current: null };
  gwRef.current = createExecutionGateway({
    ledger,
    adapters: {
      [PROVIDER_IDS.EXA]: {
        estimateCost: () => ({
          cost_usd: 0.01,
          cost_usd_status: "ESTIMATED",
          credits: 0,
        }),
        call: async (req) => {
          // Attempt nested direct call with different op id while this op is authorized
          try {
            await gwRef.current.attemptDirectAdapterCall(PROVIDER_IDS.EXA, {
              operation_id: "foreign_op",
              operation: "search_lite",
            });
            otherOpSawAuth = true;
          } catch (err) {
            otherOpSawAuth = err?.code !== "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
          }
          return {
            ok: true,
            cost_usd: 0.01,
            cost_usd_status: "REPORTED",
            provider_cost_usd: 0.01,
            facts: [],
            operation_id: req.operation_id,
          };
        },
      },
    },
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
    },
  });
  const r = await gwRef.current.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId: "recLuxvwwxID7U2B8",
    caseId: "op_scope",
    params: { n: 1 },
  });
  assert.equal(r.ok, true);
  assert.equal(otherOpSawAuth, false);
});

await testAsync("live_transports_blocked_without_network_flag", async () => {
  const t = buildLiveCapableTransports({
    founderAuth: {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
    },
    liveNetworkEnabled: false,
    env: { EXA_API_KEY: "test-key-not-used" },
  });
  let blocked = false;
  try {
    await t.exa({ operation: "search_lite", params: { numResults: 3 } });
  } catch (err) {
    blocked = err?.code === "LIVE_TRANSPORT_BLOCKED";
  }
  assert.equal(blocked, true);
  assert.equal(t.pricingPresence.credentialsExposed, false);
});

test("url_alone_not_ownership_verification", () => {
  const mapped = factsToClassifierInput([
    {
      claimType: "ULTIMATE_OWNER",
      value: "Acme REIT",
      url: "https://example.com/filing",
      verified: false,
      linkedToHpc: true,
    },
  ]);
  const scored = scoreHotelMethodResult({
    hotel: { hpcId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
    method: "EXA",
    facts: [
      {
        claimType: "ULTIMATE_OWNER",
        value: "Acme REIT",
        url: "https://example.com/filing",
        verified: false,
        linkedToHpc: true,
      },
    ],
    cost: { total_usd: 0.1 },
  });
  assert.equal(scored.questions.ultimate_owner_identified, false);
  assert.equal(scored.questions.url_alone_not_verification, true);
  assert.equal(scored.classifier.tier, OWNERSHIP_SUCCESS_TIER.UNRESOLVED);
  assert.equal(mapped.classifierInput.ultimateOwner.verified, false);
});

test("seed_identity_excluded_from_new_intel", () => {
  const scored = scoreHotelMethodResult({
    hotel: { hpcId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
    method: "EXA",
    facts: [
      {
        claimType: "PROPERTY_IDENTITY",
        value: "Bethesda Marriott",
        url: "https://example.com/hotel",
        isSeedFact: true,
        verified: true,
      },
    ],
    cost: { total_usd: 0 },
  });
  assert.equal(scored.classifier.tier, OWNERSHIP_SUCCESS_TIER.UNRESOLVED);
});

test("three_hotel_manifest_and_command", () => {
  const c = loadP3BM1PilotCohort();
  assert.equal(c.ok, true);
  assert.equal(c.hotels.length, 3);
  assert.ok(c.hotels[0].name.includes("Bethesda"));
  assert.ok(c.hotels[1].name.includes("Wyndham"));
  assert.ok(c.hotels[2].name.includes("Ritz-Carlton"));
  const planned = buildP3BM1PlannedOperations();
  assert.equal(planned.authorized, false);
  assert.ok(planned.totals.expectedMaxProviderUsd <= P3B_M1_HARD_CAP_USD);
  const cmd = buildAuthorizedExecutionCommand();
  assert.equal(cmd.paidExecutionBlockedUntilAuth, true);
  assert.ok(cmd.preciseAuthorizedCommand.includes("--enable-live-network"));
  assert.ok(cmd.preciseAuthorizedCommand.includes(P3B_M1_FOUNDER_PAID_AUTH_ID));
});

await testAsync("full_live_activation_validation_offline", async () => {
  const report = await runP3BM1LiveActivationValidation({ memoryOnly: true });
  assert.equal(report.ok, true, JSON.stringify({
    phaseA: report.phaseASafety?.ok,
    liveBlocked: report.liveProviderWiring?.liveNetworkDefaultBlocked,
    wired: report.liveProviderWiring?.gatewayWithMockFetchLiveFlag,
    bypass: report.liveProviderWiring?.bypassForbidden,
  }, null, 2));
  assert.equal(report.paidExecutionAuthorized, false);
  assert.equal(report.proposedBudgetNotAuthorized, true);
  assert.equal(report.requestManifest.authorized_for_paid_spend, false);
  assert.equal(report.requestManifest.hotels.length, 3);
  assert.ok(report.requestManifest.worstCaseSpendUsd.expectedMaxProviderUsd > 0);
  assert.equal(report.comparableReport.recommendation.develop_further, null);
  assert.equal(report.staging.graphGate.ok, true);
});

console.log(`\nOK ${passed} he-intel-001-p3b-m1-live-activation tests passed`);
