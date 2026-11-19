#!/usr/bin/env node
/**
 * HE-INTEL-001 P3B-M1 Live Activation regressions — $0 paid, no live network.
 * Includes founder PR #439 corrections: fail-closed ledger, claim exclusivity,
 * sealed gateway auth, multi-process budget safety, discovery candidates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  P3B_M1_CONTRACT_VERSION,
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  createDurableM1BudgetLedger,
  OP_STATUS,
  LEDGER_FATAL,
  runPhaseASafetySuite,
  runP3BM1LiveActivationValidation,
  buildAuthorizedExecutionCommand,
  buildP3BM1PlannedOperations,
  buildLiveCapableTransports,
  scoreHotelMethodResult,
  factsToClassifierInput,
  loadP3BM1PilotCohort,
  toDiscoveryCandidate,
  independentlyAcceptOwnershipClaim,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";
import {
  createExecutionGateway,
} from "../lib/hotel-intelligence/research-engine-v1/execution-gateway.js";
import { PROVIDER_IDS } from "../lib/hotel-intelligence/research-engine-v1/provider-interface.js";
import {
  OWNERSHIP_SUCCESS_TIER,
} from "../lib/hotel-intelligence/research-engine-v1/ownership-research-objective.js";
import { createExaGatewayAdapter } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/adapters.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER = path.join(__dirname, "he-intel-p3b-m1-multiprocess-worker-v1.mjs");

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

function runWorker(args) {
  const r = spawnSync(process.execPath, [WORKER, ...args], {
    encoding: "utf8",
    timeout: 15000,
  });
  const line = String(r.stdout || "")
    .trim()
    .split("\n")
    .filter(Boolean)
    .pop();
  let parsed = null;
  try {
    parsed = line ? JSON.parse(line) : null;
  } catch {
    parsed = { ok: false, error: "PARSE_FAIL", raw: line };
  }
  return { ...r, parsed };
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
  assert.equal(suite.opScopedAuth.rawBypassThrown, true);
  assert.equal(suite.failClosedUnknownCost.ok, false);
  assert.equal(suite.failClosedNegative.ok, false);
  assert.equal(suite.claimExclusivity.claimA, true);
  assert.equal(suite.claimExclusivity.claimBError, "OPERATION_IN_FLIGHT");
  assert.equal(suite.failClosedCorruption.capMismatch, LEDGER_FATAL.CAP_MISMATCH);
  assert.equal(suite.noDuplicateSpend.resumeReplayed, true);
});

await testAsync("corrupt_ledger_not_empty_usable", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-corr-"));
  const persistPath = path.join(dir, "bad.json");
  fs.writeFileSync(persistPath, '{"meta":{"runUsdCap":5},"operations":');
  const ledger = createDurableM1BudgetLedger({
    persistPath,
    memoryOnly: false,
    runUsdCap: 5,
    perHotelUsdCap: 2.5,
  });
  assert.equal(ledger.isFatal(), true);
  const r = ledger.reserve({
    operation_id: "x",
    hotel_id: "recLuxvwwxID7U2B8",
    provider: "EXA",
    operation: "search_lite",
    cost_usd: 0.01,
    cost_usd_status: "ESTIMATED",
  });
  assert.equal(r.ok, false);
  assert.ok(
    r.error === LEDGER_FATAL.CORRUPT || r.fatal === true,
    JSON.stringify(r)
  );
});

await testAsync("integrity_mismatch_fail_closed", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-int-"));
  const persistPath = path.join(dir, "a.json");
  const a = createDurableM1BudgetLedger({
    persistPath,
    memoryOnly: false,
    runUsdCap: 5,
    perHotelUsdCap: 2.5,
  });
  a.reserve({
    operation_id: "op1",
    hotel_id: "recLuxvwwxID7U2B8",
    provider: "EXA",
    operation: "search_lite",
    cost_usd: 0.5,
    cost_usd_status: "ESTIMATED",
  });
  const raw = JSON.parse(fs.readFileSync(persistPath, "utf8"));
  raw.operations.op1.reserved_usd = 99;
  // leave stale integrity hash
  fs.writeFileSync(persistPath, JSON.stringify(raw, null, 2));
  const b = createDurableM1BudgetLedger({
    persistPath,
    memoryOnly: false,
    runUsdCap: 5,
    perHotelUsdCap: 2.5,
  });
  assert.equal(b.isFatal(), true);
  assert.equal(b.getFatal()?.error, LEDGER_FATAL.CORRUPT);
});

await testAsync("op_scoped_auth_forged_id_and_cancellation", async () => {
  const ledger = createDurableM1BudgetLedger({ memoryOnly: true });
  let otherOpSawAuth = false;
  let forgedOk = false;
  const gwRef = { current: null };
  gwRef.current = createExecutionGateway({
    ledger,
    adapters: {
      [PROVIDER_IDS.EXA]: {
        estimateCost: () => ({
          cost_usd: 0.01,
          cost_usd_status: "ESTIMATED",
          credits: 0,
          bounded: true,
        }),
        call: async (req) => {
          try {
            await gwRef.current.attemptDirectAdapterCall(PROVIDER_IDS.EXA, {
              operation_id: "foreign_op",
              operation: "search_lite",
            });
            otherOpSawAuth = true;
          } catch (err) {
            otherOpSawAuth = err?.code !== "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
          }
          try {
            await gwRef.current.attemptRawBypass(PROVIDER_IDS.EXA, {
              operation_id: req.operation_id,
            });
            forgedOk = true;
          } catch (err) {
            forgedOk = err?.code !== "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
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
  assert.equal(forgedOk, false);

  // After execute, auth must be cancelled — sealed call with prior op id fails
  let cancelled = false;
  try {
    await gwRef.current.attemptDirectAdapterCall(PROVIDER_IDS.EXA, {
      operation_id: r.operation_id,
      operation: "search_lite",
    });
  } catch (err) {
    cancelled = err?.code === "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
  }
  assert.equal(cancelled, true);
});

await testAsync("duplicate_execution_refused_via_claim", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-dup-"));
  const persistPath = path.join(dir, "dup.json");
  let calls = 0;
  const transport = async () => {
    calls += 1;
    await new Promise((r) => setTimeout(r, 30));
    return {
      ok: true,
      cost_usd: 0.004,
      cost_usd_status: "REPORTED",
      provider_cost_usd: 0.004,
      facts: [],
    };
  };
  const makeGw = (workerId) => {
    const ledger = createDurableM1BudgetLedger({
      persistPath,
      memoryOnly: false,
      workerId,
      runUsdCap: 5,
      perHotelUsdCap: 2.5,
    });
    return createExecutionGateway({
      ledger,
      workerId,
      adapters: {
        [PROVIDER_IDS.EXA]: {
          estimateCost: () => ({
            cost_usd: 0.01,
            cost_usd_status: "ESTIMATED",
            bounded: true,
            credits: 0,
          }),
          call: transport,
        },
      },
      paidProvidersEnabled: true,
      allowBenchmarkPaidOverride: true,
      founderPaidAuthorization: {
        ok: true,
        authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
      },
    });
  };
  const opId = "op_dup_dispatch";
  const [a, b] = await Promise.all([
    makeGw("wa").execute({
      provider: PROVIDER_IDS.EXA,
      operation: "search_lite",
      hotelId: "recLuxvwwxID7U2B8",
      operationId: opId,
      params: { numResults: 8 },
    }),
    makeGw("wb").execute({
      provider: PROVIDER_IDS.EXA,
      operation: "search_lite",
      hotelId: "recLuxvwwxID7U2B8",
      operationId: opId,
      params: { numResults: 8 },
    }),
  ]);
  const winners = [a, b].filter((x) => x.ok === true);
  const losers = [a, b].filter((x) => x.ok === false);
  assert.equal(winners.length, 1, JSON.stringify({ a, b, calls }));
  assert.equal(losers.length, 1);
  assert.ok(
    ["OPERATION_IN_FLIGHT", "PENDING_RECONCILE_BLOCKS_RETRY", "ALREADY_SETTLED"].includes(
      losers[0].error
    ) || losers[0].in_flight === true || losers[0].replayed === true,
    JSON.stringify(losers[0])
  );
  assert.equal(calls, 1);
});

await testAsync("multiprocess_same_op_id_exactly_one_dispatch", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-mp-"));
  const ledgerPath = path.join(dir, "mp.json");
  const opId = "op_mp_same";
  const argsBase = [
    "--mode",
    "dispatch",
    "--ledger",
    ledgerPath,
    "--op-id",
    opId,
    "--run-cap",
    "5",
    "--hotel-cap",
    "2.5",
    "--hold-ms",
    "120",
  ];
  const { spawn } = await import("node:child_process");
  const runAsync = (workerId) =>
    new Promise((resolve) => {
      const child = spawn(process.execPath, [WORKER, ...argsBase, "--worker-id", workerId], {
        encoding: "utf8",
      });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (d) => {
        stdout += d;
      });
      child.stderr.on("data", (d) => {
        stderr += d;
      });
      child.on("close", (code) => {
        const line = stdout.trim().split("\n").filter(Boolean).pop();
        let parsed = null;
        try {
          parsed = line ? JSON.parse(line) : null;
        } catch {
          parsed = { ok: false, error: "PARSE", raw: line, stderr };
        }
        resolve({ code, parsed, stdout, stderr });
      });
    });
  const [r1, r2] = await Promise.all([runAsync("mpA"), runAsync("mpB")]);
  const results = [r1.parsed, r2.parsed];
  const liveDispatches = results.filter((x) => Number(x?.dispatchCount || 0) >= 1);
  const nonDispatch = results.filter((x) => Number(x?.dispatchCount || 0) === 0);
  // Exactly one process may perform live provider dispatch
  assert.equal(
    liveDispatches.length,
    1,
    JSON.stringify({ r1: r1.parsed, r2: r2.parsed })
  );
  assert.equal(nonDispatch.length, 1);
  const other = nonDispatch[0];
  assert.ok(
    other.replayed === true ||
      other.error === "OPERATION_IN_FLIGHT" ||
      other.in_flight === true ||
      other.ok === false,
    JSON.stringify(other)
  );
});

await testAsync("multiprocess_budget_cap_different_ops", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-cap-"));
  const ledgerPath = path.join(dir, "cap.json");
  const { spawn } = await import("node:child_process");
  const runReserve = (opId, cost, workerId) =>
    new Promise((resolve) => {
      const child = spawn(
        process.execPath,
        [
          WORKER,
          "--mode",
          "reserve",
          "--ledger",
          ledgerPath,
          "--op-id",
          opId,
          "--cost",
          String(cost),
          "--run-cap",
          "5",
          "--hotel-cap",
          "2.5",
          "--worker-id",
          workerId,
        ],
        { encoding: "utf8" }
      );
      let stdout = "";
      child.stdout.on("data", (d) => {
        stdout += d;
      });
      child.on("close", () => {
        const line = stdout.trim().split("\n").filter(Boolean).pop();
        resolve(line ? JSON.parse(line) : { ok: false });
      });
    });
  // 3 x $2.0 against $5 run cap — at most 2 succeed
  const results = await Promise.all([
    runReserve("op_a", 2.0, "c1"),
    runReserve("op_b", 2.0, "c2"),
    runReserve("op_c", 2.0, "c3"),
  ]);
  const okCount = results.filter((r) => r.ok && !r.already_reserved).length;
  assert.ok(okCount <= 2, JSON.stringify(results));
  assert.ok(results.some((r) => !r.ok), JSON.stringify(results));
});

await testAsync("multiprocess_per_hotel_cap", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-m1-ph-"));
  const ledgerPath = path.join(dir, "ph.json");
  const a = runWorker([
    "--mode",
    "reserve",
    "--ledger",
    ledgerPath,
    "--op-id",
    "op_h1",
    "--cost",
    "2.0",
    "--run-cap",
    "5",
    "--hotel-cap",
    "2.5",
    "--worker-id",
    "h1",
  ]);
  const b = runWorker([
    "--mode",
    "reserve",
    "--ledger",
    ledgerPath,
    "--op-id",
    "op_h2",
    "--cost",
    "1.0",
    "--run-cap",
    "5",
    "--hotel-cap",
    "2.5",
    "--worker-id",
    "h2",
  ]);
  assert.equal(a.parsed?.ok, true);
  // 2.0 + 1.0 = 3.0 > 2.5 per-hotel → second refuses
  assert.equal(b.parsed?.ok, false, JSON.stringify(b.parsed));
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

test("discovery_candidate_not_verified_ownership", () => {
  const c = toDiscoveryCandidate({
    url: "https://example.com/filing",
    title: "Acme REIT",
  });
  assert.equal(c.verified, false);
  assert.equal(c.kind, "DISCOVERY_CANDIDATE");
  const acc = independentlyAcceptOwnershipClaim(c, {});
  assert.equal(acc.ok, false);
  const scored = scoreHotelMethodResult({
    hotel: { hpcId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
    method: "EXA",
    facts: [c],
    cost: { total_usd: 0.1 },
  });
  assert.equal(scored.questions.ultimate_owner_identified, false);
  assert.equal(scored.questions.url_alone_not_verification, true);
  assert.equal(scored.classifier.tier, OWNERSHIP_SUCCESS_TIER.UNRESOLVED);
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
  assert.equal(mapped.classifierInput.ultimateOwner.verified, false);
});

test("exa_estimate_refuses_inline_contents", () => {
  const adapter = createExaGatewayAdapter({});
  const est = adapter.estimateCost({
    operation: "search_lite",
    params: { numResults: 8, includeContents: true },
  });
  assert.equal(est.bounded, false);
  assert.equal(est.cost_usd_status, "UNKNOWN");
});

test("expected_cost_not_max_guarantee", () => {
  const planned = buildP3BM1PlannedOperations();
  assert.equal(planned.totals.expectedCostIsNotMaxGuarantee, true);
  assert.equal(planned.totals.bindingCaps.runUsd, 5);
  assert.equal(planned.authorized, false);
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
  const cmd = buildAuthorizedExecutionCommand();
  assert.equal(cmd.paidExecutionBlockedUntilAuth, true);
  assert.ok(cmd.preciseAuthorizedCommand.includes("--enable-live-network"));
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
  assert.equal(report.requestManifest.authorized_for_paid_spend, false);
  assert.equal(
    report.requestManifest.plannedOperations.totals.expectedCostIsNotMaxGuarantee,
    true
  );
});

console.log(`\nOK ${passed} he-intel-001-p3b-m1-live-activation tests passed`);
