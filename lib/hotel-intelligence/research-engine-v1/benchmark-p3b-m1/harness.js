/**
 * P3B-M1 readiness harness — mock/offline validation of live research path.
 * Refuses real paid spend without founder authorization.
 */

import { createExecutionGateway, buildOperationId } from "../execution-gateway.js";
import { PROVIDER_IDS } from "../provider-interface.js";
import { createIsolatedDiscoveryBags } from "../benchmark-p3a/harness.js";
import { createBenchmarkStagingStore } from "../benchmark-p3a/staging-store.js";
import {
  P3B_M1_CONTRACT_ID,
  P3B_M1_CONTRACT_VERSION,
  P3B_M1_PHASE,
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
  P3B_M1_POLICY,
  writeGuaranteesP3BM1,
  resolveP3BM1FounderPaidAuthorization,
} from "./contract.js";
import { loadP3BM1PilotCohort } from "./cohort.js";
import {
  buildM1GatewayAdapterMap,
  probeConfiguredProviderAvailability,
} from "./adapters.js";
import { createM1SharedBudgetLedger } from "./shared-budget-ledger.js";
import {
  buildP3BM1PlannedOperations,
  buildProposedExecutionCommand,
} from "./planned-operations.js";
import { normalizeAndFilterNewIntelligence, PRIORITY_OBJECTIVES } from "./evidence.js";

function createGateway(opts) {
  const auth = resolveP3BM1FounderPaidAuthorization(opts.founderAuth || {});
  const ledger = opts.ledger || createM1SharedBudgetLedger();
  const adapters =
    opts.adapters ||
    buildM1GatewayAdapterMap({
      useMockTransports: opts.useMockTransports !== false,
    });
  const gateway = createExecutionGateway({
    ledger,
    adapters,
    paidProvidersEnabled: auth.ok === true,
    allowBenchmarkPaidOverride: auth.ok === true,
    founderPaidAuthorization: auth,
    surfeEnabled: false,
  });
  return { gateway, ledger, auth, adapters };
}

/**
 * Offline readiness run: budget concurrency, retries, resume, isolation, scoring.
 */
export async function runP3BM1ReadinessValidation(opts = {}) {
  const cohort = loadP3BM1PilotCohort(opts);
  if (!cohort.ok) return { ok: false, error: "COHORT_INVALID", cohort };

  const founderAuth = resolveP3BM1FounderPaidAuthorization(opts.founderAuth || {});
  const availability = probeConfiguredProviderAvailability(opts.env || process.env);
  const planned = buildP3BM1PlannedOperations();
  const executionCommand = buildProposedExecutionCommand();

  // Always validate economic path with mocks — never hit live paid APIs here.
  const ledger = createM1SharedBudgetLedger({
    runUsdCap: P3B_M1_HARD_CAP_USD,
    perHotelUsdCap: P3B_M1_PER_HOTEL_CAP_USD,
  });
  const { gateway, auth } = createGateway({
    ledger,
    useMockTransports: true,
    founderAuth: opts.simulateFounderAuth
      ? {
          authId: "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1",
          confirmPaidExecution: true,
          authorizedBudgetUsd: 5,
        }
      : {},
  });

  const store = createBenchmarkStagingStore({
    runId: opts.runId || `p3b_m1_ready_${Date.now()}`,
    memoryOnly: opts.memoryOnly !== false,
  });

  const arms = [
    PROVIDER_IDS.EXA,
    PROVIDER_IDS.PARALLEL_SEARCH,
    PROVIDER_IDS.PARALLEL_TASK,
    PROVIDER_IDS.DEALALITY_NATIVE,
  ];
  const bags = createIsolatedDiscoveryBags(arms);
  const results = [];
  const leakageViolations = [];

  // 1) Without founder auth, paid providers must refuse
  const refuseWithoutAuth = createGateway({
    useMockTransports: true,
    founderAuth: {},
  });
  const refused = await refuseWithoutAuth.gateway.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId: cohort.hpcIds[0],
    params: { numResults: 8 },
  });

  // 2) With simulated founder auth (mock transports only), exercise reserve/settle path
  if (!opts.simulateFounderAuth) {
    // still run mock economic tests under simulated auth for readiness evidence
  }
  const liveSim = createGateway({
    ledger,
    useMockTransports: true,
    founderAuth: {
      authId: "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1",
      confirmPaidExecution: true,
      authorizedBudgetUsd: 5,
    },
  });

  // Concurrent reservations against shared budget
  const hotel = cohort.hotels[0];
  const concurrent = await Promise.all(
    [0, 1, 2, 3, 4].map((i) =>
      liveSim.gateway.execute({
        provider: PROVIDER_IDS.EXA,
        operation: "search_lite",
        hotelId: hotel.hpcId,
        caseId: `conc_${i}`,
        params: { numResults: 8, concurrentSlot: i },
        attempt: 0,
      })
    )
  );

  // Failed request + release path
  const failed = await liveSim.gateway.execute({
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    hotelId: hotel.hpcId,
    caseId: "fail_probe",
    simulateFailure: true,
    params: { processor: "core", fail: true },
  });
  // Force failure via transport: rebuild with failing transport for one call
  const failLedger = createM1SharedBudgetLedger();
  const failGw = createExecutionGateway({
    ledger: failLedger,
    adapters: buildM1GatewayAdapterMap({
      useMockTransports: false,
      parallelTaskTransport: async () => {
        const err = new Error("SIMULATED_PROVIDER_FAILURE");
        err.code = "SIMULATED_PROVIDER_FAILURE";
        throw err;
      },
      exaTransport: async () => ({
        ok: true,
        cost_usd: 0.004,
        cost_usd_status: "REPORTED",
        provider_cost_usd: 0.004,
        facts: [],
      }),
      parallelSearchTransport: async () => ({
        ok: true,
        cost_usd: 0.001,
        cost_usd_status: "REPORTED",
        provider_cost_usd: 0.001,
        facts: [],
      }),
    }),
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1",
    },
  });
  const thrownFail = await failGw.execute({
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    hotelId: hotel.hpcId,
    caseId: "throw_fail",
    params: { processor: "core" },
  });

  // Resume / durable operation id — second call must not double-spend
  const opId = buildOperationId({
    caseId: "resume_demo",
    hotelId: hotel.hpcId,
    provider: PROVIDER_IDS.PARALLEL_SEARCH,
    operation: "search_fast",
    params: { searches: 1 },
    attempt: 0,
  });
  const first = await liveSim.gateway.execute({
    provider: PROVIDER_IDS.PARALLEL_SEARCH,
    operation: "search_fast",
    hotelId: hotel.hpcId,
    caseId: "resume_demo",
    operationId: opId,
    params: { searches: 1 },
  });
  const resume = await liveSim.gateway.execute({
    provider: PROVIDER_IDS.PARALLEL_SEARCH,
    operation: "search_fast",
    hotelId: hotel.hpcId,
    caseId: "resume_demo",
    operationId: opId,
    params: { searches: 1 },
  });

  // Per-hotel isolation + new-intel scoring across arms
  for (const h of cohort.hotels) {
    const seed = cohort.buildSeedA(h);
    for (const provider of arms) {
      if (provider === PROVIDER_IDS.DEALALITY_NATIVE) {
        const r = await liveSim.gateway.execute({
          provider,
          operation: "offline_loop",
          hotelId: h.hpcId,
          caseId: `native_${h.hpcId}`,
        });
        results.push({ provider, hpcId: h.hpcId, status: r.ok ? "OK" : r.error, r });
        continue;
      }
      const r = await liveSim.gateway.execute({
        provider,
        operation:
          provider === PROVIDER_IDS.EXA
            ? "search_lite"
            : provider === PROVIDER_IDS.PARALLEL_SEARCH
              ? "search_fast"
              : "task_core",
        hotelId: h.hpcId,
        caseId: `${provider}_${h.hpcId}`,
        params:
          provider === PROVIDER_IDS.EXA
            ? { numResults: 8 }
            : provider === PROVIDER_IDS.PARALLEL_SEARCH
              ? { searches: 1 }
              : { processor: "core" },
      });
      const factList = Array.isArray(r.facts) ? r.facts : [];
      for (const f of factList) bags.append(provider, f);
      const scored = normalizeAndFilterNewIntelligence(factList, {
        seedPropertyNames: [seed.propertyName],
      });
      results.push({
        provider,
        hpcId: h.hpcId,
        status: r.ok ? "OK" : r.error,
        scored,
        cost_usd: r.cost_usd,
        operation_id: r.operation_id,
      });
    }
    for (const a of arms) {
      for (const b of arms) {
        if (a === b) continue;
        const leak = bags.readOther(a, b);
        if (leak?.ok !== false) leakageViolations.push({ a, b, hpcId: h.hpcId });
      }
    }
  }

  // Shared budget exhaustion probe
  const exhaustLedger = createM1SharedBudgetLedger({ runUsdCap: 0.01, perHotelUsdCap: 0.01 });
  const exhaustGw = createExecutionGateway({
    ledger: exhaustLedger,
    adapters: buildM1GatewayAdapterMap({ useMockTransports: true }),
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1",
    },
  });
  const exhausted = await exhaustGw.execute({
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    hotelId: hotel.hpcId,
    caseId: "exhaust",
    params: { processor: "core" },
  });

  const snap = ledger.snapshot();
  const graphGate = store.assertNoProductionGraphWrite({
    productionAirtableWrite: false,
  });

  const report = {
    ok:
      refused.ok === false &&
      refused.error === "PROVIDER_UNAUTHORIZED" &&
      resume.replayed === true &&
      first.ok === true &&
      thrownFail.ok === false &&
      exhausted.ok === false &&
      leakageViolations.length === 0 &&
      cohort.ok &&
      graphGate.ok &&
      founderAuth.ok === false &&
      snap.canParallelBypassSharedBudget === false,
    phase: P3B_M1_PHASE,
    contract: P3B_M1_CONTRACT_ID,
    contract_version: P3B_M1_CONTRACT_VERSION,
    guarantees: writeGuaranteesP3BM1(),
    policy: P3B_M1_POLICY,
    founderAuthorizationGate: founderAuth,
    simulatedAuthUsedForMockPathOnly: true,
    availability,
    pricingNote:
      "List-price estimates only; account balances UNKNOWN; credentials never logged",
    plannedOperations: planned,
    proposedExecutionCommand: executionCommand,
    priorityObjectives: PRIORITY_OBJECTIVES,
    cohort: {
      hotels: cohort.hotels.map((h) => ({ hpcId: h.hpcId, name: h.name })),
      subsequentSixHotelCohort: cohort.subsequentSixHotelCohort,
    },
    atomicBudget: {
      hardCapUsd: P3B_M1_HARD_CAP_USD,
      perHotelCapUsd: P3B_M1_PER_HOTEL_CAP_USD,
      snapshot: snap,
      concurrentResults: concurrent.map((c) => ({
        ok: c.ok,
        error: c.error || null,
        operation_id: c.operation_id,
      })),
      refusedWithoutAuth: { ok: refused.ok, error: refused.error },
      failedThrown: { ok: thrownFail.ok, error: thrownFail.error },
      resume: { firstOk: first.ok, replayed: resume.replayed === true, sameOpId: opId },
      sharedCapExhausted: { ok: exhausted.ok, error: exhausted.error, scope: exhausted.scope },
    },
    leakageViolations,
    armResults: results,
    staging: { ...store.policy, graphGate },
    paidExecutionAuthorized: false,
  };

  store.writeJson("readiness-manifest.json", {
    runId: store.runId,
    phase: P3B_M1_PHASE,
    paid_authorized: false,
  });

  return report;
}

export { createGateway as createP3BM1Gateway };
