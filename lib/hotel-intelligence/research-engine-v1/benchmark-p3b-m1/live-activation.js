/**
 * P3B-M1 Live Activation harness — resolves Phase A blockers, wires providers
 * behind gateway, builds three-hotel experiment manifest + scoring report.
 * Paid live network remains DISABLED unless founder auth + LIVE_NETWORK=1.
 */

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
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
  P3B_M1_FOUNDER_PAID_AUTH_ID,
} from "./contract.js";
import { loadP3BM1PilotCohort } from "./cohort.js";
import {
  buildM1GatewayAdapterMap,
  probeConfiguredProviderAvailability,
  createMockPaidTransports,
} from "./adapters.js";
import { createDurableM1BudgetLedger, OP_STATUS } from "./durable-budget-ledger.js";
import {
  buildP3BM1PlannedOperations,
  buildProposedExecutionCommand,
} from "./planned-operations.js";
import {
  buildLiveCapableTransports,
  probeAccountPricingPresence,
} from "./live-transports.js";
import {
  buildComparableReportSkeleton,
  scoreHotelMethodResult,
  recommendResearchApproach,
} from "./experiment-scoring.js";

export const LIVE_ACTIVATION_PHASE = "P3B-M1-LIVE-ACTIVATION";

function createActivationGateway(opts = {}) {
  const auth = resolveP3BM1FounderPaidAuthorization(opts.founderAuth || {});
  const ledger =
    opts.ledger ||
    createDurableM1BudgetLedger({
      runId: opts.runId || `p3b_m1_live_act_${Date.now()}`,
      memoryOnly: opts.memoryOnly !== false,
      persistPath: opts.persistPath,
    });
  const liveTransports = buildLiveCapableTransports({
    founderAuth: auth,
    liveNetworkEnabled: opts.liveNetworkEnabled === true,
    env: opts.env,
    fetchImpl: opts.fetchImpl,
  });
  const adapters =
    opts.adapters ||
    buildM1GatewayAdapterMap({
      useMockTransports: opts.useMockTransports === true,
      exaTransport:
        opts.useMockTransports === true
          ? createMockPaidTransports().exa
          : opts.exaTransport || liveTransports.exa,
      parallelSearchTransport:
        opts.useMockTransports === true
          ? createMockPaidTransports().parallelSearch
          : opts.parallelSearchTransport || liveTransports.parallelSearch,
      parallelTaskTransport:
        opts.useMockTransports === true
          ? createMockPaidTransports().parallelTask
          : opts.parallelTaskTransport || liveTransports.parallelTask,
      nativeTransport: opts.nativeTransport,
    });
  const gateway = createExecutionGateway({
    ledger,
    adapters,
    paidProvidersEnabled: auth.ok === true,
    allowBenchmarkPaidOverride: auth.ok === true,
    founderPaidAuthorization: auth,
    surfeEnabled: false,
  });
  return { gateway, ledger, auth, adapters, liveTransports };
}

/**
 * Phase A safety suite: durable resume, concurrent workers, pending reconcile,
 * op-scoped auth, fail-closed unknown cost, no duplicate billable spend.
 */
export async function runPhaseASafetySuite(opts = {}) {
  const tmpDir =
    opts.tmpDir || fs.mkdtempSync(path.join(os.tmpdir(), "he-intel-p3b-m1-budget-"));
  const persistPath = path.join(tmpDir, "ledger.json");
  const hotelId = "recLuxvwwxID7U2B8";

  // 1) Durable reservation survives "restart" (new ledger instance, same file)
  const ledger1 = createDurableM1BudgetLedger({
    runId: "phase_a_safety",
    persistPath,
    memoryOnly: false,
    runUsdCap: P3B_M1_HARD_CAP_USD,
    perHotelUsdCap: P3B_M1_PER_HOTEL_CAP_USD,
  });
  const opPersist = "op_durable_persist_demo";
  const r1 = ledger1.reserve({
    operation_id: opPersist,
    hotel_id: hotelId,
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    cost_usd: 0.5,
    cost_usd_status: "ESTIMATED",
    credits: 0,
  });
  const afterRestart = createDurableM1BudgetLedger({
    runId: "phase_a_safety",
    persistPath,
    memoryOnly: false,
  });
  const resumed = afterRestart.getOperation(opPersist);

  // 2) Concurrent workers cannot oversubscribe $5
  const concPath = path.join(tmpDir, "concurrent.json");
  const shared = createDurableM1BudgetLedger({
    persistPath: concPath,
    memoryOnly: false,
    runUsdCap: 1.0,
    perHotelUsdCap: 1.0,
  });
  const concurrent = await Promise.all(
    [0, 1, 2, 3, 4, 5].map((i) =>
      Promise.resolve(
        shared.reserve({
          operation_id: `op_conc_${i}`,
          hotel_id: hotelId,
          provider: PROVIDER_IDS.EXA,
          operation: "search_lite",
          cost_usd: 0.3,
          cost_usd_status: "ESTIMATED",
        })
      )
    )
  );
  const accepted = concurrent.filter((c) => c.ok && !c.already_reserved).length;
  const refusedBudget = concurrent.filter((c) => !c.ok).length;

  // 3) Pending reconcile blocks retry + no auto-release
  const pendingPath = path.join(tmpDir, "pending.json");
  const pendLedger = createDurableM1BudgetLedger({
    persistPath: pendingPath,
    memoryOnly: false,
  });
  const pendOp = "op_ambiguous_timeout";
  pendLedger.reserve({
    operation_id: pendOp,
    hotel_id: hotelId,
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    cost_usd: 0.025,
    cost_usd_status: "ESTIMATED",
  });
  const marked = pendLedger.markPendingReconcile(pendOp, {
    reason: "PROVIDER_TIMEOUT",
  });
  const releaseBlocked = pendLedger.release(pendOp, { reason: "auto" });
  const retryBlocked = pendLedger.reserve({
    operation_id: pendOp,
    hotel_id: hotelId,
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    cost_usd: 0.025,
    cost_usd_status: "ESTIMATED",
  });

  // 4) Gateway: timeout → pending; settle → resume no double spend; op-scoped auth
  const gwLedger = createDurableM1BudgetLedger({
    persistPath: path.join(tmpDir, "gw.json"),
    memoryOnly: false,
  });
  const timeoutGw = createExecutionGateway({
    ledger: gwLedger,
    adapters: buildM1GatewayAdapterMap({
      useMockTransports: false,
      parallelTaskTransport: async () => {
        const err = new Error("simulated_timeout");
        err.code = "PROVIDER_TIMEOUT";
        err.dispatched = true;
        err.ambiguous = true;
        throw err;
      },
      exaTransport: async (req) => ({
        ok: true,
        cost_usd: 0.004,
        cost_usd_status: "REPORTED",
        provider_cost_usd: 0.004,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        facts: [],
        operation_id: req.operation_id,
      }),
    }),
    paidProvidersEnabled: true,
    allowBenchmarkPaidOverride: true,
    founderPaidAuthorization: {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
    },
  });
  const timeoutResult = await timeoutGw.execute({
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    hotelId,
    caseId: "timeout_case",
    params: { processor: "core" },
  });
  const timeoutRetry = await timeoutGw.execute({
    provider: PROVIDER_IDS.PARALLEL_TASK,
    operation: "task_core",
    hotelId,
    caseId: "timeout_case",
    params: { processor: "core" },
  });

  const settleOp = buildOperationId({
    caseId: "settle_resume",
    hotelId,
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    params: { numResults: 8 },
    attempt: 0,
  });
  const first = await timeoutGw.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId,
    caseId: "settle_resume",
    operationId: settleOp,
    params: { numResults: 8 },
  });
  const resume = await timeoutGw.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId,
    caseId: "settle_resume",
    operationId: settleOp,
    params: { numResults: 8 },
  });

  let bypassThrown = false;
  try {
    await timeoutGw.attemptDirectAdapterCall(PROVIDER_IDS.EXA, {
      operation: "search_lite",
      // no authorized operation id
    });
  } catch (err) {
    bypassThrown = err?.code === "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
  }

  // 5) Fail-closed unknown cost
  const unkLedger = createDurableM1BudgetLedger({ memoryOnly: true });
  const unk = unkLedger.reserve({
    operation_id: "op_unknown_cost",
    hotel_id: hotelId,
    provider: PROVIDER_IDS.DEALALITY_NATIVE,
    operation: "loop",
    cost_usd: null,
    cost_usd_status: "UNKNOWN",
  });

  const ok =
    r1.ok === true &&
    resumed?.status === OP_STATUS.RESERVED &&
    Number(resumed?.reserved_usd) === 0.5 &&
    accepted <= 3 &&
    refusedBudget >= 1 &&
    marked.ok === true &&
    releaseBlocked.ok === false &&
    retryBlocked.ok === false &&
    timeoutResult.pending_reconcile === true &&
    timeoutRetry.error === "PENDING_RECONCILE_BLOCKS_RETRY" &&
    first.ok === true &&
    resume.replayed === true &&
    bypassThrown === true &&
    unk.ok === false;

  return {
    ok,
    tmpDir,
    durableRestart: {
      reserved: r1.ok,
      statusAfterRestart: resumed?.status,
      reservedUsd: resumed?.reserved_usd,
    },
    concurrent: {
      accepted,
      refusedBudget,
      runUsdCap: 1.0,
      results: concurrent.map((c) => ({ ok: c.ok, error: c.error || null })),
    },
    pendingReconcile: {
      marked: marked.ok,
      releaseBlocked: releaseBlocked.error,
      retryBlocked: retryBlocked.error,
      gatewayTimeout: timeoutResult.error,
      gatewayRetryBlocked: timeoutRetry.error,
    },
    noDuplicateSpend: {
      firstOk: first.ok,
      resumeReplayed: resume.replayed === true,
      sameOpId: settleOp,
    },
    opScopedAuth: { bypassThrown },
    failClosedUnknownCost: { ok: unk.ok, error: unk.error, note: unk.note },
  };
}

/**
 * Full live-activation validation (paid network OFF).
 */
export async function runP3BM1LiveActivationValidation(opts = {}) {
  const cohort = loadP3BM1PilotCohort(opts);
  if (!cohort.ok) return { ok: false, error: "COHORT_INVALID", cohort };

  const founderAuth = resolveP3BM1FounderPaidAuthorization(opts.founderAuth || {});
  const availability = probeConfiguredProviderAvailability(opts.env || process.env);
  const pricingPresence = probeAccountPricingPresence(opts.env || process.env);
  const planned = buildP3BM1PlannedOperations();
  const executionCommand = buildAuthorizedExecutionCommand();

  const phaseA = await runPhaseASafetySuite(opts.phaseA || {});

  // Wire live-capable transports but keep network disabled — verify block
  const liveWire = createActivationGateway({
    useMockTransports: false,
    liveNetworkEnabled: false,
    founderAuth: {},
    memoryOnly: true,
  });
  let liveBlocked = false;
  let liveBlockCode = null;
  try {
    await liveWire.liveTransports.exa({
      operation: "search_lite",
      hotelName: "Bethesda Marriott",
      params: { numResults: 8 },
    });
  } catch (err) {
    liveBlocked = err?.code === "LIVE_TRANSPORT_BLOCKED";
    liveBlockCode = err?.code || null;
  }

  // Provider adapter verification with injected fetch (no real spend)
  let fetchCalls = 0;
  const mockFetch = async () => {
    fetchCalls += 1;
    return {
      ok: true,
      status: 200,
      json: async () => ({
        results: [
          {
            url: "https://example.com/ownership-filing",
            title: "Ownership filing candidate",
            text: "Candidate excerpt — not verified ownership",
            highlights: [],
          },
        ],
        costDollars: { total: 0.004 },
      }),
    };
  };
  const wiredWithAuthStillBlocked = createActivationGateway({
    useMockTransports: false,
    liveNetworkEnabled: false,
    founderAuth: {
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
      confirmPaidExecution: true,
      authorizedBudgetUsd: 5,
    },
    fetchImpl: mockFetch,
    memoryOnly: true,
  });
  // Gateway refuses paid without... wait, auth is ok but live network off.
  // Adapter call via gateway: paid enabled, but transport blocks network.
  const gwCallBlocked = await wiredWithAuthStillBlocked.gateway.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId: cohort.hpcIds[0],
    caseId: "wire_block",
    params: { numResults: 8 },
  });

  // With liveNetwork + auth + mock fetch: verifies wiring end-to-end ($0 real).
  // Credential presence only — value never logged; fetchImpl intercepts network.
  const wireEnv = {
    ...(opts.env || process.env),
    EXA_API_KEY: opts.env?.EXA_API_KEY || "test_exa_key_not_for_live_spend",
  };
  const wiredLiveMock = createActivationGateway({
    useMockTransports: false,
    liveNetworkEnabled: true,
    founderAuth: {
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
      confirmPaidExecution: true,
      authorizedBudgetUsd: 5,
    },
    fetchImpl: mockFetch,
    env: wireEnv,
    memoryOnly: true,
  });
  const wiredOk = await wiredLiveMock.gateway.execute({
    provider: PROVIDER_IDS.EXA,
    operation: "search_lite",
    hotelId: cohort.hpcIds[0],
    hotelName: cohort.hotels[0].name,
    caseId: "wire_ok",
    params: { numResults: 8, query: "Bethesda Marriott owner" },
  });

  // Bypass forbidden
  let bypassOk = false;
  try {
    await wiredLiveMock.attemptDirectAdapterCall?.(PROVIDER_IDS.EXA, {});
  } catch (err) {
    bypassOk = err?.code === "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
  }
  if (!bypassOk) {
    try {
      await wiredLiveMock.gateway.attemptDirectAdapterCall(PROVIDER_IDS.EXA, {});
      bypassOk = false;
    } catch (err) {
      bypassOk = err?.code === "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
    }
  }

  const methods = [
    PROVIDER_IDS.EXA,
    PROVIDER_IDS.PARALLEL_SEARCH,
    PROVIDER_IDS.PARALLEL_TASK,
    PROVIDER_IDS.DEALALITY_NATIVE,
  ];
  const bags = createIsolatedDiscoveryBags(methods);
  const store = createBenchmarkStagingStore({
    runId: opts.runId || `p3b_m1_live_act_${Date.now()}`,
    memoryOnly: opts.memoryOnly !== false,
  });

  // Offline comparable report skeleton + sample classifier rows (no paid)
  const reportSkeleton = buildComparableReportSkeleton(cohort.hotels, methods);
  const sampleScored = [];
  for (const h of cohort.hotels) {
    for (const method of methods) {
      const scored = scoreHotelMethodResult({
        hotel: h,
        method,
        facts: [],
        cost: { total_usd: 0, provider_usd: 0 },
      });
      sampleScored.push(scored);
      bags.append(method, {
        claimType: "PROPERTY_IDENTITY",
        value: h.name,
        isSeedFact: true,
      });
    }
  }
  const recommendation = recommendResearchApproach(sampleScored);

  const requestManifest = {
    envelopeId: "P3B_M1_THREE_HOTEL_LIVE_ACTIVATION",
    authorized_for_paid_spend: false,
    hotels: cohort.hotels.map((h) => ({
      hpcId: h.hpcId,
      name: h.name,
      research_objectives: [
        "ultimate_owner_or_controlling_org",
        "legal_entity_and_ownership_chain",
        "ownership_decision_makers",
        "verified_direct_contact_channels",
        "credible_indirect_ownership_contact_routes",
        "missing_conflicting_unverifiable_ownership_info",
      ],
    })),
    maxRequestsByProvider: planned.hotelPlans[0]?.maxRequests || {},
    plannedOperations: planned,
    priceAssumptions: {
      exa_search_lite_usd: 0.004,
      exa_contents_usd: 0.001,
      parallel_search_fast_usd: 0.001,
      parallel_task_core_usd: 0.025,
      parallel_task_pro_usd: 0.1,
      source: "list_price_assumptions_only",
      account_balances: pricingPresence,
    },
    worstCaseSpendUsd: {
      expectedMaxProviderUsd: planned.totals.expectedMaxProviderUsd,
      withOptionalProUsd: planned.totals.expectedMaxWithOptionalProUsd,
      hardCapUsd: P3B_M1_HARD_CAP_USD,
      perHotelCapUsd: P3B_M1_PER_HOTEL_CAP_USD,
      authorized: false,
    },
  };

  const graphGate = store.assertNoProductionGraphWrite({
    productionAirtableWrite: false,
  });

  const report = {
    ok:
      phaseA.ok === true &&
      liveBlocked === true &&
      founderAuth.ok === false &&
      gwCallBlocked.ok === false &&
      wiredOk.ok === true &&
      fetchCalls >= 1 &&
      bypassOk === true &&
      cohort.ok &&
      graphGate.ok &&
      planned.authorized === false,
    phase: LIVE_ACTIVATION_PHASE,
    parentPhase: P3B_M1_PHASE,
    contract: P3B_M1_CONTRACT_ID,
    contract_version: P3B_M1_CONTRACT_VERSION,
    guarantees: writeGuaranteesP3BM1(),
    policy: P3B_M1_POLICY,
    founderAuthorizationGate: founderAuth,
    paidExecutionAuthorized: false,
    proposedBudgetNotAuthorized: true,
    phaseASafety: phaseA,
    liveProviderWiring: {
      transportsWired: true,
      liveNetworkDefaultBlocked: liveBlocked,
      liveBlockCode,
      gatewayWithoutLiveNetwork: {
        ok: gwCallBlocked.ok,
        error: gwCallBlocked.error,
      },
      gatewayWithMockFetchLiveFlag: {
        ok: wiredOk.ok,
        operation_id: wiredOk.operation_id,
        fetchCalls,
        facts: Array.isArray(wiredOk.facts) ? wiredOk.facts.length : 0,
      },
      bypassForbidden: bypassOk,
      noDirectAdapterBypass: true,
      availability,
      pricingPresence,
    },
    requestManifest,
    executionCommand,
    verificationMethodology: {
      objective: "HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1",
      version: "1.1.0",
      rules: [
        "Evidence-grounded FULL_ACTIONABLE only",
        "Legal titleholder alone = PARTIAL",
        "Valid URL alone ≠ ownership verification",
        "Do not score known hotel identity as new intelligence",
        "Provider arms isolated",
      ],
    },
    comparableReport: {
      ...reportSkeleton,
      rows: sampleScored,
      recommendation: {
        ...recommendation,
        develop_further: null,
        rationale:
          "Live paid results not yet authorized — recommendation deferred until founder-authorized execution produces scored rows with real costs.",
      },
    },
    staging: { ...store.policy, graphGate },
    forbidden: ["SURFE", "PDL", "canonical_graph_writes", "customer_publication"],
  };

  store.writeJson("live-activation-manifest.json", {
    runId: store.runId,
    phase: LIVE_ACTIVATION_PHASE,
    paid_authorized: false,
  });

  return report;
}

export function buildAuthorizedExecutionCommand() {
  return {
    dryRunLiveActivation:
      "npm run dry-run:he-intel-001-p3b-m1-live-activation",
    paidExecutionBlockedUntilAuth: true,
    preciseAuthorizedCommand:
      "npm run run:he-intel-001-p3b-m1-live -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5 --enable-live-network",
    note:
      "Proposed $5 maximum remains NOT AUTHORIZED. Command must refuse without founder auth AND without --enable-live-network. Do not run paid spend until separate founder approval.",
    envFlagsRequired: [
      "HE_INTEL_P3B_M1_LIVE_NETWORK=1 (or --enable-live-network)",
      "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1",
      "--confirm-paid-execution",
      "--authorized-budget-usd ≤ 5",
    ],
  };
}

export { createActivationGateway as createP3BM1LiveActivationGateway };
