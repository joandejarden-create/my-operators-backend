/**
 * Gateway-compatible adapters wrapping existing Exa / Parallel / native surfaces.
 * Live HTTP is never invoked unless founder paid auth unlocks the gateway.
 * Default path: mock transports for readiness validation.
 */

import { quoteBenchmarkCall } from "../../../group-demand-intelligence/research/benchmark-provider-pricing-v1.js";
import { isParallelConfigured } from "../../research/providers/parallel/resolve-parallel-key.js";
import { PROVIDER_IDS } from "../provider-interface.js";

function envPresent(keys) {
  for (const k of keys) {
    if (process.env[k] && String(process.env[k]).trim()) return true;
  }
  return false;
}

/** Presence-only availability — never returns secret values. */
export function probeConfiguredProviderAvailability(env = process.env) {
  return {
    EXA: {
      keyConfigured: Boolean(env.EXA_API_KEY && String(env.EXA_API_KEY).trim()),
      adapterReady: true,
      liveFlag: String(env.GDI_EXA_AGENT_LIVE || "0"),
      boundedOps: ["search_lite", "contents"],
    },
    PARALLEL: {
      keyConfigured: isParallelConfigured(env),
      adapterReady: true,
      liveFlag: String(env.HOTEL_INTELLIGENCE_PARALLEL_ENABLED || "0"),
      boundedOps: ["search_fast", "task_core"],
    },
    SERPAPI: {
      keyConfigured: Boolean(env.SERPAPI_KEY && String(env.SERPAPI_KEY).trim()),
      adapterReady: true,
      liveFlag: String(env.HOTEL_INTELLIGENCE_SERPAPI || "0"),
      note: "USD/search UNKNOWN until plan confirmed — count-capped in M1 plan",
    },
    DEALALITY_NATIVE: {
      keyConfigured: envPresent(["OPENAI_API_KEY", "CONTEXT_DEV_API_KEY"]),
      adapterReady: true,
      liveFlag: "offline_loop_ready",
      note: "USD/credit UNKNOWN — credit-capped",
    },
    SURFE: { keyConfigured: Boolean(env.SURFE_API_KEY), forbidden: true },
    PDL: { keyConfigured: Boolean(env.PDL_API_KEY), forbidden: true },
  };
}

export function createExaGatewayAdapter(opts = {}) {
  const transport = opts.transport || null; // inject mock
  return {
    id: PROVIDER_IDS.EXA,
    estimateCost(req = {}) {
      const op = req.operation || "search";
      if (op === "search" || op === "search_lite") {
        const q = quoteBenchmarkCall({
          provider: "exa",
          operation: "search",
          params: { type: "lite", numResults: Math.min(10, req.params?.numResults || 8) },
        });
        return {
          cost_usd: q.bounded ? q.maxUsd : null,
          cost_usd_status: q.bounded ? "ESTIMATED" : "UNKNOWN",
          credits: 0,
          provider_cost_usd: q.bounded ? q.maxUsd : null,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          bounded: q.bounded,
        };
      }
      if (op === "contents") {
        const urls = req.params?.urls || [];
        const q = quoteBenchmarkCall({
          provider: "exa",
          operation: "contents",
          params: { urls: urls.slice(0, 5), text: true },
        });
        return {
          cost_usd: q.bounded ? q.maxUsd : null,
          cost_usd_status: q.bounded ? "ESTIMATED" : "UNKNOWN",
          credits: 0,
          provider_cost_usd: q.bounded ? q.maxUsd : null,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          bounded: q.bounded,
        };
      }
      return { cost_usd: null, cost_usd_status: "UNKNOWN", credits: 0, bounded: false };
    },
    async call(req = {}) {
      if (typeof transport === "function") return transport(req);
      const err = new Error("EXA_LIVE_TRANSPORT_REQUIRES_FOUNDER_AUTH");
      err.code = "LIVE_TRANSPORT_BLOCKED";
      throw err;
    },
  };
}

export function createParallelSearchGatewayAdapter(opts = {}) {
  const transport = opts.transport || null;
  const unitUsd = 0.001; // fast/turbo public list ~$1/1k
  return {
    id: PROVIDER_IDS.PARALLEL_SEARCH,
    estimateCost(req = {}) {
      const n = Math.min(6, Number(req.params?.searches || 1) || 1);
      const usd = Number((n * unitUsd).toFixed(6));
      return {
        cost_usd: usd,
        cost_usd_status: "ESTIMATED",
        credits: 0,
        provider_cost_usd: usd,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        bounded: true,
      };
    },
    async call(req = {}) {
      if (typeof transport === "function") return transport(req);
      const err = new Error("PARALLEL_SEARCH_LIVE_TRANSPORT_REQUIRES_FOUNDER_AUTH");
      err.code = "LIVE_TRANSPORT_BLOCKED";
      throw err;
    },
  };
}

export function createParallelTaskGatewayAdapter(opts = {}) {
  const transport = opts.transport || null;
  const coreUsd = 0.025;
  return {
    id: PROVIDER_IDS.PARALLEL_TASK,
    estimateCost(req = {}) {
      const processor = String(req.params?.processor || "core");
      const usd = processor === "pro" ? 0.1 : coreUsd;
      return {
        cost_usd: usd,
        cost_usd_status: "ESTIMATED",
        credits: 0,
        provider_cost_usd: usd,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        bounded: true,
        processor,
      };
    },
    async call(req = {}) {
      if (typeof transport === "function") return transport(req);
      const err = new Error("PARALLEL_TASK_LIVE_TRANSPORT_REQUIRES_FOUNDER_AUTH");
      err.code = "LIVE_TRANSPORT_BLOCKED";
      throw err;
    },
  };
}

export function createNativeGatewayAdapter(opts = {}) {
  const transport = opts.transport || null;
  const offlineZeroUsd = opts.offlineZeroUsd !== false;
  return {
    id: PROVIDER_IDS.DEALALITY_NATIVE,
    estimateCost() {
      if (offlineZeroUsd) {
        return {
          cost_usd: 0,
          cost_usd_status: "ZERO_AUTHORIZED",
          credits: Number(opts.creditsPerCall || 2),
          provider_cost_usd: 0,
          model_token_cost_usd: 0,
          orchestration_cost_usd: 0,
          bounded: true,
          note: "OFFLINE_READINESS_ZERO_USD",
        };
      }
      // Live credit-bounded; USD unknown until dashboard rate confirmed
      return {
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        credits: Number(opts.creditsPerCall || 2),
        provider_cost_usd: null,
        model_token_cost_usd: null,
        orchestration_cost_usd: 0,
        bounded: false,
        note: "CREDIT_BOUNDED_USD_NOT_VERIFIED",
      };
    },
    async call(req = {}) {
      if (typeof transport === "function") return transport(req);
      // Offline native mock path for readiness (zero USD)
      return {
        ok: true,
        provider: PROVIDER_IDS.DEALALITY_NATIVE,
        operation: req.operation,
        facts: [],
        cost_usd: 0,
        cost_usd_status: "ZERO_AUTHORIZED",
        provider_cost_usd: 0,
        model_token_cost_usd: 0,
        orchestration_cost_usd: 0,
        credits: 0,
        source_attribution: ["native://offline-readiness"],
      };
    },
  };
}

/**
 * Deterministic mock transports for offline M1 readiness (simulate paid path economics).
 */
export function createMockPaidTransports() {
  return {
    exa: async (req) => ({
      ok: true,
      provider: PROVIDER_IDS.EXA,
      operation: req.operation,
      facts: [
        {
          claimType: "OWNER_ENTITY",
          value: "MOCK_OWNER_NOT_VERIFIED_RESEARCH",
          url: "https://example.invalid/mock-owner",
          isSeedFact: false,
        },
      ],
      cost_usd: 0.004,
      cost_usd_status: "REPORTED",
      provider_cost_usd: 0.004,
      model_token_cost_usd: 0,
      orchestration_cost_usd: 0.0001,
      credits: 0,
      source_attribution: ["https://example.invalid/mock-owner"],
    }),
    parallelSearch: async (req) => ({
      ok: true,
      provider: PROVIDER_IDS.PARALLEL_SEARCH,
      operation: req.operation,
      facts: [],
      cost_usd: 0.001,
      cost_usd_status: "REPORTED",
      provider_cost_usd: 0.001,
      model_token_cost_usd: 0,
      orchestration_cost_usd: 0,
      credits: 0,
      source_attribution: ["https://example.invalid/mock-search"],
    }),
    parallelTask: async (req) => ({
      ok: req.simulateFailure !== true,
      provider: PROVIDER_IDS.PARALLEL_TASK,
      operation: req.operation,
      facts:
        req.simulateFailure === true
          ? []
          : [
              {
                claimType: "OPERATOR_ENTITY",
                value: "MOCK_OPERATOR",
                url: "https://example.invalid/mock-operator",
                isSeedFact: false,
              },
            ],
      cost_usd: req.simulateFailure === true ? 0 : 0.025,
      cost_usd_status: "REPORTED",
      provider_cost_usd: req.simulateFailure === true ? 0 : 0.025,
      model_token_cost_usd: 0,
      orchestration_cost_usd: 0,
      credits: 0,
      failed: req.simulateFailure === true,
      source_attribution: ["https://example.invalid/mock-operator"],
    }),
  };
}

export function buildM1GatewayAdapterMap(opts = {}) {
  const mocks = opts.useMockTransports ? createMockPaidTransports() : {};
  return {
    [PROVIDER_IDS.EXA]: createExaGatewayAdapter({
      transport: opts.useMockTransports ? mocks.exa : opts.exaTransport,
    }),
    [PROVIDER_IDS.PARALLEL_SEARCH]: createParallelSearchGatewayAdapter({
      transport: opts.useMockTransports ? mocks.parallelSearch : opts.parallelSearchTransport,
    }),
    [PROVIDER_IDS.PARALLEL_TASK]: createParallelTaskGatewayAdapter({
      transport: opts.useMockTransports ? mocks.parallelTask : opts.parallelTaskTransport,
    }),
    [PROVIDER_IDS.DEALALITY_NATIVE]: createNativeGatewayAdapter({
      transport: opts.nativeTransport,
    }),
    [PROVIDER_IDS.MOCK]: {
      estimateCost: () => ({
        cost_usd: 0,
        cost_usd_status: "ZERO_AUTHORIZED",
        credits: 0,
      }),
      call: async (req) => ({
        ok: true,
        provider: PROVIDER_IDS.MOCK,
        operation: req.operation,
        facts: [],
        cost_usd: 0,
        cost_usd_status: "ZERO_AUTHORIZED",
        credits: 0,
      }),
    },
  };
}
