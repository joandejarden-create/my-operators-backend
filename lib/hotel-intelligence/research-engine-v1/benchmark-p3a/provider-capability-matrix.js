/**
 * Provider capability + public pricing comparison for P3A.
 * Remaining account balances / plan-effective rates = UNKNOWN.
 * Never expose or log API credentials.
 */

import { quoteBenchmarkCall, BENCHMARK_PRICE_AS_OF as GDI_PRICE_AS_OF } from "../../../group-demand-intelligence/research/benchmark-provider-pricing-v1.js";
import { BENCHMARK_ARM, COST_STATUS } from "./contract.js";

export const PRICING_AS_OF = "2026-11-19";

function envConfigured(keys) {
  // Presence-only: never return key values.
  for (const k of keys) {
    if (process.env[k] && String(process.env[k]).trim()) return true;
  }
  return false;
}

function row(partial) {
  return {
    arm: partial.arm,
    provider: partial.provider,
    keyConfigured: partial.keyConfigured,
    firstPartyAdapter: partial.firstPartyAdapter,
    adapterReady: Boolean(partial.adapterReady),
    listPriceUnit: partial.listPriceUnit,
    listPriceUsdStatus: partial.listPriceUsdStatus || COST_STATUS.ESTIMATED,
    freeAllowancePublic: partial.freeAllowancePublic || null,
    remainingBalanceStatus: COST_STATUS.UNKNOWN,
    paidCallsInP3A: false,
    notes: partial.notes || "",
    sources: partial.sources || [],
    feasibility: partial.feasibility || "EVALUATE",
  };
}

/**
 * Static capability matrix (no network). Quotes Exa via existing GDI helper when useful.
 */
export function buildProviderCapabilityMatrix(opts = {}) {
  const checkEnv = opts.checkEnv !== false;
  const exaLiteQuote = quoteBenchmarkCall({
    provider: "exa",
    operation: "search",
    params: { type: "lite", numResults: 10 },
  });

  const matrix = [
    row({
      arm: BENCHMARK_ARM.EXA,
      provider: "EXA",
      keyConfigured: checkEnv ? envConfigured(["EXA_API_KEY"]) : null,
      firstPartyAdapter: "lib/group-demand-intelligence/research/exa-agent/* + benchmark-provider-pricing-v1",
      adapterReady: true,
      listPriceUnit: "lite search $4/1k ($0.004); contents $1/1k pages; agent $0.012–$1.00/run",
      listPriceUsdStatus: exaLiteQuote.bounded ? COST_STATUS.ESTIMATED : COST_STATUS.UNKNOWN,
      freeAllowancePublic: "$10/mo credits claimed by Exa (remaining UNKNOWN)",
      notes: "Prefer bounded lite + contents. Agent only if separately budgeted in P3B.",
      sources: ["https://exa.ai/docs/admin/pricing"],
      feasibility: "READY_FOR_P3B_AFTER_AUTH",
    }),
    row({
      arm: BENCHMARK_ARM.PARALLEL_TASK,
      provider: "PARALLEL_TASK",
      keyConfigured: checkEnv ? envConfigured(["PARALLEL_API_KEY"]) : null,
      firstPartyAdapter: "lib/hotel-intelligence/research/providers/parallel/*",
      adapterReady: true,
      listPriceUnit: "Task lite $5/1k … pro $100/1k ($0.10/run) … ultra8x higher; failed runs not billed",
      freeAllowancePublic: "$5/mo credits claimed (remaining UNKNOWN)",
      notes: "Existing HI ownership comparison used pro @ $0.10/run; prefer core before pro in P3B.",
      sources: ["https://docs.parallel.ai/getting-started/pricing"],
      feasibility: "READY_FOR_P3B_AFTER_AUTH",
    }),
    row({
      arm: BENCHMARK_ARM.PARALLEL_SEARCH,
      provider: "PARALLEL_SEARCH",
      keyConfigured: checkEnv ? envConfigured(["PARALLEL_API_KEY"]) : null,
      firstPartyAdapter: "lib/hotel-intelligence/research/providers/parallel/*",
      adapterReady: true,
      listPriceUnit: "Search turbo/fast $1/1k; basic/advanced $5/1k",
      freeAllowancePublic: "same Parallel credits (remaining UNKNOWN)",
      notes: "Low-cost discovery rung for Mode A.",
      sources: ["https://docs.parallel.ai/getting-started/pricing"],
      feasibility: "READY_FOR_P3B_AFTER_AUTH",
    }),
    row({
      arm: BENCHMARK_ARM.SERPAPI,
      provider: "SERPAPI",
      keyConfigured: checkEnv ? envConfigured(["SERPAPI_KEY"]) : null,
      firstPartyAdapter: "lib/hotel-intelligence/providers/serpapi.js",
      adapterReady: true,
      listPriceUnit: "Plan-based; Starter ~$25/1k; Production ~$10/1k effective",
      listPriceUsdStatus: COST_STATUS.UNKNOWN,
      freeAllowancePublic: "250 searches/mo free plan (remaining UNKNOWN)",
      notes: "USD/search depends on active plan → UNKNOWN until plan confirmed.",
      sources: ["https://serpapi.com/pricing"],
      feasibility: "READY_FOR_P3B_AFTER_PLAN_CONFIRM",
    }),
    row({
      arm: BENCHMARK_ARM.BRAVE,
      provider: "BRAVE",
      keyConfigured: checkEnv ? envConfigured(["BRAVE_API_KEY", "BRAVE_SEARCH_API_KEY"]) : null,
      firstPartyAdapter: "none",
      adapterReady: false,
      listPriceUnit: "~$5/1k requests",
      freeAllowancePublic: "$5 credits/mo claimed; N/A without key",
      notes: "Out of paid mini-pilot unless founder adds key + adapter.",
      sources: ["https://brave.com/search/api/"],
      feasibility: "NOT_READY",
    }),
    row({
      arm: BENCHMARK_ARM.DEALALITY_NATIVE,
      provider: "DEALALITY_NATIVE",
      keyConfigured: checkEnv
        ? envConfigured(["OPENAI_API_KEY", "CONTEXT_DEV_API_KEY"])
        : null,
      firstPartyAdapter: "lib/hotel-intelligence/contact-intelligence/* + research-engine-v1",
      adapterReady: true,
      listPriceUnit: "Credits known in prior pilots; USD/credit UNKNOWN (CREDIT_BOUNDED_USD_NOT_VERIFIED)",
      listPriceUsdStatus: COST_STATUS.UNKNOWN,
      freeAllowancePublic: null,
      notes: "Must log credits; convert to USD only when dashboard rate confirmed.",
      sources: [],
      feasibility: "READY_FOR_P3B_AFTER_AUTH",
    }),
    row({
      arm: BENCHMARK_ARM.HYBRID_WATERFALL,
      provider: "HYBRID_WATERFALL",
      keyConfigured: null,
      firstPartyAdapter: "composes Exa / Parallel / SerpAPI / native under gap router",
      adapterReady: true,
      listPriceUnit: "sum of invoked rungs; must not bypass shared budget",
      listPriceUsdStatus: COST_STATUS.UNKNOWN,
      notes: "Adaptive cascade; shared ledger + hard ceilings required.",
      sources: [],
      feasibility: "READY_FOR_P3B_AFTER_AUTH",
    }),
  ];

  return {
    asOf: PRICING_AS_OF,
    gdiHelperPriceAsOf: GDI_PRICE_AS_OF,
    surfeExcluded: true,
    pdlExcluded: true,
    paidExecutionAuthorized: false,
    exaLiteBoundedQuote: exaLiteQuote,
    providers: matrix,
    rules: [
      "Never log or echo API credentials",
      "Unknown pricing → refuse spend (never invent $0)",
      "Account remaining balances = UNKNOWN until founder-authorized dashboard read",
      "P3A paid budget = $0",
    ],
  };
}
