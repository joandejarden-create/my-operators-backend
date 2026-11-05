/**
 * AVA research provider policy — classifies Exa / Parallel / Context.dev / Ahrefs.
 * Enforces: no billable calls without founder authorization.
 */
import { resolveAvaResearchAuthorization } from "./research-authorization-v1.js";

export const AVA_RESEARCH_PROVIDER_POLICY_CONTRACT =
  "AVA_RESEARCH_PROVIDER_POLICY_V1";

export const PROVIDER_USAGE_CLASS = Object.freeze({
  DEFAULT: "DEFAULT",
  CONDITIONAL: "CONDITIONAL",
  OPTIONAL_DEEP_DIVE: "OPTIONAL_DEEP_DIVE",
  DO_NOT_USE: "DO_NOT_USE",
});

/**
 * Static policy derived from repo integrations + entitlements (read-only).
 * Costs are estimates for founder authorization — not live invoices.
 */
export function getAvaResearchProviderPolicy(env = process.env) {
  const auth = resolveAvaResearchAuthorization({});
  const has = (k) => Boolean(String(env[k] || "").trim());

  return {
    contract: AVA_RESEARCH_PROVIDER_POLICY_CONTRACT,
    generatedAt: new Date().toISOString(),
    paidResearchAuthorized: auth.paidResearchAuthorized,
    authorizedBudgetUsd: auth.authorizedBudgetUsd,
    providers: [
      {
        id: "exa",
        name: "Exa",
        classification: PROVIDER_USAGE_CLASS.DEFAULT,
        keyPresent: has("EXA_API_KEY") || has("EXA_KEY"),
        integratedInAva: true,
        role: "Primary public discovery for Digital Authority V2 candidate pages",
        endpointsUsed: ["POST /search"],
        incrementalValue:
          "Discovers destination/CVB/guide candidates; does not replace a backlink index",
        estimatedCostModel: "Usage-based Exa credits/API (existing Dealality Exa account)",
        estimatedCostPerHotelUsd: "Typically low single-digit credits for ≤10 searches; confirm Exa dashboard",
        billableWithoutAuth: false,
        note: "Already the AVA V2 default path. Additional Exa volume still counts as research spend — bound searches per hotel.",
      },
      {
        id: "parallel",
        name: "Parallel",
        classification: PROVIDER_USAGE_CLASS.CONDITIONAL,
        keyPresent: has("PARALLEL_KEY") || has("PARALLEL_API_KEY"),
        integratedInAva: false,
        role: "Supplementary deep research for hard hotel-specific questions / multilingual gaps Exa missed",
        endpointsAvailable: ["Task API (lite/base/core/pro)", "FindAll (preview/base)"],
        incrementalValue:
          "Broader research synthesis when Exa recall is thin; not a backlink database",
        estimatedCostModel:
          "Task list prices in-repo: lite $0.005, base $0.01, core $0.025, pro $0.10 per task (verify Parallel dashboard)",
        estimatedCostPerHotelUsd:
          "Pilot envelope: 2–4 core tasks ≈ $0.05–$0.10 + FindAll preview/base if used",
        billableWithoutAuth: true,
        requiresFounderAuth: true,
        note: "Client exists under hotel-intelligence / GDI. Do not enable as AVA default.",
      },
      {
        id: "context_dev",
        name: "Context.dev",
        classification: PROVIDER_USAGE_CLASS.CONDITIONAL,
        keyPresent: has("CONTEXT_DEV_API_KEY"),
        integratedInAva: false,
        role: "JS-heavy scrape, structured extract, brand retrieve for bot-protected or incomplete HTML",
        endpointsAvailable: [
          "POST /web/extract (10 credits)",
          "POST /web/search (1 credit / 10 results)",
          "GET /web/scrape/markdown (1 credit)",
          "POST /brand/retrieve (10 credits)",
        ],
        incrementalValue:
          "May recover facts from WAF/JS pages and extract structured hotel attributes for DCR",
        estimatedCostModel: "Credit ledger in lib/context-dev/credit-ledger.js",
        estimatedCostPerHotelUsd:
          "Pilot envelope: ~15–40 credits/hotel (search+scrape+optional extract) — convert via Context.dev plan rate",
        billableWithoutAuth: true,
        requiresFounderAuth: true,
        note: "Best for ACCESS_RESTRICTED brand pages and schema/fact extraction — not for backlink graphs.",
      },
      {
        id: "ahrefs",
        name: "Ahrefs",
        classification: PROVIDER_USAGE_CLASS.OPTIONAL_DEEP_DIVE,
        keyPresent: false,
        integratedInAva: false,
        role: "Referring-domain / URL backlink index for verified property backlinks + competitor gaps",
        endpointsAvailable: "Not integrated in this repo (no Ahrefs client or API token in env)",
        incrementalValue:
          "Highest incremental value for complete backlink inventories Ahrefs/Semrush-class indexes provide",
        estimatedCostModel:
          "Public Lite ~$129/mo class + API units (max(50, rows×per-row)); confirm current plan",
        estimatedCostPerHotelUsd:
          "Pilot 3 hotels: expect tens–hundreds of API units depending on referring-domain depth — quote before buy",
        billableWithoutAuth: true,
        requiresFounderAuth: true,
        note: "Listed in AVA paid-provider allowlist but **no API key and no client**. Account entitlement must be confirmed outside this repo before any call.",
      },
      {
        id: "dataforseo",
        name: "DataForSEO",
        classification: PROVIDER_USAGE_CLASS.OPTIONAL_DEEP_DIVE,
        keyPresent: has("DATAFORSEO_LOGIN") && has("DATAFORSEO_PASSWORD"),
        integratedInAva: false,
        role: "Optional pay-as-you-go backlinks + maps/SERP for listing URL discovery",
        estimatedCostModel: "Pay-as-you-go; backlinks ~$0.02+/task class — verify live price list",
        estimatedCostPerHotelUsd: "Pilot 3 hotels: set hard cap e.g. $15–25 total before run",
        billableWithoutAuth: true,
        requiresFounderAuth: true,
        note: "Credentials exist for census paths; AVA paid research flag still required. Not a substitute for Ahrefs quality without validation.",
      },
    ],
    defaultPipeline: {
      digitalAuthority: "exa + HTML inspection (V2)",
      digitalContentReadiness: "identity graph + zero-cost HTTP platform probes",
      paidEnrichment: "off until founder authorization + budget",
    },
    doNotUse: [
      "Semrush / Majestic / Moz — no entitlement confirmed for AVA; do not subscribe",
      "Any provider call that bypasses AVA_PAID_RESEARCH_AUTHORIZED for paid backlink indexes",
    ],
  };
}

export function assertProviderCallAllowed(providerId, opts = {}) {
  const policy = getAvaResearchProviderPolicy(opts.env || process.env);
  const p = policy.providers.find((x) => x.id === providerId);
  if (!p) {
    const err = new Error(`AVA_UNKNOWN_PROVIDER:${providerId}`);
    err.code = "AVA_UNKNOWN_PROVIDER";
    throw err;
  }
  if (p.classification === PROVIDER_USAGE_CLASS.DO_NOT_USE) {
    const err = new Error(`AVA_PROVIDER_DO_NOT_USE:${providerId}`);
    err.code = "AVA_PROVIDER_DO_NOT_USE";
    throw err;
  }
  if (p.requiresFounderAuth && !policy.paidResearchAuthorized && providerId !== "exa") {
    // Exa DEFAULT path is separately bounded by search caps in DA V2
    if (providerId === "exa" && opts.exaDefaultPath === true) {
      return { ok: true, policy, provider: p };
    }
    if (providerId !== "exa") {
      const err = new Error(`AVA_PAID_RESEARCH_NOT_AUTHORIZED:${providerId}`);
      err.code = "AVA_PAID_RESEARCH_NOT_AUTHORIZED";
      throw err;
    }
  }
  if (
    (providerId === "ahrefs" ||
      providerId === "parallel" ||
      providerId === "context_dev" ||
      providerId === "dataforseo") &&
    !policy.paidResearchAuthorized
  ) {
    const err = new Error(`AVA_PAID_RESEARCH_NOT_AUTHORIZED:${providerId}`);
    err.code = "AVA_PAID_RESEARCH_NOT_AUTHORIZED";
    throw err;
  }
  return { ok: true, policy, provider: p };
}
