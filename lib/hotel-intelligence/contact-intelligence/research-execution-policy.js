/**
 * Server-owned execution policy for full research workflow.
 * Trusted denials always override client/request budget flags.
 */

export const RESEARCH_EXECUTION_POLICY_VERSION = "research-execution-policy-v1";

export const SUPPORTED_RESEARCH_OBJECTIVES = Object.freeze([
  "HOTEL_OWNERSHIP_CONTACT",
  "HOTEL_OWNERSHIP_ONLY",
  "OWNER_PORTFOLIO",
  "CONTACT_INTELLIGENCE",
  "GDI_DEVELOPMENT",
  "ADP_RESEARCH",
  "OPEN_RESEARCH",
]);

/** Objectives with a connected ownership/contact execution path today. */
export const CONNECTED_OWNERSHIP_OBJECTIVES = Object.freeze([
  "HOTEL_OWNERSHIP_CONTACT",
  "HOTEL_OWNERSHIP_ONLY",
  "CONTACT_INTELLIGENCE",
]);

const ALLOWED_BUDGET_KEYS = new Set([
  "serpapi_max",
  "serpapi_usd_max",
  "serpapi_per_search_usd",
  "context_dev_max",
  /** remaining = incremental window (prior already netted upstream); cumulative = case cap */
  "context_dev_allowance_mode",
  "shared_pool_mode",
  "enrichment_max",
  "surfe_max",
  "fullenrich_max",
  "pdl_max",
  "parallel_max",
  "webhound_max",
  "model_reader_usd_max",
  "model_usd_max",
  "max_elapsed_ms",
  "max_people",
  "max_people_per_owner",
  "enable_contact_enrichment",
  "disable_network",
  "contact_provider",
  "iterative_ownership_loop",
  "iterative_breadth",
  "iterative_depth",
  "iterative_concurrency",
  "iterative_docs_per_query",
  "iterative_follow_up_reserve_fraction",
  "iterative_max_queries",
  "iterative_max_documents",
  "apply_structured_reader",
  "apply_model_follow_up_planner",
  "native_method_router",
]);

function finiteNonNeg(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return { ok: false, value: fallback, reason: "INVALID_BUDGET_NUMBER" };
  return { ok: true, value: n };
}

/**
 * Normalize request budgets then apply trusted policy last (deny wins).
 *
 * @param {object} requestBudgets
 * @param {{
 *   networkBlocked?: boolean,
 *   enableContactEnrichment?: boolean|null,
 *   env?: object,
 * }} trusted
 */
export function buildTrustedExecutionPolicy(requestBudgets = {}, trusted = {}) {
  const env = trusted.env || process.env;
  const unknown_keys = [];
  const invalid = [];
  const raw = requestBudgets && typeof requestBudgets === "object" ? requestBudgets : {};

  for (const key of Object.keys(raw)) {
    if (!ALLOWED_BUDGET_KEYS.has(key) && !key.startsWith("_")) {
      unknown_keys.push(key);
    }
  }

  const out = {};
  for (const key of [
    "serpapi_max",
    "serpapi_usd_max",
    "serpapi_per_search_usd",
    "context_dev_max",
    "enrichment_max",
    "surfe_max",
    "fullenrich_max",
    "pdl_max",
    "parallel_max",
    "webhound_max",
    "model_reader_usd_max",
    "model_usd_max",
    "max_elapsed_ms",
    "max_people",
    "max_people_per_owner",
  ]) {
    if (raw[key] == null) continue;
    const parsed = finiteNonNeg(raw[key], 0);
    if (!parsed.ok) invalid.push(key);
    out[key] = parsed.value;
  }

  // Defaults: zero means no attempted call for capped providers
  if (out.serpapi_max == null) out.serpapi_max = 0;
  if (out.context_dev_max == null) out.context_dev_max = 0;
  if (out.enrichment_max == null) out.enrichment_max = 0;
  if (out.surfe_max == null) out.surfe_max = out.enrichment_max;
  if (out.fullenrich_max == null) out.fullenrich_max = 0;
  if (out.pdl_max == null) out.pdl_max = 0;
  if (out.parallel_max == null) out.parallel_max = 0;
  if (out.webhound_max == null) out.webhound_max = 0;

  for (const key of [
    "enable_contact_enrichment",
    "disable_network",
    "iterative_ownership_loop",
    "apply_structured_reader",
    "apply_model_follow_up_planner",
    "native_method_router",
    "shared_pool_mode",
  ]) {
    if (raw[key] != null) out[key] = Boolean(raw[key]);
  }
  if (raw.contact_provider != null) out.contact_provider = String(raw.contact_provider);
  // Preserve remaining vs cumulative Context.dev window semantics for shared-pool resume.
  if (raw.context_dev_allowance_mode != null) {
    const mode = String(raw.context_dev_allowance_mode).trim().toLowerCase();
    if (mode === "remaining" || mode === "cumulative") {
      out.context_dev_allowance_mode = mode;
    } else {
      invalid.push("context_dev_allowance_mode");
    }
  }
  for (const key of [
    "iterative_breadth",
    "iterative_depth",
    "iterative_concurrency",
    "iterative_docs_per_query",
    "iterative_follow_up_reserve_fraction",
    "iterative_max_queries",
    "iterative_max_documents",
  ]) {
    if (raw[key] == null) continue;
    const parsed = finiteNonNeg(raw[key], 0);
    if (!parsed.ok) invalid.push(key);
    else out[key] = parsed.value;
  }

  const envNetworkDenied = String(env.RESEARCH_WORKFLOW_DISABLE_NETWORK || "") === "1";
  const trustedNetworkDenied = trusted.networkBlocked === true || envNetworkDenied;
  // Deny precedence: trusted/env denial cannot be overridden by client false
  out.disable_network = trustedNetworkDenied || out.disable_network === true;

  const trustedEnrichmentDenied = trusted.enableContactEnrichment === false;
  const clientWantsEnrichment = out.enable_contact_enrichment === true;
  const trustedOptIn = trusted.enableContactEnrichment === true;
  const enrichmentCap = Number(out.enrichment_max || 0);

  let enable_contact_enrichment = false;
  if (!out.disable_network && !trustedEnrichmentDenied && enrichmentCap > 0) {
    enable_contact_enrichment = trustedOptIn || (trusted.enableContactEnrichment !== false && clientWantsEnrichment);
  }
  out.enable_contact_enrichment = enable_contact_enrichment;

  // Zero caps: force nested paid flags off
  if (enrichmentCap <= 0) {
    out.enable_contact_enrichment = false;
    out.surfe_max = 0;
    out.fullenrich_max = 0;
    out.pdl_max = 0;
  }
  if (out.disable_network) {
    out.enable_contact_enrichment = false;
  }

  return {
    version: RESEARCH_EXECUTION_POLICY_VERSION,
    ok: invalid.length === 0,
    unknown_keys,
    invalid_keys: invalid,
    budgets: out,
    trusted: {
      network_denied: trustedNetworkDenied,
      enrichment_denied: trustedEnrichmentDenied || enrichmentCap <= 0 || out.disable_network,
      enable_contact_enrichment: out.enable_contact_enrichment,
    },
  };
}

export function validateResearchObjective(objective) {
  const o = String(objective || "").toUpperCase();
  if (!SUPPORTED_RESEARCH_OBJECTIVES.includes(o)) {
    return { ok: false, objective: o, reason: "UNKNOWN_OBJECTIVE" };
  }
  return {
    ok: true,
    objective: o,
    connected_ownership_path: CONNECTED_OWNERSHIP_OBJECTIVES.includes(o),
    unsupported_until_adapter: !CONNECTED_OWNERSHIP_OBJECTIVES.includes(o) && o !== "ADP_RESEARCH",
  };
}

/**
 * Remaining allowance after prior cumulative usage.
 * Zero remaining ⇒ no attempted call.
 */
export function remainingAllowance(max, used) {
  const m = Number(max);
  const u = Number(used || 0);
  if (!Number.isFinite(m) || m <= 0) return 0;
  return Math.max(0, m - (Number.isFinite(u) ? u : 0));
}
