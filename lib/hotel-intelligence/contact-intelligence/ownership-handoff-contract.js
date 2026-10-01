/**
 * Ownership handoff execution contract — validate hotel-only inputs + shared budgets.
 * Staging research only. Does not change production API routing.
 */

export const OWNERSHIP_HANDOFF_CONTRACT_VERSION = "ownership-handoff-contract-v1";

/** Budget keys accepted by researchHotelOwnershipContactPath (+ method router). */
export const OWNERSHIP_HANDOFF_ALLOWED_BUDGET_KEYS = Object.freeze([
  "serpapi_max",
  "serpapi_usd_max",
  "serpapi_per_search_usd",
  "context_dev_max",
  /** remaining = incremental window already net of prior; cumulative = case cap (handoff seeds alreadySpent) */
  "context_dev_allowance_mode",
  "iterative_ownership_loop",
  "iterative_breadth",
  "iterative_depth",
  "iterative_concurrency",
  "iterative_docs_per_query",
  "iterative_follow_up_reserve_fraction",
  "iterative_max_queries",
  "iterative_max_documents",
  "model_reader_usd_max",
  "apply_structured_reader",
  "apply_model_follow_up_planner",
  "native_method_router",
  "disable_network",
  "follow_up_query_max",
  // Paid / external allowances — omitted → 0 / false (never invent spend)
  "parallel_max",
  "webhound_max",
  "surfe_max",
  "fullenrich_max",
  "enrichment_max",
  "pdl_max",
  "allow_parallel",
  "allow_webhound",
  "allow_surfe",
  "allow_fullenrich",
  "allow_enrichment",
  "allow_pdl",
]);

const PAID_NUMERIC_DEFAULT_ZERO = [
  "parallel_max",
  "webhound_max",
  "surfe_max",
  "fullenrich_max",
  "enrichment_max",
  "pdl_max",
];

const PAID_BOOL_DEFAULT_FALSE = [
  "allow_parallel",
  "allow_webhound",
  "allow_surfe",
  "allow_fullenrich",
  "allow_enrichment",
  "allow_pdl",
];

/** Mojibake / replacement-char signals — flag only; never invent replacement names. */
export function detectHotelNameEncodingIssues(name) {
  const s = String(name || "");
  const issues = [];
  if (!s.trim()) return { ok: false, issues: ["EMPTY_NAME"], mojibake_suspected: false };
  if (/\uFFFD|�/.test(s)) issues.push("REPLACEMENT_CHAR");
  // Common UTF-8→Latin-1 mojibake fragments seen in BR hotel freezes
  if (/Ã.|Â.|Ã£|Ã¡|Ã©|Ã­|Ã³|Ãº|Ã§|Ã±/i.test(s)) issues.push("MOJIBAKE_PATTERN");
  if (/SÃo|SÃ£o|CristovÃo|JosÃ©/i.test(s)) issues.push("KNOWN_PT_MOJIBAKE");
  return {
    ok: issues.length === 0,
    issues,
    mojibake_suspected: issues.length > 0,
    original_name: s,
  };
}

/**
 * Normalize budgets: reject unknown keys; omitted paid allowances → 0/false.
 * Preserves existing serpapi_max / context_dev_max defaults when omitted
 * (flag-OFF behavior unchanged at call sites that rely on those defaults).
 */
export function normalizeOwnershipHandoffBudgets(budgets = {}) {
  const raw = budgets && typeof budgets === "object" ? budgets : {};
  const unknown = Object.keys(raw).filter((k) => !OWNERSHIP_HANDOFF_ALLOWED_BUDGET_KEYS.includes(k));
  if (unknown.length) {
    return {
      ok: false,
      error: "UNSUPPORTED_BUDGET_KEYS",
      unknown_keys: unknown,
      budgets: null,
    };
  }

  const out = { ...raw };
  for (const k of PAID_NUMERIC_DEFAULT_ZERO) {
    if (out[k] == null || out[k] === "") out[k] = 0;
  }
  for (const k of PAID_BOOL_DEFAULT_FALSE) {
    if (out[k] == null || out[k] === "") out[k] = false;
  }
  // Hard-disable paid enrichment / Parallel / Webhound unless explicitly allowed
  if (!out.allow_parallel) out.parallel_max = 0;
  if (!out.allow_webhound) out.webhound_max = 0;
  if (!out.allow_surfe) out.surfe_max = 0;
  if (!out.allow_fullenrich) out.fullenrich_max = 0;
  if (!out.allow_enrichment) out.enrichment_max = 0;
  if (!out.allow_pdl) out.pdl_max = 0;

  return { ok: true, error: null, unknown_keys: [], budgets: out };
}

/**
 * Validate hotel-only handoff input before any provider call.
 * Preserves original hotel fields; does not guess replacement names.
 */
export function validateOwnershipHandoffInput(caseInput = {}, budgets = {}) {
  const hotel = caseInput?.hotel;
  if (!hotel || typeof hotel !== "object") {
    return {
      ok: false,
      error: "MISSING_CASE_INPUT_HOTEL",
      message: "caseInput.hotel must exist",
      hotel: null,
      budgets: null,
      encoding: null,
    };
  }

  const name = hotel.hotel_name ?? hotel.property_name ?? hotel.name ?? "";
  if (!String(name).trim()) {
    return {
      ok: false,
      error: "EMPTY_HOTEL_NAME",
      message: "hotel name must be nonempty",
      hotel: null,
      budgets: null,
      encoding: detectHotelNameEncodingIssues(name),
    };
  }

  const encoding = detectHotelNameEncodingIssues(name);
  const budgetNorm = normalizeOwnershipHandoffBudgets(budgets);
  if (!budgetNorm.ok) {
    return {
      ok: false,
      error: budgetNorm.error,
      message: `Unsupported budget keys: ${(budgetNorm.unknown_keys || []).join(", ")}`,
      hotel: null,
      budgets: null,
      encoding,
      unknown_keys: budgetNorm.unknown_keys,
    };
  }

  // Preserve originals — shallow freeze snapshot for audit
  const preserved = {
    hotel_id: hotel.hotel_id ?? hotel.id ?? null,
    hotel_name: hotel.hotel_name ?? null,
    property_name: hotel.property_name ?? null,
    name: hotel.name ?? null,
    city: hotel.city ?? null,
    state: hotel.state ?? hotel.state_region ?? hotel.uf ?? null,
    country: hotel.country ?? null,
    language: hotel.language ?? null,
    latitude: hotel.latitude ?? hotel.lat ?? null,
    longitude: hotel.longitude ?? hotel.lng ?? hotel.lon ?? null,
    address: hotel.address ?? null,
    census_id: hotel.census_id ?? null,
    identifiers: hotel.identifiers ? { ...hotel.identifiers } : null,
  };

  return {
    ok: true,
    error: null,
    message: null,
    hotel: {
      ...hotel,
      // Ensure hotel_name is present for downstream queries without replacing encoding
      hotel_name: String(hotel.hotel_name || hotel.property_name || hotel.name || "").trim(),
    },
    preserved_identity: preserved,
    budgets: budgetNorm.budgets,
    encoding,
    encoding_flagged: encoding.mojibake_suspected,
  };
}

/**
 * Documented difference: Hotel Explorer / contact-intelligence API vs this handoff.
 * Production routing must not change in the method-router task.
 */
export const OWNERSHIP_HANDOFF_VS_HOTEL_EXPLORER = Object.freeze({
  handoff_entry: "researchHotelOwnershipContactPath",
  handoff_file: "lib/hotel-intelligence/contact-intelligence/ownership-contact-research-handoff.js",
  hotel_explorer_api: "api/hotel-intelligence-research.js + research orchestrator / playbooks",
  contact_intelligence_api: "api/contact-intelligence.js (read surface; no live ownership research)",
  service_js: "lib/hotel-intelligence/contact-intelligence/service.js (surface reads; no handoff call)",
  difference:
    "Hotel Explorer research uses Research Center / Webhound / blind-native / playbooks. " +
    "researchHotelOwnershipContactPath is the Contact Intelligence staging handoff used by " +
    "evaluation/benchmark scripts. They are separate entry points; this task does not merge them.",
  production_routing_changed: false,
});
