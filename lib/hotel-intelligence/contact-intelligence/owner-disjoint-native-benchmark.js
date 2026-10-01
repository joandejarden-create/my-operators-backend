/**
 * Owner-disjoint native benchmark — fail-closed hotel input + budget helpers.
 * Used by the runner and offline tests. No paid providers here.
 */
import { ownershipQueries } from "./ownership-research-planning.js";

export const OWNER_DISJOINT_BENCHMARK_CAPS = Object.freeze({
  max_hotels: 5,
  /** Combined Context.dev credits across BOTH arms for the whole run. */
  context_dev_max_run: 40,
  /**
   * Per hotel × arm allocation. 5 hotels × 2 arms × 4 = 40 worst-case.
   * Must keep worst-case ≤ context_dev_max_run.
   */
  context_dev_max_per_hotel_arm: 4,
  model_usd_max_run: 0.25,
  model_usd_max_per_hotel_arm: 0.05,
  serpapi_max: 0,
});

export const FORBIDDEN_BENCHMARK_INPUT_KEYS = Object.freeze([
  "owner_name",
  "expected_owner",
  "teacher_owner",
  "known_url",
  "expected_person",
  "expected_email",
  "expected_domain",
  "ground_truth",
  "scoring_key",
  "curated_passage",
]);

/**
 * Canonical hotel object for researchHotelOwnershipContactPath (caseInput.hotel).
 */
export function buildHandoffHotel(h = {}) {
  return {
    hotel_id: h.hotel_id || null,
    hotel_name: String(h.hotel_name || "").trim(),
    aliases: Array.isArray(h.aliases) ? h.aliases : [],
    city: h.city || null,
    country: h.country || null,
    language: h.language || (h.country === "Brazil" ? "pt" : "en"),
    official_website_if_known: h.official_website || h.website || h.official_website_if_known || null,
  };
}

/**
 * Handoff caseInput shape matching the working flat-vs-iterative runner.
 */
export function buildHandoffCaseInput(hotel, { iterative = false } = {}) {
  const h = buildHandoffHotel(hotel);
  return {
    hotel: h,
    iterative_ownership_loop: Boolean(iterative),
    return_to_ownership_lane: true,
  };
}

export function hotelIdentityToken(hotel = {}) {
  const name = String(hotel.hotel_name || "").trim();
  if (!name) return "";
  // Prefer a stable core token (≥4 chars) for query containment checks.
  const core = name
    .replace(/,?\s+(?:an? |by )?(?:autograph collection|tapestry collection|lxr hotels|all[- ]inclusive|adults only).*$/i, "")
    .replace(/^(?:the|el|la|le|les|los|las)\s+/i, "")
    .trim();
  const tokens = core
    .split(/[^a-zA-ZÀ-ÿ0-9]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 4);
  return tokens[0] || core.slice(0, 24) || name.slice(0, 24);
}

export function queryContainsHotelIdentity(query, hotel) {
  const q = String(query || "").toLowerCase();
  const name = String(hotel.hotel_name || "").trim().toLowerCase();
  if (!name || !q) return false;
  if (q.includes(name)) return true;
  const token = hotelIdentityToken(hotel).toLowerCase();
  return Boolean(token && token.length >= 3 && q.includes(token));
}

export function isEmptyOrGenericOwnershipQuery(query) {
  const q = String(query || "").trim();
  if (!q) return true;
  if (q === '""' || q === "''") return true;
  if (/^""\s/.test(q) || /^""\s*\(/.test(q)) return true;
  // Ownership keywords alone with empty hotel quotes
  if (/^""\s+/.test(q)) return true;
  return false;
}

/**
 * Fail-closed hotel + query assertion. Throws before any paid call.
 */
export function assertHotelReadyForPaidResearch(caseInput, { queries = null } = {}) {
  const errors = [];
  if (!caseInput || typeof caseInput !== "object") {
    throw new Error("BENCHMARK_HOTEL_ASSERT:caseInput_missing");
  }
  if (!caseInput.hotel || typeof caseInput.hotel !== "object") {
    errors.push("hotel_object_missing — handoff expects caseInput.hotel");
  }
  const hotel = caseInput.hotel || {};
  const name = String(hotel.hotel_name || "").trim();
  if (!name) errors.push("hotel_name_empty");
  if (name && !/\S/.test(name)) errors.push("hotel_name_whitespace_only");
  if (!hotel.hotel_id) errors.push("hotel_id_missing");

  for (const k of FORBIDDEN_BENCHMARK_INPUT_KEYS) {
    if (Object.prototype.hasOwnProperty.call(caseInput, k)) {
      errors.push(`forbidden_top_level_key:${k}`);
    }
    if (Object.prototype.hasOwnProperty.call(hotel, k)) {
      errors.push(`forbidden_hotel_key:${k}`);
    }
  }

  const qs = queries != null ? queries : ownershipQueries(hotel);
  if (!Array.isArray(qs) || qs.length === 0) {
    errors.push("ownership_queries_empty");
  } else {
    for (const q of qs) {
      if (isEmptyOrGenericOwnershipQuery(q)) {
        errors.push(`empty_or_generic_query:${JSON.stringify(q)}`);
      }
      if (!queryContainsHotelIdentity(q, hotel)) {
        errors.push(`query_missing_hotel_identity:${JSON.stringify(q)}`);
      }
    }
  }

  if (errors.length) {
    const err = new Error(`BENCHMARK_HOTEL_ASSERT_FAILED:${errors.join("|")}`);
    err.code = "BENCHMARK_HOTEL_ASSERT_FAILED";
    err.errors = errors;
    err.hotel_id = hotel.hotel_id || null;
    throw err;
  }

  return {
    ok: true,
    hotel,
    queries: qs,
    first_query: qs[0],
    identity_token: hotelIdentityToken(hotel),
  };
}

/**
 * Normalize runner budgets to the handoff contract.
 * Canonical internal fields: context_dev_max, context_dev_spent, model_usd_max, model_usd_spent.
 *
 * Rejects legacy wrong keys (context_dev_credits) unless mapped via explicit allowLegacyAlias
 * — for fail-closed tests we reject them by default.
 */
export function normalizeBenchmarkBudgets(raw = {}, { allowLegacyAlias = false } = {}) {
  const errors = [];
  const unrecognized = [];

  // Fail closed on the known-wrong key unless explicitly aliased for migration tests.
  if (Object.prototype.hasOwnProperty.call(raw, "context_dev_credits") && !allowLegacyAlias) {
    errors.push("unrecognized_budget_field:context_dev_credits (use context_dev_max)");
  }
  if (Object.prototype.hasOwnProperty.call(raw, "spent") && !Object.prototype.hasOwnProperty.call(raw, "context_dev_spent")) {
    // soft: ignore bare spent on input object
  }

  let context_dev_max = raw.context_dev_max;
  if (context_dev_max == null && allowLegacyAlias && raw.context_dev_credits != null) {
    context_dev_max = raw.context_dev_credits;
  }

  let model_usd_max = raw.model_usd_max ?? raw.model_reader_usd_max;
  if (model_usd_max == null && allowLegacyAlias && raw.model_usd != null) {
    model_usd_max = raw.model_usd;
  }

  if (context_dev_max == null || context_dev_max === "" || Number.isNaN(Number(context_dev_max))) {
    errors.push("context_dev_max_undefined");
  }
  if (model_usd_max == null || model_usd_max === "" || Number.isNaN(Number(model_usd_max))) {
    errors.push("model_usd_max_undefined");
  }

  const known = new Set([
    "context_dev_max",
    "context_dev_spent",
    "model_usd_max",
    "model_usd_spent",
    "model_reader_usd_max",
    "iterative_ownership_loop",
    "serpapi_max",
    "serpapi_usd_max",
    "serpapi_per_search_usd",
    "apply_structured_reader",
    "apply_model_follow_up_planner",
    "allow_surfe",
    "allow_fullenrich",
    "allow_parallel",
    "allow_webhound",
    "canonical_writes",
    "iterative_breadth",
    "iterative_depth",
    "iterative_concurrency",
    "iterative_docs_per_query",
    "iterative_follow_up_reserve_fraction",
    "iterative_max_queries",
    "iterative_max_documents",
  ]);
  if (allowLegacyAlias) {
    known.add("context_dev_credits");
    known.add("model_usd");
  }
  for (const k of Object.keys(raw)) {
    if (!known.has(k)) unrecognized.push(k);
  }
  // Unrecognized budget-looking keys abort
  for (const k of unrecognized) {
    if (/credit|budget|usd|spend|max/i.test(k) && k !== "context_dev_credits") {
      errors.push(`unrecognized_budget_field:${k}`);
    }
  }

  if (errors.length) {
    const err = new Error(`BENCHMARK_BUDGET_ASSERT_FAILED:${errors.join("|")}`);
    err.code = "BENCHMARK_BUDGET_ASSERT_FAILED";
    err.errors = errors;
    throw err;
  }

  const contextMax = Number(context_dev_max);
  const modelMax = Number(model_usd_max);
  if (!(contextMax >= 0) || !(modelMax >= 0)) {
    const err = new Error("BENCHMARK_BUDGET_ASSERT_FAILED:negative_or_invalid_budget");
    err.code = "BENCHMARK_BUDGET_ASSERT_FAILED";
    throw err;
  }

  return {
    context_dev_max: contextMax,
    context_dev_spent: Number(raw.context_dev_spent || 0),
    model_usd_max: modelMax,
    model_usd_spent: Number(raw.model_usd_spent || 0),
    // Handoff-facing payload (engine contract).
    // Only OWNERSHIP_HANDOFF_ALLOWED_BUDGET_KEYS — model_usd_max / canonical_writes are rejected.
    handoff_budgets: {
      context_dev_max: contextMax,
      model_reader_usd_max: modelMax,
      serpapi_max: Number(raw.serpapi_max ?? OWNER_DISJOINT_BENCHMARK_CAPS.serpapi_max),
      iterative_ownership_loop: Boolean(raw.iterative_ownership_loop),
      apply_structured_reader: raw.apply_structured_reader !== false,
      apply_model_follow_up_planner: raw.apply_model_follow_up_planner !== false,
      allow_surfe: false,
      allow_fullenrich: false,
      allow_parallel: false,
      allow_webhound: false,
      allow_enrichment: false,
      enrichment_max: 0,
      surfe_max: 0,
      fullenrich_max: 0,
      pdl_max: 0,
      parallel_max: 0,
      webhound_max: 0,
    },
  };
}

export function readContextSpendFromResearch(research = {}) {
  const b = research?.budgets?.context_dev || {};
  return Number(b.spent_credits ?? b.context_dev_spent ?? 0);
}

export function readModelSpendFromResearch(research = {}) {
  const b = research?.budgets?.model_usd || {};
  return Number(b.spent_usd ?? b.model_usd_spent ?? b.spent ?? 0);
}

/**
 * Shared run ledger — abort before a call that would exceed remaining reserve.
 */
export function createRunBudgetLedger({
  context_dev_max = OWNER_DISJOINT_BENCHMARK_CAPS.context_dev_max_run,
  model_usd_max = OWNER_DISJOINT_BENCHMARK_CAPS.model_usd_max_run,
} = {}) {
  let context_dev_spent = 0;
  let model_usd_spent = 0;
  const entries = [];

  function remainingContext() {
    return Math.max(0, context_dev_max - context_dev_spent);
  }
  function remainingModel() {
    return Math.max(0, model_usd_max - model_usd_spent);
  }

  function allocateHotelArm({
    hotel_id,
    arm,
    perHotelContextMax = OWNER_DISJOINT_BENCHMARK_CAPS.context_dev_max_per_hotel_arm,
    perHotelModelMax = 0,
    iterative_ownership_loop = null,
    apply_structured_reader = null,
    apply_model_follow_up_planner = null,
  }) {
    const remCtx = remainingContext();
    const remModel = remainingModel();
    if (remCtx <= 0) {
      const err = new Error("BENCHMARK_BUDGET_EXHAUSTED:context_dev_run_cap");
      err.code = "BENCHMARK_BUDGET_EXHAUSTED";
      throw err;
    }
    const context_dev_max = Math.min(perHotelContextMax, remCtx);
    const model_usd_max = Math.min(perHotelModelMax, remModel);
    if (!(context_dev_max > 0)) {
      const err = new Error("BENCHMARK_BUDGET_EXHAUSTED:no_context_for_hotel_arm");
      err.code = "BENCHMARK_BUDGET_EXHAUSTED";
      throw err;
    }
    // Legacy owner-disjoint arms keep prior defaults; workflow arms must not inherit
    // NATIVE_PATCHED_ITERATIVE-only reader flags (that silently disabled model assist).
    const iterativeDefault =
      arm === "NATIVE_PATCHED_ITERATIVE" ||
      arm === "NATIVE_DISCOVERY_ONLY" ||
      arm === "E2E_WORKFLOW";
    const readerDefault =
      arm === "NATIVE_PATCHED_ITERATIVE" ||
      ((arm === "NATIVE_DISCOVERY_ONLY" || arm === "E2E_WORKFLOW") && model_usd_max > 0);
    return normalizeBenchmarkBudgets({
      context_dev_max,
      context_dev_spent: 0,
      model_usd_max,
      model_usd_spent: 0,
      iterative_ownership_loop:
        iterative_ownership_loop != null ? Boolean(iterative_ownership_loop) : iterativeDefault,
      apply_structured_reader:
        apply_structured_reader != null ? Boolean(apply_structured_reader) : readerDefault,
      apply_model_follow_up_planner:
        apply_model_follow_up_planner != null
          ? Boolean(apply_model_follow_up_planner)
          : readerDefault,
      serpapi_max: 0,
    });
  }

  function recordSpend({ hotel_id, arm, context_dev_spent: c, model_usd_spent: m, requested }) {
    const ctx = Number(c) || 0;
    const model = Number(m) || 0;
    if (ctx > Number(requested?.context_dev_max || 0) + 1e-9) {
      const err = new Error(
        `BENCHMARK_BUDGET_OVERSPEND:hotel_arm_context hotel=${hotel_id} spent=${ctx} max=${requested?.context_dev_max}`
      );
      err.code = "BENCHMARK_BUDGET_OVERSPEND";
      throw err;
    }
    if (context_dev_spent + ctx > context_dev_max + 1e-9) {
      const err = new Error(
        `BENCHMARK_BUDGET_OVERSPEND:run_context spent=${context_dev_spent + ctx} max=${context_dev_max}`
      );
      err.code = "BENCHMARK_BUDGET_OVERSPEND";
      throw err;
    }
    if (model_usd_spent + model > model_usd_max + 1e-6) {
      const err = new Error(
        `BENCHMARK_BUDGET_OVERSPEND:run_model spent=${model_usd_spent + model} max=${model_usd_max}`
      );
      err.code = "BENCHMARK_BUDGET_OVERSPEND";
      throw err;
    }
    context_dev_spent += ctx;
    model_usd_spent += model;
    entries.push({
      at: new Date().toISOString(),
      hotel_id,
      arm,
      context_dev_spent: ctx,
      model_usd_spent: model,
      requested_context_dev_max: requested?.context_dev_max ?? null,
      requested_model_usd_max: requested?.model_usd_max ?? null,
    });
  }

  function snapshot() {
    return {
      context_dev_max,
      context_dev_spent,
      context_dev_remaining: remainingContext(),
      model_usd_max,
      model_usd_spent,
      model_usd_remaining: remainingModel(),
      entries: entries.slice(),
    };
  }

  return {
    allocateHotelArm,
    recordSpend,
    remainingContext,
    remainingModel,
    snapshot,
    get context_dev_spent() {
      return context_dev_spent;
    },
    get model_usd_spent() {
      return model_usd_spent;
    },
  };
}

export function worstCaseContextSpend({
  hotelCount = OWNER_DISJOINT_BENCHMARK_CAPS.max_hotels,
  arms = 2,
  perHotelArm = OWNER_DISJOINT_BENCHMARK_CAPS.context_dev_max_per_hotel_arm,
} = {}) {
  return hotelCount * arms * perHotelArm;
}

/**
 * Zero-cost preflight for live execution.
 */
export function buildLivePreflight(hotels, caps = OWNER_DISJOINT_BENCHMARK_CAPS) {
  const hotelRows = hotels.map((h) => {
    const caseInput = buildHandoffCaseInput(h, { iterative: false });
    const asserted = assertHotelReadyForPaidResearch(caseInput);
    return {
      hotel_id: h.hotel_id,
      hotel_name: h.hotel_name,
      city: h.city || null,
      country: h.country || null,
      first_query: asserted.first_query,
      query_count: asserted.queries.length,
      context_dev_max_per_arm: caps.context_dev_max_per_hotel_arm,
      handoff_shape_ok: Boolean(caseInput.hotel?.hotel_name),
    };
  });

  const worstCase = worstCaseContextSpend({
    hotelCount: hotels.length,
    arms: 2,
    perHotelArm: caps.context_dev_max_per_hotel_arm,
  });

  const errors = [];
  if (hotels.length === 0) errors.push("no_hotels");
  if (hotels.length > caps.max_hotels) errors.push("too_many_hotels");
  if (worstCase > caps.context_dev_max_run) {
    errors.push(`worst_case_context_${worstCase}_exceeds_cap_${caps.context_dev_max_run}`);
  }
  for (const row of hotelRows) {
    if (!row.first_query || isEmptyOrGenericOwnershipQuery(row.first_query)) {
      errors.push(`empty_query:${row.hotel_id}`);
    }
    if (!row.handoff_shape_ok) errors.push(`handoff_shape:${row.hotel_id}`);
  }
  if (caps.context_dev_max_per_hotel_arm == null) errors.push("context_dev_max_per_hotel_arm_undefined");
  if (caps.context_dev_max_run == null) errors.push("context_dev_max_run_undefined");

  // Identical inputs check: flat and patched must see the same hotel objects
  const flatInputs = hotels.map((h) => buildHandoffCaseInput(h, { iterative: false }).hotel);
  const patchedInputs = hotels.map((h) => buildHandoffCaseInput(h, { iterative: true }).hotel);
  if (JSON.stringify(flatInputs) !== JSON.stringify(patchedInputs)) {
    errors.push("flat_patched_hotel_inputs_differ");
  }

  return {
    ok: errors.length === 0,
    errors,
    hotel_count: hotels.length,
    hotels: hotelRows,
    per_hotel_context_budget: caps.context_dev_max_per_hotel_arm,
    combined_context_cap: caps.context_dev_max_run,
    worst_case_context_spend: worstCase,
    model_cap: caps.model_usd_max_run,
    model_per_hotel_arm_patched: caps.model_usd_max_per_hotel_arm,
    providers: {
      context_dev: true,
      serpapi: false,
      parallel: false,
      webhound: false,
      surfe: false,
      fullenrich: false,
      apify: false,
    },
    flags: {
      enrichment: false,
      outreach: false,
      customer_publication: false,
      airtable_census_writes: false,
      canonical_ownership_writes: false,
    },
    confirm_spend_required: true,
  };
}
