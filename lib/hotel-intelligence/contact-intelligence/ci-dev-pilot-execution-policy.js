/**
 * Unified DEVELOPMENT pilot execution policy for ci-20hotel / five-hotel runner.
 * Dry-run and live execution must share this object — declarations are not advisory.
 */

import {
  createPilotSpendTracker as createDurablePilotSpendTracker,
  buildPilotId,
} from "./ci-dev-pilot-budget-ledger.js";

export const CI_DEV_PILOT_POLICY_VERSION = "ci-dev-pilot-execution-policy-v2";

/** Explicit DEVELOPMENT mode: hard credit caps; USD may be UNKNOWN. */
export const CREDIT_LIMITED_USD_UNKNOWN_MODE = "credit_limited_usd_unknown";

/** Founder-authorized cumulative Context.dev hard cap for the five-hotel DEVELOPMENT pilot. */
export const CONTEXT_DEV_PILOT_CREDIT_HARD_CAP_DEFAULT = 500;

/** Frozen five-hotel set for the authorized DEVELOPMENT pilot. */
export const CI_20HOTEL_AUTHORIZED_FIVE_IDS = Object.freeze([
  "rec07GLNG5oNBkAj8",
  "rec00Cp2AgpZAtp9Z",
  "rec08YF1M2BOFBcYG",
  "rec00eC8DN3Ow0hFo",
  "rec09k8RdHgiAAKsG",
]);

export function resolveCreditLimitedUsdUnknownMode({ argv = [], env = process.env } = {}) {
  const flag =
    argv.includes("--credit-limited-development") ||
    argv.some((a) => a.startsWith("--credit-limited-development="));
  const envOn =
    String(env.CI_20HOTEL_CREDIT_LIMITED_USD_UNKNOWN || "").trim() === "1" ||
    String(env.CI_20HOTEL_CREDIT_LIMITED_DEVELOPMENT || "").trim() === "1";
  return Boolean(flag || envOn);
}

export function resolvePilotCreditHardCap({ env = process.env, creditLimitedUsdUnknown = false } = {}) {
  const raw = env.CONTEXT_DEV_PILOT_CREDIT_HARD_CAP ?? env.CI_20HOTEL_CONTEXT_DEV_CREDIT_HARD_CAP;
  if (raw != null && String(raw).trim() !== "") {
    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) return n;
  }
  if (creditLimitedUsdUnknown) return CONTEXT_DEV_PILOT_CREDIT_HARD_CAP_DEFAULT;
  return null;
}

/**
 * Founder-authorized live resume after expired deadlines / unresolved scrape billing.
 * Does not invent a proven scrape ceiling — allows documented reservation with overrun/unknown liability retained.
 */
export function resolveFounderAuthorizeLiveResume({ argv = [], env = process.env } = {}) {
  const flag =
    argv.includes("--founder-authorize-live-resume") ||
    argv.some((a) => a.startsWith("--founder-authorize-live-resume="));
  const envOn =
    String(env.CONTEXT_DEV_PILOT_FOUNDER_AUTHORIZE_LIVE_RESUME || "").trim() === "1" ||
    String(env.CI_20HOTEL_FOUNDER_AUTHORIZE_LIVE_RESUME || "").trim() === "1";
  return Boolean(flag || envOn);
}

export function isAuthorizedFiveHotelSelection(hotelIds = []) {
  const set = new Set((hotelIds || []).map(String));
  if (set.size !== CI_20HOTEL_AUTHORIZED_FIVE_IDS.length) return false;
  return CI_20HOTEL_AUTHORIZED_FIVE_IDS.every((id) => set.has(id));
}

const FROZEN_COUNT_DEFAULT = 20;

/**
 * @param {object} opts
 * @param {string[]} opts.argv
 * @param {object} [opts.env]
 * @param {Array<{hotel_id:string,stratum:string}>} opts.frozenSelection
 * @param {number} opts.contextCap
 * @param {number} opts.serpCap
 * @param {string} opts.caseStoreRoot
 */
export function resolvePilotSelection({ argv = [], frozenSelection = [], contextCap = 100 } = {}) {
  function argValue(flag) {
    const eq = argv.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const idx = argv.indexOf(flag);
    if (idx >= 0 && argv[idx + 1] != null && !String(argv[idx + 1]).startsWith("--")) {
      return argv[idx + 1];
    }
    return null;
  }

  const subsetRaw = argValue("--subset");
  // Use ?? so --hotel-ids= (empty) is not discarded as falsy
  const hotelIdsRaw = argValue("--hotel-ids") ?? argValue("--ids");
  let selection = frozenSelection.slice();
  let selectionMode = `full_frozen_${frozenSelection.length || FROZEN_COUNT_DEFAULT}`;
  const errors = [];

  if (hotelIdsRaw != null) {
    const wanted = String(hotelIdsRaw)
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);
    if (wanted.length === 0) {
      errors.push({ code: "EMPTY_HOTEL_IDS", message: "--hotel-ids / --ids must be nonempty" });
    } else {
      selection = frozenSelection.filter((h) => wanted.includes(h.hotel_id));
      const missing = wanted.filter((id) => !selection.some((h) => h.hotel_id === id));
      if (missing.length) {
        errors.push({
          code: "UNKNOWN_HOTEL_IDS",
          message: `Unknown or non-frozen hotel ids: ${missing.join(",")}`,
        });
      }
      selectionMode = `explicit_hotel_ids_${selection.length}`;
    }
  } else if (subsetRaw != null) {
    const asNum = Number(subsetRaw);
    // Integer only — reject 5.5, "3.0" via Number.isInteger after ensuring no float token
    const looksFloat = /[.]/.test(String(subsetRaw));
    if (looksFloat || !Number.isInteger(asNum) || asNum < 1 || asNum > frozenSelection.length) {
      errors.push({
        code: "INVALID_SUBSET",
        message: `Invalid --subset=${subsetRaw}; expected integer 1..${frozenSelection.length}`,
      });
    } else {
      const byStratum = new Map();
      for (const h of frozenSelection) {
        if (!byStratum.has(h.stratum)) byStratum.set(h.stratum, []);
        byStratum.get(h.stratum).push(h);
      }
      const strata = [...byStratum.keys()];
      selection = [];
      let i = 0;
      while (selection.length < asNum) {
        const bucket = byStratum.get(strata[i % strata.length]);
        const next = bucket && bucket.shift();
        if (next) selection.push(next);
        i += 1;
        if (strata.every((st) => (byStratum.get(st) || []).length === 0) && selection.length < asNum) break;
      }
      selectionMode = `subset_${asNum}_stratified_round_robin`;
    }
  }

  if (!errors.length && selection.length === 0) {
    errors.push({ code: "EMPTY_SELECTION", message: "Hotel selection resolved empty" });
  }

  return { ok: errors.length === 0, errors, selection, selectionMode, contextCap };
}

/**
 * Build the single pilot policy used by dry-run and live.
 */
export function buildCiDevPilotExecutionPolicy({
  argv = [],
  env = process.env,
  frozenSelection = [],
  contextCap = 100,
  serpCap = 0,
  emailCap = 30,
  mobileCap = 10,
  searchReqCap = 20,
  caseStoreRoot,
  confirmLive = false,
  dryRun = false,
} = {}) {
  const sel = resolvePilotSelection({ argv, frozenSelection, contextCap });
  const hotelCount = sel.selection.length || 1;

  const creditLimitedUsdUnknown = resolveCreditLimitedUsdUnknownMode({ argv, env });
  const pilotHardCap = resolvePilotCreditHardCap({ env, creditLimitedUsdUnknown });
  const authorizedFive = isAuthorizedFiveHotelSelection(sel.selection.map((h) => h.hotel_id));
  const founderAuthorizeLiveResume = resolveFounderAuthorizeLiveResume({ argv, env });

  // Shared-pool mode for authorized five-hotel credit-limited DEVELOPMENT pilot:
  // CONTEXT_DEV_PILOT_CREDIT_HARD_CAP controls spend; former per-hotel 8 / batch 100 must not override.
  const sharedPoolMode =
    creditLimitedUsdUnknown &&
    authorizedFive &&
    Number.isFinite(pilotHardCap) &&
    pilotHardCap > 0;

  const effectiveContextCap = sharedPoolMode ? pilotHardCap : contextCap;
  // Shared pool: per-case max equals remaining-pool controller (same as batch hard cap).
  // Non-shared: retain historical fixed per-case slice of 8.
  const perCaseContextDevMax = sharedPoolMode
    ? effectiveContextCap
    : Math.min(8, effectiveContextCap);
  const maxElapsedMsPerCase = (() => {
    const raw = env.CI_20HOTEL_MAX_ELAPSED_MS_PER_CASE ?? env.CONTEXT_DEV_PILOT_MAX_ELAPSED_MS_PER_CASE;
    const n = raw != null && raw !== "" ? Number(raw) : NaN;
    if (Number.isFinite(n) && n > 0) return n;
    // Founder-authorized live resume: allow deeper ownership/contact research per hotel.
    if (founderAuthorizeLiveResume) return 15 * 60 * 1000;
    return 180_000;
  })();
  const maxElapsedMsBatch = (() => {
    const raw = env.CI_20HOTEL_MAX_ELAPSED_MS_BATCH ?? env.CONTEXT_DEV_PILOT_MAX_ELAPSED_MS_BATCH;
    const n = raw != null && raw !== "" ? Number(raw) : NaN;
    if (Number.isFinite(n) && n > 0) return n;
    if (founderAuthorizeLiveResume) return 3 * 60 * 60 * 1000;
    return 30 * 60 * 1000;
  })();

  const usdPerCreditRaw = env.CONTEXT_DEV_USD_PER_CREDIT ?? env.CI_20HOTEL_CONTEXT_DEV_USD_PER_CREDIT;
  const usdCapRaw = env.CI_20HOTEL_CONTEXT_DEV_USD_MAX ?? env.CONTEXT_DEV_USD_MAX;
  const usdPerCreditNum =
    usdPerCreditRaw != null && usdPerCreditRaw !== "" ? Number(usdPerCreditRaw) : null;
  const usdCapNum = usdCapRaw != null && usdCapRaw !== "" ? Number(usdCapRaw) : null;
  const pricingKnown =
    Number.isFinite(usdPerCreditNum) &&
    usdPerCreditNum > 0 &&
    Number.isFinite(usdCapNum) &&
    usdCapNum > 0;

  // Never coerce missing USD to 0 — UNKNOWN when absent / unconfirmed.
  const usdPerCredit = pricingKnown ? usdPerCreditNum : "UNKNOWN";
  const usdCap = pricingKnown ? usdCapNum : "UNKNOWN";

  const nodeEnv = String(env.NODE_ENV || "").toLowerCase();
  const dealalityEnv = String(env.DEALALITY_ENV || env.DEALALITY_RUNTIME_ENV || "").toLowerCase();
  const isProduction =
    nodeEnv === "production" ||
    dealalityEnv === "production" ||
    String(env.CI_20HOTEL_FORCE_PRODUCTION || "") === "1";

  const configuredBatchCap = sharedPoolMode ? pilotHardCap : contextCap;
  const selectionCreditCeiling = sharedPoolMode
    ? pilotHardCap
    : hotelCount * perCaseContextDevMax;
  const effectiveBatchCreditLimit = sharedPoolMode
    ? pilotHardCap
    : Math.min(configuredBatchCap, selectionCreditCeiling);

  const providers = {
    context_dev: {
      enabled: true,
      total_cap_credits: effectiveContextCap,
      per_case_cap_credits: perCaseContextDevMax,
      shared_pool: sharedPoolMode,
      hard_cap: sharedPoolMode ? pilotHardCap : null,
      requires_pricing: !creditLimitedUsdUnknown,
      credit_limited_usd_unknown: creditLimitedUsdUnknown,
    },
    serpapi: { enabled: false, cap: serpCap },
    surfe: {
      enabled: false,
      email_cap: emailCap,
      mobile_cap: mobileCap,
      search_req_cap: searchReqCap,
      reason: "paid_enrichment_disabled_initial_pilot",
      block_balance_checks: true,
      block_search: true,
      block_enrichment: true,
      block_polling: true,
    },
    fullenrich: { enabled: false, cap: 0 },
    webhound: { enabled: false, cap: 0 },
    apify: { enabled: false, cap: 0 },
    model_reader: { enabled: false, usd_max: 0 },
    model_follow_up_planner: { enabled: false },
  };

  const workflowBudgets = {
    // Shared pool: pass hard cap; runtime remaining is enforced by ledger + journaler.
    context_dev_max: perCaseContextDevMax,
    serpapi_max: serpCap,
    enrichment_max: 0,
    enable_contact_enrichment: false,
    surfe_max: 0,
    fullenrich_max: 0,
    pdl_max: 0,
    parallel_max: 0,
    webhound_max: 0,
    model_reader_usd_max: 0,
    model_usd_max: 0,
    apply_structured_reader: false,
    apply_model_follow_up_planner: false,
    disable_network: false,
  };

  const blockers = [];
  if (!sel.ok) blockers.push(...sel.errors.map((e) => e.code));
  if (isProduction) blockers.push("PRODUCTION_EXECUTION_FORBIDDEN");
  // Pricing required unless explicit credit-limited DEVELOPMENT mode authorizes UNKNOWN USD.
  if (!pricingKnown && !creditLimitedUsdUnknown) {
    blockers.push("MISSING_CONTEXT_DEV_PRICING_OR_USD_CAP");
  }
  if (!confirmLive && !dryRun) blockers.push("LIVE_OPT_IN_REQUIRED");

  const supersededLimits = sharedPoolMode
    ? {
        prior_per_hotel_cap: 8,
        prior_batch_cap: 100,
        superseded_by: "CONTEXT_DEV_PILOT_CREDIT_HARD_CAP",
        authorized_hard_cap: pilotHardCap,
        note: "Former 8-credit per-hotel and 100-credit batch settings do not override the authorized 500-credit shared pool for this pilot.",
      }
    : null;

  const policy = {
    version: CI_DEV_PILOT_POLICY_VERSION,
    // Keep digest anchor at legacy per-case fingerprint so raising the hard cap
    // does not orphan durable ledger pilot_763f42999f6bf0bd for these five hotels.
    pilot_identity_digest_anchor: "8",
    ok: blockers.length === 0 || dryRun,
    dry_run: dryRun,
    confirm_live: confirmLive,
    selection_ok: sel.ok,
    selection_errors: sel.errors,
    selection_mode: sel.selectionMode,
    hotels: sel.selection,
    hotel_ids: sel.selection.map((h) => h.hotel_id),
    hotel_count: sel.selection.length,
    case_store_root: caseStoreRoot,
    staging: true,
    canonical_promotion: false,
    customer_publication: "BLOCKED",
    development_only: true,
    production_forbidden: true,
    is_production_env: isProduction,
    accounting_mode: creditLimitedUsdUnknown
      ? CREDIT_LIMITED_USD_UNKNOWN_MODE
      : pricingKnown
        ? "usd_and_credit_capped"
        : "pricing_required",
    credit_limited_usd_unknown: creditLimitedUsdUnknown,
    shared_pool_mode: sharedPoolMode,
    founder_authorize_live_resume: founderAuthorizeLiveResume,
    // Unresolved scrape credits_consumed=10 vs documented 1 — refuse scrape under
    // credit-limited pilot rather than claiming a proven hard ceiling, unless the
    // founder explicitly authorizes live resume (documented reservation + overrun/unknown liability retained).
    block_unproven_scrape_bound:
      creditLimitedUsdUnknown === true && founderAuthorizeLiveResume !== true,
    authorized_five_hotel_pilot: authorizedFive,
    providers,
    limits: {
      total: {
        context_dev: effectiveContextCap,
        context_dev_configured_batch_cap: configuredBatchCap,
        context_dev_selection_ceiling: selectionCreditCeiling,
        context_dev_effective_batch_limit: effectiveBatchCreditLimit,
        context_dev_hard_cap: sharedPoolMode ? pilotHardCap : null,
        serpapi: serpCap,
        surfe_email: 0,
        surfe_mobile: 0,
        surfe_search_req: 0,
        fullenrich: 0,
        webhound: 0,
        apify: 0,
        model_usd: 0,
        max_elapsed_ms_batch: maxElapsedMsBatch,
      },
      per_case: {
        context_dev_max: perCaseContextDevMax,
        serpapi_max: serpCap,
        enrichment_max: 0,
        enable_contact_enrichment: false,
        model_reader_usd_max: 0,
        model_usd_max: 0,
        max_elapsed_ms_per_case: maxElapsedMsPerCase,
        allocation_rule: sharedPoolMode
          ? "shared_pilot_hard_cap_pool"
          : "fixed_per_case_cap_not_batch_split",
      },
    },
    workflow_budgets: workflowBudgets,
    cost_accounting: {
      context_dev: {
        unit: "credits",
        usd_per_credit: usdPerCredit,
        usd_cap: usdCap,
        usd_rate_known: pricingKnown,
        usd_status: pricingKnown ? "KNOWN" : "UNKNOWN",
        credit_limited_mode: creditLimitedUsdUnknown,
        hard_cap: sharedPoolMode ? pilotHardCap : null,
        required_env: creditLimitedUsdUnknown
          ? []
          : ["CONTEXT_DEV_USD_PER_CREDIT", "CI_20HOTEL_CONTEXT_DEV_USD_MAX"],
        note: creditLimitedUsdUnknown
          ? sharedPoolMode
            ? `Explicit DEVELOPMENT credit-limited shared pool: CONTEXT_DEV_PILOT_CREDIT_HARD_CAP=${pilotHardCap}; USD pricing UNKNOWN (not zero).`
            : "Explicit DEVELOPMENT credit-limited mode: hard credit caps active; USD pricing UNKNOWN (not zero)."
          : pricingKnown
            ? "USD rate and cap confirmed."
            : "Live blocked until CONTEXT_DEV_USD_PER_CREDIT and CI_20HOTEL_CONTEXT_DEV_USD_MAX are set, or --credit-limited-development is opted in.",
      },
      surfe: { enabled: false, usd_rate_known: false },
    },
    superseded_limits: supersededLimits,
    blockers: [...new Set(blockers)],
    live_executable: !dryRun && blockers.length === 0,
  };

  return policy;
}

/**
 * Fail closed before any external call on the live path.
 */
export function assertPilotLiveExecutable(policy) {
  if (!policy) {
    return { ok: false, error: "POLICY_MISSING", message: "Pilot policy required" };
  }
  if (policy.is_production_env) {
    return {
      ok: false,
      error: "PRODUCTION_EXECUTION_FORBIDDEN",
      message: "ci-20hotel DEVELOPMENT pilot refuses production environment",
    };
  }
  if (!policy.confirm_live) {
    return {
      ok: false,
      error: "LIVE_OPT_IN_REQUIRED",
      message: "Refusing live run without --confirm-live-execution",
    };
  }
  if (!policy.selection_ok || !policy.hotel_ids?.length) {
    return {
      ok: false,
      error: "INVALID_SELECTION",
      message: (policy.selection_errors || []).map((e) => e.message).join("; ") || "invalid selection",
    };
  }
  const creditLimited = policy.credit_limited_usd_unknown === true;
  const usdKnown = policy.cost_accounting?.context_dev?.usd_rate_known === true;
  if (!usdKnown && !creditLimited) {
    return {
      ok: false,
      error: "MISSING_CONTEXT_DEV_PRICING_OR_USD_CAP",
      message:
        "Set CONTEXT_DEV_USD_PER_CREDIT and CI_20HOTEL_CONTEXT_DEV_USD_MAX (>0), or pass --credit-limited-development for explicit DEVELOPMENT credit-limited mode (USD=UNKNOWN)",
    };
  }
  if (creditLimited && policy.cost_accounting?.context_dev?.usd_status !== "UNKNOWN" && !usdKnown) {
    // Defensive: credit-limited mode must not silently treat missing USD as zero
    if (policy.cost_accounting?.context_dev) {
      policy.cost_accounting.context_dev.usd_per_credit = "UNKNOWN";
      policy.cost_accounting.context_dev.usd_cap = "UNKNOWN";
      policy.cost_accounting.context_dev.usd_status = "UNKNOWN";
    }
  }
  if (!(Number(policy.limits?.per_case?.context_dev_max) > 0) || !(Number(policy.limits?.total?.context_dev) > 0)) {
    return {
      ok: false,
      error: "MISSING_CREDIT_CAPS",
      message: "Credit-limited DEVELOPMENT pilot requires positive per-case and batch Context.dev credit caps",
    };
  }
  if (policy.providers?.surfe?.enabled) {
    return {
      ok: false,
      error: "SURFE_MUST_REMAIN_DISABLED",
      message: "Initial pilot policy requires Surfe disabled",
    };
  }
  return { ok: true, credit_limited_usd_unknown: creditLimited, usd_status: usdKnown ? "KNOWN" : "UNKNOWN" };
}

/**
 * Cumulative budget + deadline tracker for first pass, follow-up, and resume.
 */
export function createPilotSpendTracker(policy, { nowFn = () => Date.now() } = {}) {
  const batchStarted = nowFn();
  const batchLimit = Number(policy.limits.total.max_elapsed_ms_batch);
  const caseLimit = Number(policy.limits.per_case.max_elapsed_ms_per_case);
  const totalContext = Number(policy.limits.total.context_dev);
  const perCaseContext = Number(policy.limits.per_case.context_dev_max);
  const usdPerCredit = Number(policy.cost_accounting.context_dev.usd_per_credit);
  const usdCap = Number(policy.cost_accounting.context_dev.usd_cap);

  const state = {
    batch_started_ms: batchStarted,
    context_dev_spent: 0,
    usd_spent: 0,
    per_case: new Map(), // hotel_id -> { spent, started_ms, cancelled }
    cancelled_batch: false,
    cancel_reason: null,
  };

  function caseState(hotelId) {
    if (!state.per_case.has(hotelId)) {
      state.per_case.set(hotelId, { spent: 0, started_ms: nowFn(), cancelled: false, cancel_reason: null });
    }
    return state.per_case.get(hotelId);
  }

  function batchElapsed() {
    return nowFn() - state.batch_started_ms;
  }

  function caseElapsed(hotelId) {
    return nowFn() - caseState(hotelId).started_ms;
  }

  function remainingBatchContext() {
    return Math.max(0, totalContext - state.context_dev_spent);
  }

  function remainingCaseContext(hotelId) {
    return Math.max(0, perCaseContext - caseState(hotelId).spent);
  }

  function remainingUsd() {
    if (!Number.isFinite(usdCap) || !Number.isFinite(usdPerCredit)) return 0;
    return Math.max(0, usdCap - state.usd_spent);
  }

  /**
   * Gate a dispatch. Returns allowance credits for this call (0 = do not dispatch).
   */
  function allowDispatch(hotelId, { requested = perCaseContext } = {}) {
    if (state.cancelled_batch) {
      return { ok: false, allowance: 0, reason: state.cancel_reason || "BATCH_CANCELLED" };
    }
    const cs = caseState(hotelId);
    if (cs.cancelled) {
      return { ok: false, allowance: 0, reason: cs.cancel_reason || "CASE_CANCELLED" };
    }
    if (batchElapsed() >= batchLimit) {
      state.cancelled_batch = true;
      state.cancel_reason = "BATCH_DEADLINE_EXCEEDED";
      return { ok: false, allowance: 0, reason: "BATCH_DEADLINE_EXCEEDED" };
    }
    if (caseElapsed(hotelId) >= caseLimit) {
      cs.cancelled = true;
      cs.cancel_reason = "CASE_DEADLINE_EXCEEDED";
      return { ok: false, allowance: 0, reason: "CASE_DEADLINE_EXCEEDED" };
    }
    if (remainingBatchContext() <= 0) {
      return { ok: false, allowance: 0, reason: "BATCH_CONTEXT_EXHAUSTED" };
    }
    if (remainingCaseContext(hotelId) <= 0) {
      return { ok: false, allowance: 0, reason: "CASE_CONTEXT_EXHAUSTED" };
    }
    if (remainingUsd() <= 0) {
      state.cancelled_batch = true;
      state.cancel_reason = "USD_CAP_EXHAUSTED";
      return { ok: false, allowance: 0, reason: "USD_CAP_EXHAUSTED" };
    }
    const byUsd = Math.floor(remainingUsd() / usdPerCredit);
    const allowance = Math.max(
      0,
      Math.min(Number(requested) || 0, remainingCaseContext(hotelId), remainingBatchContext(), byUsd)
    );
    if (allowance <= 0) {
      return { ok: false, allowance: 0, reason: "NO_ALLOWANCE" };
    }
    return { ok: true, allowance, reason: null };
  }

  function recordSpend(hotelId, credits) {
    const used = Math.max(0, Number(credits) || 0);
    const cs = caseState(hotelId);
    cs.spent += used;
    state.context_dev_spent += used;
    state.usd_spent += used * usdPerCredit;
    return { case_spent: cs.spent, batch_spent: state.context_dev_spent, usd_spent: state.usd_spent };
  }

  function snapshot() {
    return {
      context_dev_spent: state.context_dev_spent,
      usd_spent: state.usd_spent,
      batch_elapsed_ms: batchElapsed(),
      cancelled_batch: state.cancelled_batch,
      cancel_reason: state.cancel_reason,
      per_case: Object.fromEntries(state.per_case),
      limits: policy.limits,
    };
  }

  return {
    allowDispatch,
    recordSpend,
    remainingBatchContext,
    remainingCaseContext,
    remainingUsd,
    snapshot,
    perCaseContextDevMax: perCaseContext,
  };
}

/**
 * Dry-run plan projection — must mirror live limits exactly.
 */
export function pilotPolicyToDryRunPlan(policy, { runner, objective, outputs } = {}) {
  return {
    mode: "DRY_RUN",
    policy_version: policy.version,
    runner,
    durable_entry: "lib/hotel-intelligence/contact-intelligence/full-research-workflow.js",
    selection_mode: policy.selection_mode,
    hotel_count: policy.hotel_count,
    hotels: policy.hotels.map((h) => ({
      hotel_id: h.hotel_id,
      stratum: h.stratum,
      proposed_operations: [
        "assessHotelPhysicalIdentity (HPC load)",
        "ownership/contact research via runFullResearchWorkflow",
        "context_dev search/scrape within shared pilot credit pool (live only)",
        "persist staged research case (no canonical promotion)",
      ],
      per_case_context_dev_max: policy.limits.per_case.context_dev_max,
    })),
    hotel_ids: policy.hotel_ids,
    objective,
    development_case_storage: policy.case_store_root,
    staging: true,
    canonical_promotion: false,
    customer_publication: "BLOCKED",
    providers: {
      context_dev: {
        enabled_when_live: policy.providers.context_dev.enabled,
        total_cap_credits: policy.limits.total.context_dev,
        per_case_cap_credits: policy.limits.per_case.context_dev_max,
        shared_pool: policy.shared_pool_mode === true,
        hard_cap: policy.limits.total.context_dev_hard_cap,
      },
      serpapi: { enabled_when_live: false, cap: policy.limits.total.serpapi },
      surfe: {
        enabled_when_live: false,
        reason: policy.providers.surfe.reason,
        block_balance_checks: true,
        block_search: true,
        block_enrichment: true,
        block_polling: true,
      },
      fullenrich: { enabled_when_live: false },
      webhound: { enabled_when_live: false },
      apify: { enabled_when_live: false },
      model_reader: { enabled_when_live: false },
      model_follow_up_planner: { enabled_when_live: false },
    },
    limits: policy.limits,
    workflow_budgets: policy.workflow_budgets,
    cost_accounting: policy.cost_accounting,
    accounting_mode: policy.accounting_mode,
    credit_limited_usd_unknown: policy.credit_limited_usd_unknown === true,
    shared_pool_mode: policy.shared_pool_mode === true,
    superseded_limits: policy.superseded_limits || null,
    credit_limits_display: {
      authorized_hard_cap: policy.limits.total.context_dev_hard_cap,
      configured_batch_cap: policy.limits.total.context_dev_configured_batch_cap,
      selection_ceiling: policy.limits.total.context_dev_selection_ceiling,
      effective_batch_limit: policy.limits.total.context_dev_effective_batch_limit,
      per_case_max: policy.limits.per_case.context_dev_max,
      hotel_count: policy.hotel_count,
      allocation_rule: policy.limits.per_case.allocation_rule,
      note: "Prior settled spend and outstanding reservations reduce remaining allowance at runtime. Account-wide usage is NOT pilot consumption.",
    },
    remaining_active_non_credit_limits: {
      max_elapsed_ms_per_case: policy.limits.per_case.max_elapsed_ms_per_case,
      max_elapsed_ms_batch: policy.limits.total.max_elapsed_ms_batch,
      serpapi: 0,
      surfe: "disabled",
      model: "disabled",
    },
    live_blockers_if_executed_now: policy.blockers.filter((b) => b !== "LIVE_OPT_IN_REQUIRED"),
    live_opt_in_required: true,
    production_forbidden: true,
    outputs,
    note: policy.shared_pool_mode
      ? `Dry-run and live share buildCiDevPilotExecutionPolicy. Shared pool CONTEXT_DEV_PILOT_CREDIT_HARD_CAP=${policy.limits.total.context_dev_hard_cap}; USD=UNKNOWN (not zero). Former 8/hotel and 100/batch superseded for this pilot. Surfe disabled.`
      : policy.credit_limited_usd_unknown
        ? "Dry-run and live share buildCiDevPilotExecutionPolicy. Credit-limited DEVELOPMENT mode: hard credit caps; USD=UNKNOWN (not zero). Surfe disabled."
        : "Dry-run and live share buildCiDevPilotExecutionPolicy. Surfe fully disabled. Missing pricing blocks live unless --credit-limited-development. Per-case cap is fixed (not batch_split).",
  };
}
