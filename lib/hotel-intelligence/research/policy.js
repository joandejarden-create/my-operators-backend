/**
 * Packet 2.6C-R2 — paid / external research safety policy.
 * Default: HOTEL_INTELLIGENCE_EXTERNAL_RESEARCH_ENABLED=false
 * Pilot hard cap: $5.00 provider budget per run.
 */

export const PILOT_MAX_PROVIDER_BUDGET_USD = 5;
export const PILOT_DEFAULT_DAILY_EXTERNAL_BUDGET_USD = 25;
export const PILOT_MAX_CONCURRENT_WEBHOUND_RUNS = 1;

export function isExternalResearchEnabled(env = process.env) {
  const v = String(env.HOTEL_INTELLIGENCE_EXTERNAL_RESEARCH_ENABLED || "false")
    .trim()
    .toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}

/**
 * Founder/admin internal cost visibility.
 * Honors HOTEL_INTELLIGENCE_RESEARCH_INTERNAL_DEBUG and the intentional alias
 * HOTEL_INTELLIGENCE_SHOW_INTERNAL_RESEARCH_COSTS (same semantics).
 */
export function isFounderInternalDebug(env = process.env) {
  const keys = [
    "HOTEL_INTELLIGENCE_RESEARCH_INTERNAL_DEBUG",
    "HOTEL_INTELLIGENCE_SHOW_INTERNAL_RESEARCH_COSTS",
  ];
  return keys.some((k) => {
    const v = String(env[k] || "false")
      .trim()
      .toLowerCase();
    return v === "1" || v === "true" || v === "yes";
  });
}

export function getPilotMaxProviderBudgetUsd(env = process.env) {
  const raw = Number(env.HOTEL_INTELLIGENCE_MAX_PROVIDER_BUDGET_USD);
  if (Number.isFinite(raw) && raw > 0) {
    return Math.min(raw, PILOT_MAX_PROVIDER_BUDGET_USD);
  }
  return PILOT_MAX_PROVIDER_BUDGET_USD;
}

export function getDailyExternalBudgetUsd(env = process.env) {
  const raw = Number(env.HOTEL_INTELLIGENCE_DAILY_EXTERNAL_RESEARCH_BUDGET_USD);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return PILOT_DEFAULT_DAILY_EXTERNAL_BUDGET_USD;
}

export function getMaxConcurrentWebhoundRuns(env = process.env) {
  const raw = Number(env.HOTEL_INTELLIGENCE_MAX_CONCURRENT_WEBHOUND_RUNS);
  if (Number.isFinite(raw) && raw >= 1) return Math.floor(raw);
  return PILOT_MAX_CONCURRENT_WEBHOUND_RUNS;
}

/**
 * Clamp any requested budget to the pilot hard cap.
 */
export function clampProviderBudgetUsd(budgetUsd, env = process.env) {
  const max = getPilotMaxProviderBudgetUsd(env);
  const budget = Number(budgetUsd);
  if (!Number.isFinite(budget) || budget <= 0) {
    const err = new Error("provider_budget_required");
    err.code = "provider_budget_required";
    err.customer_safe = "Research could not be started.";
    throw err;
  }
  if (budget > max) {
    const err = new Error("provider_budget_exceeds_hard_cap");
    err.code = "provider_budget_exceeds_hard_cap";
    err.customer_safe = "Research budget exceeds the allowed maximum.";
    err.max_provider_budget_usd = max;
    throw err;
  }
  return Number(budget.toFixed(2));
}

export function requireProviderBudget(budgetUsd, env = process.env) {
  return clampProviderBudgetUsd(budgetUsd, env);
}

/**
 * Reject unbounded or unauthorized external runs.
 */
export function assertExternalResearchAllowed(input = {}) {
  const env = input.env || process.env;
  if (!isExternalResearchEnabled(env)) {
    const err = new Error("external_research_disabled");
    err.code = "external_research_disabled";
    err.customer_safe = "External research is disabled.";
    throw err;
  }
  if (input.authorized !== true) {
    const err = new Error("external_research_not_authorized");
    err.code = "external_research_not_authorized";
    err.customer_safe = "Research could not be started.";
    throw err;
  }
  if (input.confirm_spend !== true && input.explicit_user_action !== true) {
    const err = new Error("explicit_run_research_required");
    err.code = "explicit_run_research_required";
    err.customer_safe = "Confirm Run Research to start live investigation.";
    throw err;
  }
  const budget = clampProviderBudgetUsd(input.budget_usd, env);
  if (!input.hotel_id || !input.request_id || !input.template_id) {
    const err = new Error("external_run_missing_required_ids");
    err.code = "external_run_missing_required_ids";
    err.customer_safe = "Research could not be started.";
    throw err;
  }
  return { ok: true, budget_usd: budget, max_provider_budget_usd: getPilotMaxProviderBudgetUsd(env) };
}
