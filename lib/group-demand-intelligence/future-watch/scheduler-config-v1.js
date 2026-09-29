/**
 * Scheduler config from env — production-safe defaults.
 */

import { SCHEDULER_DEFAULTS } from "./constants.js";

function num(envName, fallback) {
  const v = process.env[envName];
  if (v == null || v === "") return fallback;
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

function bool(envName, fallback = false) {
  const v = String(process.env[envName] || "").trim().toLowerCase();
  if (!v) return fallback;
  if (["1", "true", "yes", "on"].includes(v)) return true;
  if (["0", "false", "no", "off"].includes(v)) return false;
  return fallback;
}

/**
 * Resolve runtime scheduler config.
 */
export function resolveSchedulerConfig(overrides = {}) {
  return {
    maxWatchesPerBatch: num(
      "GDI_WATCH_BATCH_LIMIT",
      overrides.maxWatchesPerBatch ?? SCHEDULER_DEFAULTS.maxWatchesPerBatch
    ),
    maxEstimatedCostUsd: num(
      "GDI_WATCH_BATCH_BUDGET_USD",
      overrides.maxEstimatedCostUsd ?? SCHEDULER_DEFAULTS.maxEstimatedCostUsd
    ),
    maxQueriesPerAction: num(
      "GDI_WATCH_MAX_QUERIES",
      overrides.maxQueriesPerAction ?? SCHEDULER_DEFAULTS.maxQueriesPerAction
    ),
    maxFetchesPerAction: num(
      "GDI_WATCH_MAX_FETCHES",
      overrides.maxFetchesPerAction ?? SCHEDULER_DEFAULTS.maxFetchesPerAction
    ),
    maxJevActionsPerWatch: 1, // hard cap for V1
    jevEnabled: bool("GDI_WATCH_JEV_ENABLED", overrides.jevEnabled !== false),
    maxProviderErrors: num(
      "GDI_WATCH_MAX_PROVIDER_ERRORS",
      overrides.maxProviderErrors ?? SCHEDULER_DEFAULTS.maxProviderErrors
    ),
    maxRetries: num(
      "GDI_WATCH_MAX_RETRIES",
      overrides.maxRetries ?? SCHEDULER_DEFAULTS.maxRetries
    ),
    retryBackoffDays:
      overrides.retryBackoffDays || SCHEDULER_DEFAULTS.retryBackoffDays,
    staleClaimMs: num(
      "GDI_WATCH_STALE_CLAIM_MS",
      overrides.staleClaimMs ?? SCHEDULER_DEFAULTS.staleClaimMs
    ),
    allowForceDue: bool("GDI_WATCH_ALLOW_FORCE_DUE", false),
  };
}

/**
 * Force-due is only allowed when GDI_WATCH_ALLOW_FORCE_DUE=1 and NODE_ENV !== production.
 * Impossible to enable accidentally in production scheduled execution.
 */
export function isForceDueAllowed(config = {}) {
  const explicit =
    Boolean(config.allowForceDue) ||
    ["1", "true", "yes"].includes(
      String(process.env.GDI_WATCH_ALLOW_FORCE_DUE || "").toLowerCase()
    );
  if (!explicit) return false;
  if (String(process.env.NODE_ENV || "").toLowerCase() === "production") return false;
  return true;
}
