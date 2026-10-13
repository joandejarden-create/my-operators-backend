/**
 * Native iterative research bounds — Iteration 2 (external tool spend guards).
 */

export const NATIVE_LOOP_BOUNDS_VERSION = "native-loop-bounds-v2";

export const DEFAULT_NATIVE_LOOP_BOUNDS = Object.freeze({
  max_iterations: 12,
  max_actions: 18,
  max_provider_cost_usd: 0,
  max_total_cost_usd: 0,
  max_external_cost_usd: 0,
  max_parallel_calls: 1,
  max_search_calls: 4,
  max_page_fetches: 3,
  max_elapsed_ms: 120_000,
  min_confidence_to_stop: "HIGH",
  verification_required_for_high_confidence: true,
  allow_external: true,
  /** auto | live | fixture | off */
  external_mode: "auto",
  confirm_spend: false,
  force_live: false,
});

export function resolveNativeLoopBounds(input = {}) {
  const envMode = process.env.HI_NATIVE_EXTERNAL_MODE;
  return {
    ...DEFAULT_NATIVE_LOOP_BOUNDS,
    ...(input.bounds || {}),
    max_iterations: Number(input.bounds?.max_iterations ?? input.max_iterations ?? 12),
    max_actions: Number(input.bounds?.max_actions ?? input.max_actions ?? 18),
    max_elapsed_ms: Number(input.bounds?.max_elapsed_ms ?? input.max_elapsed_ms ?? 120_000),
    max_parallel_calls: Number(input.bounds?.max_parallel_calls ?? 1),
    max_search_calls: Number(input.bounds?.max_search_calls ?? 4),
    max_page_fetches: Number(input.bounds?.max_page_fetches ?? 3),
    max_external_cost_usd: Number(
      input.bounds?.max_external_cost_usd ?? input.max_external_cost_usd ?? 0
    ),
    max_total_cost_usd: Number(input.bounds?.max_total_cost_usd ?? input.max_total_cost_usd ?? 0),
    allow_external: input.bounds?.allow_external !== false,
    external_mode: input.bounds?.external_mode || envMode || "auto",
    confirm_spend: input.bounds?.confirm_spend === true,
    force_live: input.bounds?.force_live === true,
  };
}

export const TERMINATION_REASONS = Object.freeze({
  VERIFIED: "VERIFIED",
  SUFFICIENT_EVIDENCE: "SUFFICIENT_EVIDENCE",
  NO_MORE_HIGH_VALUE_ACTIONS: "NO_MORE_HIGH_VALUE_ACTIONS",
  BUDGET_REACHED: "BUDGET_REACHED",
  MAX_ITERATIONS: "MAX_ITERATIONS",
  MAX_ACTIONS: "MAX_ACTIONS",
  TIME_LIMIT: "TIME_LIMIT",
  UNRESOLVED_CONTRADICTION: "UNRESOLVED_CONTRADICTION",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  IDENTITY_UNRESOLVED: "IDENTITY_UNRESOLVED",
  PROVIDER_FAILURE: "PROVIDER_FAILURE",
});
