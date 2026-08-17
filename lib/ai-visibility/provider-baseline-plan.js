/**
 * Build governed 84-prompt provider baseline execution plan (Phase 3B.3).
 */

import { buildWave1ShowcaseDryRunPlan, WAVE1_EXECUTION_ORDER } from "./wave1-showcase-plan.js";
import { attachProviderFingerprints } from "./provider-validation-plan.js";
import { PROVIDER_BASELINE_SERIES } from "./provider-baseline-state.js";

export const PROVIDER_BASELINE_PLAN_VERSION = "ai_visibility_provider_baseline_plan_v1";

/**
 * @param {string} provider
 */
export function buildProviderBaselineExecutionPlan(provider) {
  const id = String(provider || "").trim().toLowerCase();
  const plan = buildWave1ShowcaseDryRunPlan();
  if (!plan.ok) {
    return { ok: false, errors: plan.errors, provider: id, EXECUTIONS: [] };
  }

  const executions = attachProviderFingerprints(plan.EXECUTIONS, id);
  const bySlot = Object.fromEntries(WAVE1_EXECUTION_ORDER.map((s) => [s.key, 0]));
  for (const exec of executions) {
    bySlot[exec.slot] = (bySlot[exec.slot] || 0) + 1;
  }

  const slotErrors = WAVE1_EXECUTION_ORDER.filter((s) => bySlot[s.key] !== 12).map(
    (s) => `slot_count_${s.key}_${bySlot[s.key]}`
  );

  return {
    ok: slotErrors.length === 0 && executions.length === 84,
    errors: slotErrors,
    provider: id,
    baselineSeriesId: PROVIDER_BASELINE_SERIES[id] || null,
    EXECUTIONS: executions,
    EXECUTION_ORDER: WAVE1_EXECUTION_ORDER.map((s) => s.key),
    SLOT_COUNTS: bySlot,
    planVersion: PROVIDER_BASELINE_PLAN_VERSION,
    PLANNED: executions.length,
  };
}
