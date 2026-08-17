/**
 * Wave-1 Global EN activation gate (Phase 3A.11).
 * Pipeline health only — commercial recommendation content cannot fail the gate.
 */

import { projectWaveCostFromSample, WAVE1_HARD_CAP_USD } from "./wave1-cost.js";

/**
 * @param {{
 *   slotKey?: string,
 *   planned?: number,
 *   succeeded?: number,
 *   failed?: number,
 *   retries?: number,
 *   timeouts?: number,
 *   providerErrors?: number,
 *   authErrors?: number,
 *   parseFailures?: number,
 *   storageFailures?: number,
 *   identityMissing?: number,
 *   resolverMalfunctions?: number,
 *   classifierMalfunctions?: number,
 *   citationCorruptions?: number,
 *   slotCostUsd?: number,
 *   governanceViolations?: string[],
 * }} slotResult
 */
export function evaluateGlobalEnActivationGate(slotResult = {}) {
  const reasons = [];
  const notes = [];
  const planned = Number(slotResult.planned ?? 12);
  const succeeded = Number(slotResult.succeeded ?? 0);
  const failed = Number(slotResult.failed ?? 0);
  const retries = Number(slotResult.retries ?? 0);
  const timeouts = Number(slotResult.timeouts ?? 0);
  const providerErrors = Number(slotResult.providerErrors ?? 0);
  const authErrors = Number(slotResult.authErrors ?? 0);
  const parseFailures = Number(slotResult.parseFailures ?? 0);
  const storageFailures = Number(slotResult.storageFailures ?? 0);
  const identityMissing = Number(slotResult.identityMissing ?? 0);
  const resolverMalfunctions = Number(slotResult.resolverMalfunctions ?? 0);
  const classifierMalfunctions = Number(slotResult.classifierMalfunctions ?? 0);
  const citationCorruptions = Number(slotResult.citationCorruptions ?? 0);
  const slotCostUsd = Number(slotResult.slotCostUsd ?? 0);
  const governanceViolations = Array.isArray(slotResult.governanceViolations)
    ? slotResult.governanceViolations
    : [];

  // A. Execution — allow small bounded failure (≤2 of 12), not systemic collapse
  if (planned !== 12) reasons.push(`execution_planned_${planned}_expected_12`);
  if (authErrors > 0) reasons.push("execution_provider_auth_error");
  if (succeeded + failed < planned) reasons.push("execution_incomplete_slot");
  if (succeeded < 10) reasons.push(`execution_success_${succeeded}_below_floor_10`);
  if (timeouts >= 6) reasons.push(`execution_timeouts_systemic_${timeouts}`);
  if (providerErrors >= 6) reasons.push(`execution_provider_errors_systemic_${providerErrors}`);
  if (retries > planned) reasons.push(`execution_retries_exceed_policy_${retries}`);

  // B. Response parsing / identity
  if (parseFailures > 0) reasons.push(`parser_failures_${parseFailures}`);
  if (identityMissing > 0) reasons.push(`identity_missing_${identityMissing}`);

  // C–E. Material pipeline malfunctions only
  if (resolverMalfunctions > 0) reasons.push(`resolver_malfunction_${resolverMalfunctions}`);
  if (classifierMalfunctions > 0) reasons.push(`classifier_malfunction_${classifierMalfunctions}`);
  if (citationCorruptions > 0) reasons.push(`citation_corruption_${citationCorruptions}`);

  // F. Storage
  if (storageFailures > 0) reasons.push(`storage_failures_${storageFailures}`);

  // G. Cost projection
  const projection = projectWaveCostFromSample(slotCostUsd, Math.max(succeeded, 1));
  // Use succeeded sample size when available
  const costProj = projectWaveCostFromSample(slotCostUsd, succeeded > 0 ? succeeded : 0);
  if (costProj.likelyHardCapBreach) {
    reasons.push(
      `projected_cost_${costProj.projected84}_exceeds_hard_cap_${WAVE1_HARD_CAP_USD}`
    );
  }
  if (slotCostUsd > WAVE1_HARD_CAP_USD) {
    reasons.push(`slot_cost_already_exceeds_hard_cap_${slotCostUsd}`);
  }

  // H. Governance
  for (const v of governanceViolations) {
    reasons.push(`governance_${v}`);
  }

  notes.push(
    "Recommendation content desirability is not an activation criterion.",
    "Citation absence in a response is not an automatic failure (Citation Rate PARTIAL)."
  );

  const result = reasons.length === 0 ? "PASS" : "FAIL";
  return {
    GLOBAL_EN_ACTIVATION_GATE: result,
    RESULT: result,
    REASONS: reasons,
    NOTES: notes,
    COUNTS: {
      planned,
      succeeded,
      failed,
      retries,
      timeouts,
      providerErrors,
      authErrors,
      parseFailures,
      storageFailures,
      identityMissing,
      resolverMalfunctions,
      classifierMalfunctions,
      citationCorruptions,
    },
    COST: {
      GLOBAL_EN_ACTUAL: slotCostUsd,
      PROJECTION: costProj.projected84 != null ? costProj : projection,
      HARD_CAP: WAVE1_HARD_CAP_USD,
    },
    evaluatedAt: new Date().toISOString(),
  };
}
