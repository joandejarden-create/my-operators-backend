/**
 * Brand AI Intelligence — Longitudinal Collection Foundation V1
 */

export * from "./grain.js";
export * from "./comparability.js";
export * from "./common-cohort.js";
export * from "./cohort-v1.js";
export * from "./cost-model.js";
export * from "./measurement-period.js";
export * from "./idempotency.js";
export * from "./current-vs-prior.js";
export * from "./baseline-audit.js";
export * from "./trend-display-rules.js";
export * from "./metric-classification.js";

import { auditBrandLongitudinalBaseline } from "./baseline-audit.js";
import { buildLongitudinalCostModel } from "./cost-model.js";
import { buildCohortExecutionMatrix, buildMonthlyExecutionMatrix } from "./cohort-v1.js";
import { trendClientCopy } from "./trend-display-rules.js";

/**
 * Full foundation readiness report (dry-run — no provider calls).
 */
export async function buildBrandLongitudinalFoundationReport(opts = {}) {
  const baseline = await auditBrandLongitudinalBaseline(opts);
  const cost = buildLongitudinalCostModel();
  const matrix = buildCohortExecutionMatrix();
  const monthlyMatrix = buildMonthlyExecutionMatrix();
  const trendUi = opts.trendUiAudit !== false ? (await import("./trend-display-rules.js")).auditExistingTrendUiContract() : null;

  const gates = {
    cohortDefined: matrix.promptCount >= 30 && matrix.promptCount <= 50,
    costGatePass: cost.costGatePass,
    storageReady: true,
    idempotencyReady: true,
    comparabilityReady: true,
    noSyntheticHistory: true,
    schedulerEnabled: false,
  };

  const readiness =
    gates.costGatePass && gates.cohortDefined
      ? "BRAND_LONGITUDINAL_FOUNDATION_READY"
      : "BRAND_LONGITUDINAL_FOUNDATION_REMEDIATION_REQUIRED";

  const initialWave = {
    STATUS: "APPROVAL_REQUIRED",
    PROMPTS: monthlyMatrix.promptCount,
    PROVIDER_CALLS: monthlyMatrix.callCount,
    PROJECTED_COST: cost.historicExpectedCostUsd,
    ACTUAL_COST: null,
    MEASUREMENT_PERIOD_ID: null,
    note: "BRAND_LONGITUDINAL_INITIAL_WAVE_APPROVAL_REQUIRED — no live execution in foundation phase",
  };

  if (cost.historicExpectedCostUsd > cost.maxInitialSpendUsd) {
    initialWave.STATUS = "BLOCKED_COST_CAP";
  }

  return {
    HOTEL_BRAND_AI_INTELLIGENCE_LONGITUDINAL_FOUNDATION_COMPLETE: true,
    baseline,
    cost,
    cohort: matrix,
    monthlyCohort: monthlyMatrix,
    trendUi,
    gates,
    readiness,
    initialWave,
    clientTrendCopy: trendClientCopy(baseline.REAL_DISTINCT_PERIODS),
    operatorReuse: [
      "grain.js — PROMPT × OPERATOR × PROVIDER × GEO × LANGUAGE × DATE",
      "comparability.js + common-cohort.js — period intersection denominators",
      "measurement-period.js + idempotency.js — period manifests + duplicate lock",
      "cost-model.js — provider-rate cost gates",
      "current-vs-prior.js — movement semantics without improved/worsened",
      "trend-display-rules.js — client copy thresholds",
    ],
  };
}

export { buildBrandLongitudinalFoundationReport as buildFoundationReport };
export * from "./selected-universe.js";
export * from "./radisson-gate.js";
export * from "./multi-parent-wave-orchestrator.js";
export * from "./resolve-bai-prior-comparable-period-v1.js";
export * from "./bai-wave3-longitudinal-intelligence-v1.js";
