/**
 * PROVIDER_COVERAGE_RECOVERY_COMPLETE — permanent ADP Measurement Assurance gate.
 *
 * Principles:
 *   MISSING != ZERO (unchanged)
 *   MATERIAL_MISSING_OBSERVATIONS_REQUIRE_RECOVERY_ATTEMPT
 *
 * Defect class: UNRECOVERED_PROVIDER_COVERAGE_GAP
 *
 * Does NOT execute LLM recovery — audit + certification policy only.
 */

import { isComparableObservation } from "../metrics/grain-governance.js";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";

export const PROVIDER_COVERAGE_RECOVERY_GATE = "PROVIDER_COVERAGE_RECOVERY_COMPLETE";
export const MATERIAL_MISSING_OBSERVATIONS_REQUIRE_RECOVERY_ATTEMPT =
  "MATERIAL_MISSING_OBSERVATIONS_REQUIRE_RECOVERY_ATTEMPT";
export const DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP = "UNRECOVERED_PROVIDER_COVERAGE_GAP";
export const PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION = "adp_provider_coverage_recovery_policy_v1";

/** Governed failure taxonomy. */
export const FAILURE_CLASSES = Object.freeze({
  PROVIDER_503: "PROVIDER_503",
  PROVIDER_429: "PROVIDER_429",
  TIMEOUT: "TIMEOUT",
  NETWORK_ERROR: "NETWORK_ERROR",
  MALFORMED_RESPONSE: "MALFORMED_RESPONSE",
  PARSER_FAILURE: "PARSER_FAILURE",
  EMPTY_RESPONSE: "EMPTY_RESPONSE",
  AUTH_ERROR: "AUTH_ERROR",
  TOOL_EXECUTION_FAILURE: "TOOL_EXECUTION_FAILURE",
  UNSUPPORTED_RESPONSE: "UNSUPPORTED_RESPONSE",
  INTENTIONAL_SKIP: "INTENTIONAL_SKIP",
  UNKNOWN: "UNKNOWN",
});

/** Recommended bounded retry policy (founder-approvable). */
export const RECOMMENDED_RETRY_POLICY = Object.freeze({
  version: PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
  maxAttemptsPerObservation: 3,
  initialBackoffMs: 5000,
  backoffMultiplier: 2,
  maxBackoffMs: 120000,
  rateLimitCooldownMs: 60000,
  retryOnlyRecoverable: true,
  neverRerunSuccessfulObservations: true,
  samePeriodOnly: true,
  requireFounderApprovalForPaidCalls: true,
  preserveOriginalFailureRecord: true,
  costEstimateRequiredBeforeExecute: true,
});

/** Materiality thresholds — certification cannot ignore these without recovery attempt. */
export const MATERIALITY_THRESHOLDS = Object.freeze({
  minAbsoluteMissing: 5,
  minPercentOfProviderUniverse: 5,
  minClusterInSingleTerritory: 3,
  note:
    "Material if absolute≥5 OR pct≥5% of provider scheduled OR ≥3 failures in one demand territory. " +
    "Material recoverable gaps with zero recovery attempts → ASSURANCE_REVIEW_REQUIRED / block CERTIFIED.",
});

export function classifyProviderFailure(obs) {
  const err = String(obs?.error || "").toLowerCase();
  if (!err && obs?.dryRun) return FAILURE_CLASSES.INTENTIONAL_SKIP;
  if (!err && !obs?.rawResponse && !obs?.parsed) return FAILURE_CLASSES.EMPTY_RESPONSE;
  if (/503|high demand|unavailable|overloaded/.test(err)) return FAILURE_CLASSES.PROVIDER_503;
  if (/429|rate.?limit|too many requests/.test(err)) return FAILURE_CLASSES.PROVIDER_429;
  if (/timeout|aborted|etimedout/.test(err)) return FAILURE_CLASSES.TIMEOUT;
  if (/econnreset|enotfound|network|socket|fetch failed/.test(err)) return FAILURE_CLASSES.NETWORK_ERROR;
  if (/401|403|api.?key|unauthorized|auth/.test(err)) return FAILURE_CLASSES.AUTH_ERROR;
  if (/malformed|invalid json|unexpected token/.test(err)) return FAILURE_CLASSES.MALFORMED_RESPONSE;
  if (/parse|parser/.test(err)) return FAILURE_CLASSES.PARSER_FAILURE;
  if (/unsupported|not supported/.test(err)) return FAILURE_CLASSES.UNSUPPORTED_RESPONSE;
  if (/tool|function.?call/.test(err)) return FAILURE_CLASSES.TOOL_EXECUTION_FAILURE;
  if (err) return FAILURE_CLASSES.UNKNOWN;
  return FAILURE_CLASSES.UNKNOWN;
}

export function isRecoverableFailureClass(failureClass) {
  return [
    FAILURE_CLASSES.PROVIDER_503,
    FAILURE_CLASSES.PROVIDER_429,
    FAILURE_CLASSES.TIMEOUT,
    FAILURE_CLASSES.NETWORK_ERROR,
    FAILURE_CLASSES.MALFORMED_RESPONSE,
    FAILURE_CLASSES.TOOL_EXECUTION_FAILURE,
  ].includes(failureClass);
}

export function isNonRecoverableWithoutOperator(failureClass) {
  return [
    FAILURE_CLASSES.AUTH_ERROR,
    FAILURE_CLASSES.UNSUPPORTED_RESPONSE,
    FAILURE_CLASSES.INTENTIONAL_SKIP,
  ].includes(failureClass);
}

/**
 * Same-period recovery architecture assessment.
 * Period JSON is mutable via savePeriod; no append-only ledger exists yet.
 */
export const RESIDUAL_COVERAGE_CLASS = Object.freeze({
  COMPLETE: "COMPLETE",
  ACCEPTABLE_RESIDUAL_DISCLOSURE: "ACCEPTABLE_RESIDUAL_DISCLOSURE",
  MATERIAL_UNRESOLVED_GAP: "MATERIAL_UNRESOLVED_GAP",
});

/**
 * Classify residual provider coverage after (or before) recovery attempts.
 */
export function classifyResidualProviderCoverage(ledger) {
  const missing = ledger?.missingObservations || [];
  if (!missing.length) {
    return {
      class: RESIDUAL_COVERAGE_CLASS.COMPLETE,
      reason: "No missing provider observations",
    };
  }
  const materialRecoverableUnattempted = (ledger.recoverableUnattempted || []).filter((r) => {
    const providerRow = (ledger.providers || []).find((p) => p.provider === r.provider);
    return providerRow?.materialGap;
  });
  if (materialRecoverableUnattempted.length) {
    return {
      class: RESIDUAL_COVERAGE_CLASS.MATERIAL_UNRESOLVED_GAP,
      reason: `${materialRecoverableUnattempted.length} material recoverable gap(s) still unattempted`,
      count: materialRecoverableUnattempted.length,
    };
  }
  return {
    class: RESIDUAL_COVERAGE_CLASS.ACCEPTABLE_RESIDUAL_DISCLOSURE,
    reason:
      "Residual missing after recovery attempt and/or non-recoverable failures; disclose under MISSING != ZERO",
    residualCount: missing.length,
  };
}

export function assessSamePeriodRecoveryArchitecture() {
  return {
    technicallyFeasible: true,
    governedRecoveryModuleExists: true,
    governedRecoveryModulePath:
      "lib/ai-demand-positioning/execution/same-period-provider-recovery-v1.js",
    appendOnlyLedgerExists: false,
    safePattern:
      "Keep original failed observation via originalFailureSnapshot; append recoveryAttempts[] / " +
      "recoveryHistory[]; on success parse onto live fields without deleting provenance; " +
      "never create a new periodId.",
    riskIfNaiveOverwrite: "Losing original error/status would break auditability.",
    status: "GOVERNED_MODULE_AVAILABLE",
    permanentPipeline:
      "OBSERVATION_COMPLETE → detect provider gaps → classify recoverability → " +
      "bounded targeted recovery (founder approval for paid) → re-assurance → certification decision",
    blockerBeforeExecute: null,
  };
}

/**
 * Build missing-observation ledger for one period.
 */
export function buildProviderCoverageGapLedger({
  propertyId,
  period,
  scenarios = [],
  propertyProfile = null,
}) {
  const scenarioById = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  const rows = [];
  const byProvider = Object.create(null);

  for (const obs of period?.observations || []) {
    const provider = obs.provider || "unknown";
    if (!byProvider[provider]) {
      byProvider[provider] = {
        provider,
        scheduled: 0,
        success: 0,
        failed: 0,
        missing: 0,
        comparable: 0,
        failureClasses: Object.create(null),
        missingObservations: [],
      };
    }
    const bucket = byProvider[provider];
    bucket.scheduled += 1;

    if (isComparableObservation(obs)) {
      bucket.success += 1;
      bucket.comparable += 1;
      continue;
    }

    bucket.failed += 1;
    bucket.missing += 1;
    const failureClass = classifyProviderFailure(obs);
    bucket.failureClasses[failureClass] = (bucket.failureClasses[failureClass] || 0) + 1;
    const sc = scenarioById[obs.scenarioId] || {};
    const recoverable = isRecoverableFailureClass(failureClass);
    const recoveryAttempted = Boolean(obs.recoveryAttempts?.length || obs.recoveryAttempted);
    const row = {
      propertyId,
      propertyName: propertyProfile?.name || propertyId,
      periodId: period?.periodId,
      observationId: obs.observationId || null,
      scenarioId: obs.scenarioId,
      intent: sc.intent || null,
      territory: sc.intent || null,
      frame: sc.frame || null,
      provider,
      failureClass,
      failureReason: String(obs.error || "unknown").slice(0, 240),
      recoverable,
      recoveryAttempted,
      originalTimestamp: obs.executedAt || obs.createdAt || obs.timestamp || period?.executionDate || null,
      attemptCount: (obs.recoveryAttempts?.length || 0) + 1,
      hasRawResponse: Boolean(obs.rawResponse),
      parsed: obs.parsed === true,
    };
    bucket.missingObservations.push(row);
    rows.push(row);
  }

  const providers = Object.values(byProvider).map((b) => {
    const pctMissing = b.scheduled ? (b.missing / b.scheduled) * 100 : 0;
    const byTerritory = Object.create(null);
    for (const m of b.missingObservations) {
      const t = m.intent || "unknown";
      byTerritory[t] = (byTerritory[t] || 0) + 1;
    }
    const maxCluster = Math.max(0, ...Object.values(byTerritory));
    const material =
      b.missing >= MATERIALITY_THRESHOLDS.minAbsoluteMissing ||
      pctMissing >= MATERIALITY_THRESHOLDS.minPercentOfProviderUniverse ||
      maxCluster >= MATERIALITY_THRESHOLDS.minClusterInSingleTerritory;

    const recoverableUnattempted = b.missingObservations.filter((m) => m.recoverable && !m.recoveryAttempted);
    const dominantFailure =
      Object.entries(b.failureClasses).sort((a, c) => c[1] - a[1])[0]?.[0] || null;

    return {
      ...b,
      pctMissing: Math.round(pctMissing * 10) / 10,
      clusteringByTerritory: byTerritory,
      maxTerritoryCluster: maxCluster,
      materialGap: material && b.missing > 0,
      recoverableUnattemptedCount: recoverableUnattempted.length,
      dominantFailure,
      failureReasonSummary: dominantFailure,
    };
  });

  const recoverableUnattempted = rows.filter((r) => r.recoverable && !r.recoveryAttempted);
  const materialProviders = providers.filter((p) => p.materialGap);
  const materialRecoverableUnattempted = materialProviders.filter((p) => p.recoverableUnattemptedCount > 0);

  const estimatedCalls = recoverableUnattempted.length;
  const costByProvider = Object.create(null);
  let estimatedCost = 0;
  for (const r of recoverableUnattempted) {
    const unit = PROVIDER_CONFIGS[r.provider]?.costPerCall || 0.03;
    costByProvider[r.provider] = (costByProvider[r.provider] || 0) + unit;
    estimatedCost += unit;
  }

  const architecture = assessSamePeriodRecoveryArchitecture();

  let gateStatus = "PASS";
  let certificationImpact = "NONE";
  if (materialRecoverableUnattempted.length) {
    gateStatus = "FAIL";
    certificationImpact = "ASSURANCE_REVIEW_REQUIRED";
  } else if (rows.some((r) => r.recoverable && !r.recoveryAttempted && !materialProviders.length)) {
    gateStatus = "PASS_WITH_DISCLOSURE";
    certificationImpact = "DISCLOSURE_ONLY";
  } else if (rows.length && rows.every((r) => r.recoveryAttempted || !r.recoverable)) {
    gateStatus = "PASS_WITH_DISCLOSURE";
    certificationImpact = "RESIDUAL_UNRECOVERABLE_OR_ATTEMPTED";
  }

  const residualCoverage = classifyResidualProviderCoverage({
    missingObservations: rows,
    recoverableUnattempted,
    providers,
  });

  const recoveryExecuted = (period?.observations || []).some(
    (o) => o.recoveryAttempted || (o.recoveryAttempts && o.recoveryAttempts.length)
  );

  return {
    version: PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
    gate: PROVIDER_COVERAGE_RECOVERY_GATE,
    principle: MATERIAL_MISSING_OBSERVATIONS_REQUIRE_RECOVERY_ATTEMPT,
    defectClass: DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
    propertyId,
    periodId: period?.periodId,
    providers,
    missingObservations: rows,
    recoverableUnattempted,
    materialProviders: materialProviders.map((p) => p.provider),
    gateStatus,
    certificationImpact,
    residualCoverage,
    architecture,
    recoveryPreview: {
      estimatedCalls,
      estimatedCostUsd: Math.round(estimatedCost * 100) / 100,
      costByProvider,
      samePeriodRecoverySafe: architecture.technicallyFeasible,
      requiresFounderApproval: true,
      executed: recoveryExecuted,
      samePeriodRecoveryMeta: period?.samePeriodRecovery || null,
    },
    recommendedRetryPolicy: RECOMMENDED_RETRY_POLICY,
    materialityThresholds: MATERIALITY_THRESHOLDS,
  };
}

/**
 * Evaluate permanent gate for assurance certification.
 */
export function evaluateProviderCoverageRecoveryGate(ledger) {
  const pass = ledger.gateStatus === "PASS" || ledger.gateStatus === "PASS_WITH_DISCLOSURE";
  return {
    gate: PROVIDER_COVERAGE_RECOVERY_GATE,
    status: ledger.gateStatus,
    pass,
    blocksFullCertification: ledger.gateStatus === "FAIL",
    certificationImpact: ledger.certificationImpact,
    summary:
      ledger.gateStatus === "FAIL"
        ? `${ledger.recoverableUnattempted.length} recoverable missing observation(s) unattempted; material provider gap(s): ${ledger.materialProviders.join(", ") || "none"}`
        : ledger.missingObservations?.length
          ? "Residual gaps disclosed or non-material"
          : "Full provider coverage",
    defectClass: DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
  };
}
