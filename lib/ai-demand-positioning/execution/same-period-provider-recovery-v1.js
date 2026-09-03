/**
 * Governed same-period targeted provider recovery V1.
 *
 * Completes missing observations inside an existing frozen period.
 * Never creates a new period. Never reruns successful observations.
 * Never overwrites original failure provenance.
 *
 * Gate linkage: PROVIDER_COVERAGE_RECOVERY_COMPLETE
 * Defect: UNRECOVERED_PROVIDER_COVERAGE_GAP
 */

import { savePeriod } from "../data-model.js";
import { parseObservation } from "../execution/response-parser.js";
import {
  callProvider,
  PROVIDER_CONFIGS,
} from "../execution/multi-provider-runner.js";
import { isComparableObservation } from "../metrics/grain-governance.js";
import {
  classifyProviderFailure,
  isRecoverableFailureClass,
  RECOMMENDED_RETRY_POLICY,
  PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
} from "../measurement-assurance/provider-coverage-recovery-v1.js";

export const SAME_PERIOD_PROVIDER_RECOVERY_VERSION = "adp_same_period_provider_recovery_v1";
export const RECOVERY_REASON = "MATERIAL_MISSING_OBSERVATIONS_REQUIRE_RECOVERY_ATTEMPT";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoffMs(attemptIndex, policy = RECOMMENDED_RETRY_POLICY) {
  const raw = policy.initialBackoffMs * Math.pow(policy.backoffMultiplier, attemptIndex);
  return Math.min(policy.maxBackoffMs, Math.max(policy.initialBackoffMs, raw));
}

/**
 * Immutable snapshot of the original failed observation state (first recovery only).
 */
export function captureOriginalFailureSnapshot(obs) {
  return {
    capturedAt: new Date().toISOString(),
    error: obs.error ?? null,
    failureClass: classifyProviderFailure(obs),
    timestamp: obs.timestamp || null,
    rawResponse: obs.rawResponse ?? null,
    rawResponseLength: obs.rawResponseLength ?? null,
    parsed: obs.parsed === true,
    mentioned: obs.mentioned ?? null,
    position: obs.position ?? null,
    competitorsMentioned: Array.isArray(obs.competitorsMentioned)
      ? [...obs.competitorsMentioned]
      : [],
    attributesRecognized: Array.isArray(obs.attributesRecognized)
      ? [...obs.attributesRecognized]
      : [],
    sourcesCited: Array.isArray(obs.sourcesCited) ? [...obs.sourcesCited] : [],
    governedInterpretation: obs.governedInterpretation
      ? JSON.parse(JSON.stringify(obs.governedInterpretation))
      : null,
    model: obs.model || null,
  };
}

function pushAttempt(obs, attempt) {
  if (!Array.isArray(obs.recoveryAttempts)) obs.recoveryAttempts = [];
  obs.recoveryAttempts.push(attempt);
  if (!Array.isArray(obs.recoveryHistory)) obs.recoveryHistory = [];
  obs.recoveryHistory.push({
    type: "RECOVERY_ATTEMPT",
    ...attempt,
  });
}

/**
 * Apply a successful provider response onto the existing observation without
 * destroying originalFailureSnapshot / prior recoveryAttempts.
 */
export function applySuccessfulRecoveryToObservation(obs, result, meta, propertyProfile) {
  if (!obs.originalFailureSnapshot) {
    obs.originalFailureSnapshot = captureOriginalFailureSnapshot(obs);
  }

  const attempt = {
    attemptNumber: meta.attemptNumber,
    timestamp: meta.timestamp || new Date().toISOString(),
    provider: meta.provider,
    model: result.model || meta.model || PROVIDER_CONFIGS[meta.provider]?.model || null,
    success: true,
    error: null,
    failureClass: null,
    responseLength: (result.response || "").length,
    reason: RECOVERY_REASON,
    policyVersion: PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
    recoveryWriterVersion: SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
  };
  pushAttempt(obs, attempt);

  // Clear live failure; keep provenance on originalFailureSnapshot
  delete obs.error;
  obs.rawResponse = result.response || "";
  obs.rawResponseLength = obs.rawResponse.length;
  if (result.model || meta.model) obs.model = result.model || meta.model;
  if (result.citations?.length) obs.providerCitations = result.citations;

  const parsed = parseObservation({ ...obs }, propertyProfile);
  Object.assign(obs, parsed);

  obs.recoveredAt = attempt.timestamp;
  obs.recoveryReason = RECOVERY_REASON;
  obs.finalGovernedStatus = "RECOVERED_COMPARABLE";
  obs.recoveryInclusionStatus = "INCLUDED_IN_METRICS";
  obs.metricInclusion = true;
  return obs;
}

export function applyFailedRecoveryAttempt(obs, error, meta) {
  if (!obs.originalFailureSnapshot) {
    obs.originalFailureSnapshot = captureOriginalFailureSnapshot(obs);
  }
  const failureClass = classifyProviderFailure({ error });
  const attempt = {
    attemptNumber: meta.attemptNumber,
    timestamp: meta.timestamp || new Date().toISOString(),
    provider: meta.provider,
    model: meta.model || PROVIDER_CONFIGS[meta.provider]?.model || null,
    success: false,
    error: String(error || "unknown"),
    failureClass,
    responseLength: 0,
    reason: RECOVERY_REASON,
    policyVersion: PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
    recoveryWriterVersion: SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
  };
  pushAttempt(obs, attempt);
  obs.error = attempt.error;
  obs.finalGovernedStatus = "RESIDUAL_MISSING_AFTER_RECOVERY";
  obs.recoveryInclusionStatus = "EXCLUDED_FROM_METRICS";
  obs.metricInclusion = false;
  obs.recoveryAttempted = true;
  return obs;
}

/**
 * Recover one missing observation in-place (same period object).
 */
export async function recoverOneObservation({
  obs,
  query,
  provider,
  propertyProfile,
  policy = RECOMMENDED_RETRY_POLICY,
  dryRun = false,
}) {
  if (isComparableObservation(obs)) {
    return { status: "SKIPPED_ALREADY_COMPARABLE", obs, attempts: 0 };
  }
  const failureClass = classifyProviderFailure(obs);
  if (!isRecoverableFailureClass(failureClass) && policy.retryOnlyRecoverable) {
    return { status: "SKIPPED_NON_RECOVERABLE", obs, failureClass, attempts: 0 };
  }

  const config = PROVIDER_CONFIGS[provider];
  if (!config) return { status: "FAIL_UNKNOWN_PROVIDER", obs, attempts: 0 };

  const maxAttempts = policy.maxAttemptsPerObservation || 3;
  let lastError = obs.error;

  for (let i = 0; i < maxAttempts; i++) {
    if (i > 0) {
      let wait = backoffMs(i - 1, policy);
      if (/429|rate.?limit/i.test(String(lastError || ""))) {
        wait = Math.max(wait, policy.rateLimitCooldownMs || 60000);
      }
      if (!dryRun) await sleep(wait);
    } else if (!dryRun && config.delayMs) {
      await sleep(config.delayMs);
    }

    const attemptNumber = (obs.recoveryAttempts?.length || 0) + 1;
    const timestamp = new Date().toISOString();

    if (dryRun) {
      pushAttempt(obs, {
        attemptNumber,
        timestamp,
        provider,
        model: config.model,
        success: false,
        error: "DRY_RUN",
        failureClass: "DRY_RUN",
        responseLength: 0,
        reason: RECOVERY_REASON,
        policyVersion: PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
        recoveryWriterVersion: SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
      });
      return { status: "DRY_RUN", obs, attempts: 1 };
    }

    const result = await callProvider(provider, query, config);
    if (!result.error && result.response != null) {
      applySuccessfulRecoveryToObservation(
        obs,
        result,
        { attemptNumber, timestamp, provider, model: config.model },
        propertyProfile
      );
      obs.recoveryAttempted = true;
      return {
        status: "RECOVERED",
        obs,
        attempts: i + 1,
        comparable: isComparableObservation(obs),
      };
    }

    lastError = result.error;
    applyFailedRecoveryAttempt(obs, result.error, {
      attemptNumber,
      timestamp,
      provider,
      model: config.model,
    });
  }

  obs.recoveryAttempted = true;
  return {
    status: "RESIDUAL_MISSING",
    obs,
    attempts: maxAttempts,
    lastError,
    comparable: false,
  };
}

/**
 * Recover a list of { observationId, scenarioId, query } targets on one period.
 */
export async function recoverMissingObservationsInPeriod({
  period,
  propertyProfile,
  targets,
  provider = "gemini",
  policy = RECOMMENDED_RETRY_POLICY,
  dryRun = false,
  onProgress = null,
  save = true,
}) {
  const results = [];
  let idx = 0;
  for (const target of targets) {
    idx += 1;
    const obs = (period.observations || []).find(
      (o) =>
        o.provider === provider &&
        (o.observationId === target.observationId ||
          (o.scenarioId === target.scenarioId && !isComparableObservation(o)))
    );
    if (!obs) {
      results.push({
        scenarioId: target.scenarioId,
        status: "NOT_FOUND",
      });
      continue;
    }
    if (isComparableObservation(obs)) {
      results.push({
        observationId: obs.observationId,
        scenarioId: obs.scenarioId,
        status: "SKIPPED_ALREADY_COMPARABLE",
      });
      continue;
    }

    const query = target.query;
    if (!query) {
      results.push({
        observationId: obs.observationId,
        scenarioId: obs.scenarioId,
        status: "MISSING_QUERY",
      });
      continue;
    }

    const outcome = await recoverOneObservation({
      obs,
      query,
      provider,
      propertyProfile,
      policy,
      dryRun,
    });
    results.push({
      observationId: obs.observationId,
      scenarioId: obs.scenarioId,
      intent: target.intent || null,
      ...outcome,
      status: outcome.status,
    });
    if (onProgress) onProgress(idx, targets.length, outcome);

    if (save && !dryRun && idx % 3 === 0) {
      period.samePeriodRecovery = {
        version: SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
        provider,
        inProgress: true,
        lastCheckpointAt: new Date().toISOString(),
        completedAttempts: idx,
      };
      savePeriod(period);
    }
  }

  const recovered = results.filter((r) => r.status === "RECOVERED").length;
  const residual = results.filter((r) => r.status === "RESIDUAL_MISSING").length;

  period.samePeriodRecovery = {
    version: SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
    provider,
    reason: RECOVERY_REASON,
    policyVersion: PROVIDER_COVERAGE_RECOVERY_POLICY_VERSION,
    completedAt: new Date().toISOString(),
    dryRun,
    targeted: targets.length,
    recovered,
    residualMissing: residual,
    results: results.map((r) => ({
      observationId: r.observationId,
      scenarioId: r.scenarioId,
      status: r.status,
      attempts: r.attempts || 0,
    })),
  };

  if (save && !dryRun) savePeriod(period);

  return {
    periodId: period.periodId,
    propertyId: period.propertyId,
    provider,
    targeted: targets.length,
    recovered,
    residualMissing: residual,
    results,
    period,
  };
}
