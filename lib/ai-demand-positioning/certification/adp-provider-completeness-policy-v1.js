/**
 * ADP_PROVIDER_COMPLETENESS_POLICY_V1 — source-of-truth completeness contract.
 *
 * Extracted from certifyAdpPeriod providerCompletenessGate (no methodology change).
 * Thresholds documented here so inventory / YOTEL forensics do not invent policy.
 *
 * Rules (current implementation):
 * 1. Expected matrix = periodScenarioCount × governed providers (openai, gemini, perplexity, claude).
 * 2. Overall material failure (→ QA_REVIEW_REQUIRED unless silent reduction → hard fail):
 *    failed + timedOut > max(2, floor(expected × 0.10))
 *    OR any governed provider has successful === 0
 * 3. Provider-level material failure (same 10% absolute floor, per provider expected = scenarioCount):
 *    provider.failed + provider.timedOut > max(2, floor(scenarioCount × 0.10))
 * 4. Silent denominator reduction (→ hard fail SILENT_DENOMINATOR_REDUCTION):
 *    attempted < expected × 0.85 AND failed+timedOut cannot explain the gap
 * 5. Failed/timeout observations are NOT comparable → excluded from AI Consideration denominator
 *    (filterComparableObservations). Denominator shrinks to successful parseable responses.
 * 6. Retries: not required by this gate; runners may retry before observations are finalized.
 */

import { PROVIDERS } from "../data-model.js";

export const ADP_PROVIDER_COMPLETENESS_POLICY_NAME = "ADP_PROVIDER_COMPLETENESS_POLICY_V1";
export const ADP_PROVIDER_COMPLETENESS_POLICY_VERSION = "adp_provider_completeness_policy_v1";

/** Absolute floor on allowed failures (overall and per-provider). */
export const COMPLETENESS_ABSOLUTE_FAILURE_FLOOR = 2;

/** Fraction of expected responses allowed to fail/timeout before material failure. */
export const COMPLETENESS_PERCENT_TOLERANCE = 0.1;

/** Attempted/expected below this without explained failures → silent reduction hard fail. */
export const COMPLETENESS_SILENT_REDUCTION_ATTEMPTED_RATIO = 0.85;

export function maxAllowedFailures(expectedCount) {
  const n = Math.max(0, Number(expectedCount) || 0);
  return Math.max(COMPLETENESS_ABSOLUTE_FAILURE_FLOOR, Math.floor(n * COMPLETENESS_PERCENT_TOLERANCE));
}

/**
 * Evaluate provider completeness for a period against the governed policy.
 * @returns {object} gate result consumed by certifyAdpPeriod
 */
export function evaluateProviderCompletenessGate(period, liveScenarioCount = null) {
  const expectedProviders = [...PROVIDERS];
  const byProvider = {};
  let attempted = 0;
  let successful = 0;
  let failed = 0;
  let timedOut = 0;
  let parsed = 0;
  let rankEligible = 0;
  let citationEligible = 0;

  const periodScenarioIds = [
    ...new Set((period?.observations || []).map((o) => o.scenarioId).filter(Boolean)),
  ];
  const scenarioCount =
    period?.scenarioCount ||
    period?.scenarioUniverse?.scenarioCount ||
    periodScenarioIds.length ||
    0;

  for (const p of expectedProviders) {
    byProvider[p] = {
      expected: scenarioCount,
      attempted: 0,
      successful: 0,
      failed: 0,
      timedOut: 0,
      parsed: 0,
      rankEligible: 0,
      citationEligible: 0,
    };
  }

  for (const o of period?.observations || []) {
    const p = o.provider || "unknown";
    if (!byProvider[p]) {
      byProvider[p] = {
        expected: scenarioCount,
        attempted: 0,
        successful: 0,
        failed: 0,
        timedOut: 0,
        parsed: 0,
        rankEligible: 0,
        citationEligible: 0,
      };
    }
    byProvider[p].attempted += 1;
    attempted += 1;
    const isFail = Boolean(
      o.error || o.providerError || o.status === "FAILED" || (o.httpStatus && o.httpStatus >= 400)
    );
    const isTimeout = Boolean(o.timeout || /timeout/i.test(String(o.error || "")));
    if (isTimeout) {
      byProvider[p].timedOut += 1;
      timedOut += 1;
    } else if (isFail) {
      byProvider[p].failed += 1;
      failed += 1;
    } else {
      byProvider[p].successful += 1;
      successful += 1;
    }
    if (o.parsed || o.mentioned != null) {
      byProvider[p].parsed += 1;
      parsed += 1;
    }
    if (o.rankEligible !== false && (o.position != null || o.rank != null || o.parsed)) {
      byProvider[p].rankEligible += 1;
      rankEligible += 1;
    }
    const cites = o.sourcesCited?.length || o.providerCitations?.length || 0;
    if (cites > 0 || o.citationEligible) {
      byProvider[p].citationEligible += 1;
      citationEligible += 1;
    }
  }

  const expected = scenarioCount * expectedProviders.length;
  const overallFailureCap = maxAllowedFailures(expected);
  const overallFailureCount = failed + timedOut;
  const overallMaterial =
    scenarioCount > 0 && overallFailureCount > overallFailureCap;

  const zeroSuccessProvider = expectedProviders.some(
    (p) => (byProvider[p]?.successful || 0) === 0
  );

  // Provider-level imbalance guard — same 10% / floor-2 rule per provider expected matrix.
  const providerFailureCap = maxAllowedFailures(scenarioCount);
  const providerImbalance = [];
  for (const p of expectedProviders) {
    const row = byProvider[p] || {};
    const pFail = (row.failed || 0) + (row.timedOut || 0);
    const successRate = scenarioCount > 0 ? (row.successful || 0) / scenarioCount : 1;
    if (pFail > providerFailureCap || (scenarioCount > 0 && (row.successful || 0) === 0)) {
      providerImbalance.push({
        provider: p,
        failed: row.failed || 0,
        timedOut: row.timedOut || 0,
        successful: row.successful || 0,
        expected: scenarioCount,
        failureCap: providerFailureCap,
        successRate: Math.round(successRate * 1000) / 10,
      });
    }
  }

  const silentReduction =
    scenarioCount > 0 &&
    attempted > 0 &&
    attempted < expected * COMPLETENESS_SILENT_REDUCTION_ATTEMPTED_RATIO &&
    failed + timedOut < Math.max(1, expected - attempted);

  const liveUniverseDrift =
    liveScenarioCount != null && scenarioCount > 0 && liveScenarioCount !== scenarioCount;

  const materialFailure =
    overallMaterial || zeroSuccessProvider || providerImbalance.length > 0 || silentReduction;

  return {
    policyName: ADP_PROVIDER_COMPLETENESS_POLICY_NAME,
    policyVersion: ADP_PROVIDER_COMPLETENESS_POLICY_VERSION,
    absoluteFailureFloor: COMPLETENESS_ABSOLUTE_FAILURE_FLOOR,
    percentTolerance: COMPLETENESS_PERCENT_TOLERANCE,
    overallFailureCap,
    providerFailureCap,
    expected,
    attempted,
    successful,
    failed,
    timedOut,
    parsed,
    rankEligible,
    citationEligible,
    byProvider,
    expectedProviders,
    periodScenarioCount: scenarioCount,
    liveScenarioCount,
    liveUniverseDrift,
    overallMaterial,
    zeroSuccessProvider,
    providerImbalance,
    materialFailure,
    silentDenominatorReduction: silentReduction,
    /** Failed/timeout rows are excluded from consideration denominator (comparable filter). */
    failedExcludedFromConsiderationDenominator: true,
    retryRequiredByPolicy: false,
  };
}

/**
 * Human-readable contract summary for reports.
 */
export function describeProviderCompletenessPolicy() {
  return {
    policyName: ADP_PROVIDER_COMPLETENESS_POLICY_NAME,
    policyVersion: ADP_PROVIDER_COMPLETENESS_POLICY_VERSION,
    howManyFailuresMayOccur: `max(${COMPLETENESS_ABSOLUTE_FAILURE_FLOOR}, floor(expected × ${COMPLETENESS_PERCENT_TOLERANCE})) overall AND per provider`,
    percentageBased: true,
    percentTolerance: COMPLETENESS_PERCENT_TOLERANCE,
    absoluteTolerance: COMPLETENESS_ABSOLUTE_FAILURE_FLOOR,
    oneTimeoutAllowed: `YES when failed+timedOut ≤ cap (for expected=252, cap=${maxAllowedFailures(252)}; for per-provider scenarioCount=63, cap=${maxAllowedFailures(63)})`,
    retriesRequired: false,
    providerMateriallyIncompleteMayCertify:
      "NO — zero successful responses for any provider OR per-provider failures above cap → QA_REVIEW_REQUIRED (or hard fail if silent reduction)",
    whenQaReviewRequired:
      "materialFailure without silentDenominatorReduction (overall or provider-level imbalance)",
    whenQaFailed: "silentDenominatorReduction (attempted < 85% expected unexplained) or other hard gates",
    considerationDenominator:
      "comparable successful observations only — timeouts/errors excluded (not silent grain change; documented exclusion)",
  };
}
