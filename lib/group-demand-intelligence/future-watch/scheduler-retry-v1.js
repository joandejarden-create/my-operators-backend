/**
 * Provider error classification + bounded retry backoff for watch scheduler.
 */

import { PROVIDER_ERROR_CLASS, WATCH_STATUS, DATE_PROVENANCE } from "./constants.js";
import { addDays } from "./trigger-policy-v1.js";

/**
 * Classify an error / HTTP-ish failure.
 */
export function classifyProviderFailure(errOrMeta = {}) {
  const status = Number(errOrMeta.status || errOrMeta.statusCode || 0);
  const msg = String(errOrMeta.message || errOrMeta.error || errOrMeta || "").toLowerCase();
  const code = String(errOrMeta.code || "").toLowerCase();

  if (status === 429 || /rate.?limit|too many requests|429/.test(msg)) {
    return {
      class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR,
      reason: "RATE_LIMIT_429",
    };
  }
  if (status >= 500 || /5\d\d/.test(String(status))) {
    return {
      class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR,
      reason: "HTTP_5XX",
    };
  }
  if (
    /timeout|etimedout|aborted|econnreset|econnrefused|network|fetch failed|socket/.test(
      msg + code
    )
  ) {
    return {
      class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR,
      reason: "TIMEOUT_OR_NETWORK",
    };
  }
  if (/jev.?unavailable|circuit_open|jev_disabled/.test(msg)) {
    return {
      class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR,
      reason: "JEV_UNAVAILABLE",
    };
  }
  if (/invalid.?response|parse|schema/.test(msg)) {
    return {
      class: PROVIDER_ERROR_CLASS.NONRETRYABLE_PROVIDER_ERROR,
      reason: "INVALID_RESPONSE",
    };
  }
  if (errOrMeta.partial === true) {
    return { class: PROVIDER_ERROR_CLASS.PARTIAL_RESEARCH, reason: "PARTIAL" };
  }
  if (errOrMeta.noEvidence === true) {
    return { class: PROVIDER_ERROR_CLASS.NO_EVIDENCE, reason: "NO_EVIDENCE" };
  }
  return {
    class: PROVIDER_ERROR_CLASS.NONRETRYABLE_PROVIDER_ERROR,
    reason: "UNKNOWN_PROVIDER_ERROR",
  };
}

/**
 * Apply bounded retry to a watch. Never rejects candidate for provider failure.
 */
export function applyProviderRetry(watch = {}, classification = {}, config = {}) {
  const maxRetries = config.maxRetries ?? 3;
  const backoff = config.retryBackoffDays || [1, 3, 7];
  const retryCount = (Number(watch.retryCount) || 0) + 1;
  const today = new Date().toISOString().slice(0, 10);

  if (classification.class !== PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR) {
    return {
      ...watch,
      lastProviderError: classification,
      lastCheckedAt: new Date().toISOString(),
      // stay ACTIVE — do not reject
      watchStatus: watch.watchStatus,
      nextResearchDate: addDays(today, backoff[0] || 1),
      dateProvenance: DATE_PROVENANCE.HEURISTIC,
      nextTriggerCondition: `provider_error_reschedule:${classification.reason}`,
    };
  }

  if (retryCount > maxRetries) {
    return {
      ...watch,
      retryCount,
      watchStatus: WATCH_STATUS.RESEARCH_BLOCKED_PROVIDER,
      lastProviderError: classification,
      lastCheckedAt: new Date().toISOString(),
      nextTriggerCondition: `retry_exhausted:${classification.reason}`,
      stopConditions: [...(watch.stopConditions || []), "RESEARCH_BLOCKED_PROVIDER"],
    };
  }

  const days = backoff[Math.min(retryCount - 1, backoff.length - 1)] || 7;
  return {
    ...watch,
    retryCount,
    watchStatus: WATCH_STATUS.ACTIVE,
    lastProviderError: classification,
    lastCheckedAt: new Date().toISOString(),
    nextResearchDate: addDays(today, days),
    researchWindowStart: addDays(today, days),
    researchWindowEnd: addDays(today, days + 3),
    dateProvenance: DATE_PROVENANCE.HEURISTIC,
    nextTriggerCondition: `retry_${retryCount}_after_${days}d:${classification.reason}`,
  };
}
