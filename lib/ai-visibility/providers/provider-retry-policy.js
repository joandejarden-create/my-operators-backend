/**
 * Per-provider retry policies (Phase 3B.1).
 * Bounded: default 1 retry → max 2 attempts per planned call.
 */

export const OPENAI_RETRY_POLICY = Object.freeze({
  provider: "openai",
  maxRetriesPerCall: 1,
  maxAttemptsPerCall: 2,
  backoffMs: 1500,
  timeoutMsDefault: 180000,
  retryCategories: ["RATE_LIMIT", "TIMEOUT", "SERVER", "TOOL_FAILURE"],
});

export const GEMINI_RETRY_POLICY = Object.freeze({
  provider: "gemini",
  maxRetriesPerCall: 1,
  maxAttemptsPerCall: 2,
  backoffMs: 1500,
  timeoutMsDefault: 180000,
  retryCategories: ["RATE_LIMIT", "TIMEOUT", "SERVER", "TOOL_FAILURE"],
});

export const PERPLEXITY_RETRY_POLICY = Object.freeze({
  provider: "perplexity",
  maxRetriesPerCall: 1,
  maxAttemptsPerCall: 2,
  backoffMs: 2000,
  timeoutMsDefault: 120000,
  retryCategories: ["RATE_LIMIT", "TIMEOUT", "SERVER"],
});

export const CLAUDE_RETRY_POLICY = Object.freeze({
  provider: "claude",
  maxRetriesPerCall: 1,
  maxAttemptsPerCall: 2,
  backoffMs: 1500,
  /** 300s — web_search with direct callers; do not raise further without diagnosis. */
  timeoutMsDefault: 300000,
  retryCategories: ["RATE_LIMIT", "TIMEOUT", "SERVER", "TOOL_FAILURE"],
  webSearchMaxUses: 5,
  pauseContinuationEnabled: true,
});

/** @type {Record<string, object>} */
export const PROVIDER_RETRY_POLICIES = Object.freeze({
  openai: OPENAI_RETRY_POLICY,
  gemini: GEMINI_RETRY_POLICY,
  perplexity: PERPLEXITY_RETRY_POLICY,
  claude: CLAUDE_RETRY_POLICY,
});

/**
 * @param {string} providerId
 */
export function getProviderRetryPolicy(providerId) {
  return PROVIDER_RETRY_POLICIES[String(providerId || "").toLowerCase()] || OPENAI_RETRY_POLICY;
}
