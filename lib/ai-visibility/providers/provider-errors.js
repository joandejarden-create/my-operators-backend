/**
 * Provider-neutral error categories (Phase 3B.1).
 * Provider failure never becomes Brand absence.
 */

import { ProviderError } from "./base-provider.js";

export const PROVIDER_ERROR_CATEGORIES = Object.freeze({
  AUTH: "AUTH",
  RATE_LIMIT: "RATE_LIMIT",
  TIMEOUT: "TIMEOUT",
  SERVER: "SERVER",
  TOOL_FAILURE: "TOOL_FAILURE",
  MALFORMED_RESPONSE: "MALFORMED_RESPONSE",
  SAFETY_REFUSAL: "SAFETY_REFUSAL",
  CONTENT_EMPTY: "CONTENT_EMPTY",
  UNKNOWN: "UNKNOWN",
});

/** @type {Record<string, { retryable: boolean }>} */
export const PROVIDER_ERROR_RETRY_POLICY = Object.freeze({
  AUTH: { retryable: false },
  RATE_LIMIT: { retryable: true },
  TIMEOUT: { retryable: true },
  SERVER: { retryable: true },
  TOOL_FAILURE: { retryable: true },
  MALFORMED_RESPONSE: { retryable: false },
  SAFETY_REFUSAL: { retryable: false },
  CONTENT_EMPTY: { retryable: false },
  UNKNOWN: { retryable: false },
});

/**
 * Map provider-specific errors into canonical categories.
 * Preserves raw provider error on details.
 *
 * @param {unknown} err
 * @returns {{ category: string, retryable: boolean, message: string, raw: unknown }}
 */
export function classifyProviderError(err) {
  const message = String(err?.message || err || "Unknown provider error");
  const status = err?.status ?? err?.statusCode ?? null;
  const type = String(err?.type || "").toLowerCase();
  let category = PROVIDER_ERROR_CATEGORIES.UNKNOWN;

  if (status === 401 || status === 403 || type === "auth_error" || type === "config_error") {
    category = PROVIDER_ERROR_CATEGORIES.AUTH;
  } else if (status === 429 || type === "rate_limit") {
    category = PROVIDER_ERROR_CATEGORIES.RATE_LIMIT;
  } else if (
    status === 408 ||
    status === 504 ||
    type === "timeout" ||
    err?.name === "AbortError"
  ) {
    category = PROVIDER_ERROR_CATEGORIES.TIMEOUT;
  } else if (status >= 500 || type === "upstream_error" || type === "network_error") {
    category = PROVIDER_ERROR_CATEGORIES.SERVER;
  } else if (/tool|web_search|grounding|search/.test(type) || /tool|grounding/.test(message)) {
    category = PROVIDER_ERROR_CATEGORIES.TOOL_FAILURE;
  } else if (/credit balance|billing|payment|insufficient.*credit|quota exceeded/i.test(message)) {
    category = PROVIDER_ERROR_CATEGORIES.AUTH;
    category = PROVIDER_ERROR_CATEGORIES.SAFETY_REFUSAL;
  } else if (/empty|no content|no text/.test(message.toLowerCase())) {
    category = PROVIDER_ERROR_CATEGORIES.CONTENT_EMPTY;
  } else if (/malformed|parse|json|invalid response/.test(message.toLowerCase())) {
    category = PROVIDER_ERROR_CATEGORIES.MALFORMED_RESPONSE;
  }

  const retryable = PROVIDER_ERROR_RETRY_POLICY[category]?.retryable ?? false;
  return {
    category,
    retryable,
    message,
    raw: err instanceof ProviderError ? { ...err, name: err.name } : err,
  };
}
