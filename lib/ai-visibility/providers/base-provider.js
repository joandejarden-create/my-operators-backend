/**
 * Provider base helpers — shared error normalization (no provider-specific parsing).
 */

export class ProviderError extends Error {
  /**
   * @param {string} message
   * @param {{ type?: string, status?: number, retryable?: boolean, details?: object }} [opts]
   */
  constructor(message, opts = {}) {
    super(message);
    this.name = "ProviderError";
    this.type = opts.type || "provider_error";
    this.status = opts.status ?? null;
    this.retryable = Boolean(opts.retryable);
    this.details = opts.details || null;
  }
}

export function normalizeProviderHttpError(status, bodyMessage) {
  const msg = String(bodyMessage || `Provider HTTP ${status}`);
  if (status === 429) {
    return new ProviderError(msg, { type: "rate_limit", status, retryable: true });
  }
  if (status === 408 || status === 504) {
    return new ProviderError(msg, { type: "timeout", status, retryable: true });
  }
  if (status >= 500) {
    return new ProviderError(msg, { type: "upstream_error", status, retryable: true });
  }
  return new ProviderError(msg, { type: "request_error", status, retryable: false });
}

/**
 * Strip secrets from objects before logging/storage of providerMeta summaries.
 * @param {unknown} value
 */
export function redactSecrets(value) {
  if (value == null) return value;
  if (typeof value === "string") {
    if (/sk-[a-zA-Z0-9_-]{10,}/.test(value) || /Bearer\s+\S+/i.test(value)) {
      return "[REDACTED]";
    }
    return value;
  }
  if (Array.isArray(value)) return value.map(redactSecrets);
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (/authorization|api[_-]?key|secret|token|password/i.test(k)) {
        out[k] = "[REDACTED]";
      } else {
        out[k] = redactSecrets(v);
      }
    }
    return out;
  }
  return value;
}
