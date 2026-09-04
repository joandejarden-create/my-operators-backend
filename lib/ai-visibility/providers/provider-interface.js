/**
 * Canonical AI Visibility provider adapter interface (Phase 3B.1).
 * No live provider calls in this module.
 */

/** @typedef {"openai"|"gemini"|"perplexity"|"claude"} CanonicalProviderId */

export const CANONICAL_PROVIDER_IDS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);

export const CANONICAL_PROVIDER_LABELS = Object.freeze({
  openai: "ChatGPT",
  gemini: "Gemini",
  perplexity: "Perplexity",
  claude: "Claude",
});

/**
 * Required adapter surface. Optional methods return null/empty when unsupported.
 *
 * @typedef {object} AiVisibilityProviderAdapter
 * @property {CanonicalProviderId} id
 * @property {string} vendor
 * @property {() => object} getProviderCapabilities
 * @property {(args?: object) => object} getModelIdentity
 * @property {(args?: object) => object} buildRequest
 * @property {(args?: object) => Promise<object>} execute
 * @property {(raw: object, ctx?: object) => object} normalizeResponse
 * @property {(raw: object) => object|null} extractUsage
 * @property {(raw: object, ctx?: object) => object} extractEvidence
 * @property {(err: unknown) => object} classifyError
 * @property {(usage: object|null, ctx?: object) => object} estimateCost
 */

export const ADAPTER_INTERFACE_METHODS = Object.freeze([
  "id",
  "vendor",
  "getProviderCapabilities",
  "getModelIdentity",
  "buildRequest",
  "execute",
  "normalizeResponse",
  "extractUsage",
  "extractEvidence",
  "classifyError",
  "estimateCost",
]);

/**
 * Capability flags — explicit per provider; do not assume equivalence.
 */
export function buildProviderCapabilityFlags(overrides = {}) {
  return {
    supportsWebSearch: false,
    supportsInlineCitations: false,
    supportsTopLevelCitationList: false,
    supportsSearchResultMetadata: false,
    supportsWebGroundingMetadata: false,
    supportsUsageTokens: false,
    supportsProviderCost: false,
    supportsResponseId: false,
    supportsMultilingual: true,
    supportsToolUse: false,
    ...overrides,
  };
}

/**
 * Citation capability descriptors — not quality rankings.
 */
export const CITATION_CAPABILITY_STATES = Object.freeze({
  INLINE_CITATION: "INLINE_CITATION",
  TOP_LEVEL_CITATION_LIST: "TOP_LEVEL_CITATION_LIST",
  SEARCH_RESULT_METADATA: "SEARCH_RESULT_METADATA",
  WEB_GROUNDING_METADATA: "WEB_GROUNDING_METADATA",
  NO_EQUIVALENT_FIELD: "NO_EQUIVALENT_FIELD",
});

/**
 * @param {CanonicalProviderId} providerId
 */
export function formatCanonicalProviderLabel(providerId) {
  const id = String(providerId || "").trim().toLowerCase();
  return CANONICAL_PROVIDER_LABELS[id] || id;
}

/**
 * Validate adapter implements required contract surface.
 * @param {AiVisibilityProviderAdapter} adapter
 */
export function validateProviderAdapter(adapter) {
  const errors = [];
  if (!adapter || typeof adapter !== "object") return { ok: false, errors: ["missing_adapter"] };
  for (const method of ADAPTER_INTERFACE_METHODS) {
    if (adapter[method] == null) errors.push(`missing_${method}`);
  }
  if (!CANONICAL_PROVIDER_IDS.includes(adapter.id)) {
    errors.push(`invalid_provider_id:${adapter.id}`);
  }
  return { ok: errors.length === 0, errors };
}
