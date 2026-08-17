/**
 * Provider-neutral cost accounting foundation (Phase 3B.1).
 * Do not hard-code prices into metric logic.
 */

export const COST_LEDGER_VERSION = "ai_visibility_provider_cost_v1";

export const COST_UNKNOWN = "COST_UNKNOWN";

/**
 * Versioned pricing config placeholder — populate from official pricing docs / live calibration.
 * Values are USD estimates per 1M tokens unless noted.
 */
export const PROVIDER_PRICING_CONFIG_V1 = Object.freeze({
  version: "ai_visibility_provider_pricing_v1",
  openai: {
    model: "gpt-5.6",
    inputPer1M: null,
    outputPer1M: null,
    searchPerCall: null,
    source: "wave1_live_calibration",
    calibratedFromWave1: { perCallExpected: 0.4572, calls: 84, totalUsd: 38.405555 },
  },
  gemini: {
    model: "gemini-2.5-flash",
    inputPer1M: null,
    outputPer1M: null,
    searchPerCall: null,
    source: "needs_live_usage_calibration",
  },
  perplexity: {
    model: "sonar",
    inputPer1M: null,
    outputPer1M: null,
    requestPer1K: null,
    source: "needs_live_usage_calibration",
  },
  claude: {
    model: "claude-sonnet-4-6",
    inputPer1M: null,
    outputPer1M: null,
    searchPerCall: null,
    source: "needs_live_usage_calibration",
  },
});

/** Per-provider cost capability audit. */
export const PROVIDER_COST_CAPABILITIES = Object.freeze({
  openai: {
    TOKEN_USAGE_AVAILABLE: true,
    SEARCH_USAGE_AVAILABLE: false,
    PROVIDER_COST_RETURNED: false,
    LOCAL_COST_CALC_REQUIRED: true,
    PRICE_CONFIG_REQUIRED: true,
  },
  gemini: {
    TOKEN_USAGE_AVAILABLE: true,
    SEARCH_USAGE_AVAILABLE: false,
    PROVIDER_COST_RETURNED: false,
    LOCAL_COST_CALC_REQUIRED: true,
    PRICE_CONFIG_REQUIRED: true,
  },
  perplexity: {
    TOKEN_USAGE_AVAILABLE: true,
    SEARCH_USAGE_AVAILABLE: false,
    PROVIDER_COST_RETURNED: true,
    LOCAL_COST_CALC_REQUIRED: false,
    PRICE_CONFIG_REQUIRED: false,
  },
  claude: {
    TOKEN_USAGE_AVAILABLE: true,
    SEARCH_USAGE_AVAILABLE: false,
    PROVIDER_COST_RETURNED: false,
    LOCAL_COST_CALC_REQUIRED: true,
    PRICE_CONFIG_REQUIRED: true,
  },
});

/**
 * Estimate cost from usage when pricing config is available.
 * Returns COST_UNKNOWN when insufficient data.
 *
 * @param {string} providerId
 * @param {object|null} usage
 * @param {object} [options]
 */
export function estimateProviderCost(providerId, usage, options = {}) {
  const id = String(providerId || "").toLowerCase();
  const caps = PROVIDER_COST_CAPABILITIES[id];
  if (!caps) {
    return { provider: id, amountUsd: null, status: COST_UNKNOWN, reason: "unknown_provider" };
  }

  if (caps.PROVIDER_COST_RETURNED && usage?.providerCostUsd != null) {
    return {
      provider: id,
      amountUsd: Number(usage.providerCostUsd),
      status: "PROVIDER_RETURNED",
      pricingVersion: PROVIDER_PRICING_CONFIG_V1.version,
    };
  }

  const pricing = options.pricingConfig?.[id] || PROVIDER_PRICING_CONFIG_V1[id];
  if (!pricing || (pricing.inputPer1M == null && pricing.outputPer1M == null)) {
    return {
      provider: id,
      amountUsd: null,
      status: COST_UNKNOWN,
      reason: "NEEDS_LIVE_USAGE_CALIBRATION",
      pricingVersion: PROVIDER_PRICING_CONFIG_V1.version,
    };
  }

  const inputTokens = Number(usage?.inputTokens || 0);
  const outputTokens = Number(usage?.outputTokens || 0);
  const amountUsd =
    (inputTokens / 1_000_000) * (pricing.inputPer1M || 0) +
    (outputTokens / 1_000_000) * (pricing.outputPer1M || 0) +
    (pricing.searchPerCall || 0);

  return {
    provider: id,
    amountUsd: Number.isFinite(amountUsd) ? Number(amountUsd.toFixed(6)) : null,
    status: amountUsd > 0 ? "ESTIMATED" : COST_UNKNOWN,
    pricingVersion: PROVIDER_PRICING_CONFIG_V1.version,
    usage: { inputTokens, outputTokens },
  };
}

/**
 * Model version comparability rule — distinct analytical execution identity per model change.
 */
export const MODEL_VERSION_COMPARABILITY_RULE = Object.freeze({
  rule:
    "Store exact providerModel identifier returned/requested. Model generation changes create a distinct analytical execution identity; do not silently compare different model generations as identical longitudinal series.",
  fields: ["provider", "providerModel", "providerVendor", "metricVersion", "peerSetVersion", "promptVersion"],
  waveIdentity: "provider-wave ID + model + startedAt/completedAt",
});
