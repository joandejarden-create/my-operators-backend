/**
 * Provider-neutral normalized visibility response contract (Phase 3A.10).
 * Coexists with provider-specific raw payloads. No provider calls.
 */

export const NORMALIZED_PROVIDER_RESPONSE_VERSION = "ai_visibility_normalized_provider_response_v1";
/** Additive extension for multi-provider (Phase 3B.1). Wave-1 OpenAI artifacts remain v1. */
export const NORMALIZED_PROVIDER_RESPONSE_VERSION_V1_1 =
  "ai_visibility_normalized_provider_response_v1_1";

/**
 * @typedef {object} NormalizedVisibilityProviderResponse
 * @property {string} contractVersion
 * @property {string} provider
 * @property {string} providerModel
 * @property {string|null} providerResponseId
 * @property {string} promptId
 * @property {string|number|null} promptVersion
 * @property {string|null} geography
 * @property {string|null} language
 * @property {string|null} intent
 * @property {string|null} peerSetVersion
 * @property {string} rawText
 * @property {object|null} structuredResponse
 * @property {object[]} citations
 * @property {object|null} usage
 * @property {number|null} latencyMs
 * @property {"completed"|"failed"|"partial"} status
 * @property {object|null} error
 * @property {string|null} rawArtifactUri
 * @property {object|null} raw
 * @property {object|null} providerMeta
 */

/**
 * Map adapter output + run context into the normalized contract.
 * Never strips `raw` — OpenAI-specific payload remains available.
 *
 * @param {object} adapterResult
 * @param {object} [ctx]
 * @returns {NormalizedVisibilityProviderResponse}
 */
export function normalizeVisibilityProviderResponse(adapterResult = {}, ctx = {}) {
  const status =
    adapterResult.error || ctx.error
      ? "failed"
      : adapterResult.text || adapterResult.rawText
        ? "completed"
        : "partial";

  const contractVersion =
    ctx.contractVersion ||
    (ctx.useV1_1 ? NORMALIZED_PROVIDER_RESPONSE_VERSION_V1_1 : NORMALIZED_PROVIDER_RESPONSE_VERSION);

  return {
    contractVersion,
    provider: adapterResult.provider || ctx.provider || null,
    providerVendor: adapterResult.providerVendor || ctx.providerVendor || null,
    providerModel: adapterResult.model || ctx.model || null,
    providerResponseId:
      adapterResult.providerMeta?.responseId ||
      adapterResult.raw?.id ||
      ctx.providerResponseId ||
      null,
    promptId: ctx.promptId || adapterResult.promptId || null,
    promptVersion: ctx.promptVersion ?? ctx.version ?? null,
    promptFamily: ctx.promptFamily || null,
    semanticPairId: ctx.semanticPairId || null,
    geography: ctx.geography || ctx.geographyKey || null,
    language: ctx.language || null,
    intent: ctx.intent || ctx.intentTerritory || null,
    peerSetId: ctx.peerSetId || null,
    peerSetVersion: ctx.peerSetVersion || null,
    metricVersion: ctx.metricVersion || null,
    rawText: adapterResult.text || adapterResult.rawText || "",
    structuredResponse: adapterResult.structuredResponse || null,
    citations: Array.isArray(adapterResult.citations) ? adapterResult.citations : [],
    searchResults: Array.isArray(adapterResult.searchResults) ? adapterResult.searchResults : null,
    usage: adapterResult.usage || null,
    providerCost: adapterResult.providerCost || ctx.providerCost || null,
    latencyMs: adapterResult.latencyMs ?? null,
    status,
    error: adapterResult.error || ctx.error || null,
    rawArtifactUri: ctx.rawArtifactUri || null,
    raw: adapterResult.raw ?? null,
    providerMeta: adapterResult.providerMeta || null,
    providerCapabilities: adapterResult.providerCapabilities || ctx.providerCapabilities || null,
    searchMetadata: adapterResult.searchMetadata || null,
    finishReason: adapterResult.finishReason || adapterResult.stopReason || null,
    stopReason: adapterResult.stopReason || adapterResult.finishReason || null,
    toolUsage: adapterResult.toolUsage || null,
    providerSpecificMetadata: adapterResult.providerSpecificMetadata || null,
    citationCapability: adapterResult.citationCapability || null,
    parserVersion: adapterResult.parserVersion || null,
  };
}

/**
 * Required contract field checklist for portability audits.
 */
export function listNormalizedProviderContractFields(version = "v1") {
  const base = [
    "provider",
    "providerModel",
    "providerResponseId",
    "promptId",
    "promptVersion",
    "geography",
    "language",
    "intent",
    "peerSetVersion",
    "rawText",
    "structuredResponse",
    "citations",
    "usage",
    "latencyMs",
    "status",
    "error",
    "rawArtifactUri",
    "raw",
  ];
  if (version === "v1_1" || version === "v1.1") {
    return [
      ...base.slice(0, 1),
      "providerVendor",
      ...base.slice(1, 6),
      "promptFamily",
      "semanticPairId",
      "peerSetId",
      "metricVersion",
      ...base.slice(10, 14),
      "searchResults",
      "providerCost",
      ...base.slice(14),
      "providerCapabilities",
      "searchMetadata",
      "finishReason",
      "toolUsage",
      "providerSpecificMetadata",
    ];
  }
  return base;
}
