import { openaiProvider } from "./openai.js";
import { geminiProvider } from "./gemini.js";
import { perplexityProvider } from "./perplexity.js";
import { claudeProvider } from "./claude.js";
import { ProviderError } from "./base-provider.js";
import { validateProviderAdapter, CANONICAL_PROVIDER_IDS } from "./provider-interface.js";

export {
  normalizeVisibilityProviderResponse,
  listNormalizedProviderContractFields,
  NORMALIZED_PROVIDER_RESPONSE_VERSION,
  NORMALIZED_PROVIDER_RESPONSE_VERSION_V1_1,
} from "./normalized-response.js";
export {
  normalizeProviderCitation,
  normalizeProviderCitations,
  listNormalizedCitationContractFields,
  NORMALIZED_CITATION_CONTRACT_VERSION,
} from "./normalized-citation.js";
export {
  CANONICAL_PROVIDER_IDS,
  CANONICAL_PROVIDER_LABELS,
  validateProviderAdapter,
  ADAPTER_INTERFACE_METHODS,
} from "./provider-interface.js";
export { PROVIDER_CAPABILITY_MATRIX, getProviderCapabilityAudit } from "./provider-capabilities.js";
export { classifyProviderError, PROVIDER_ERROR_CATEGORIES } from "./provider-errors.js";
export {
  PROVIDER_RETRY_POLICIES,
  getProviderRetryPolicy,
  OPENAI_RETRY_POLICY,
  GEMINI_RETRY_POLICY,
  PERPLEXITY_RETRY_POLICY,
  CLAUDE_RETRY_POLICY,
} from "./provider-retry-policy.js";
export {
  estimateProviderCost,
  PROVIDER_COST_CAPABILITIES,
  MODEL_VERSION_COMPARABILITY_RULE,
  COST_UNKNOWN,
} from "./provider-cost.js";
export { buildOpenAiVisibilityRequest } from "./openai.js";
export {
  buildGeminiVisibilityRequest,
  GEMINI_EVIDENCE_READY,
} from "./gemini.js";
export {
  buildPerplexityVisibilityRequest,
  PERPLEXITY_EVIDENCE_READY,
  RECOMMENDED_PERPLEXITY_EXECUTION_MODE,
  RECOMMENDED_PERPLEXITY_EXECUTION_MODE_WHY,
} from "./perplexity.js";
export {
  buildClaudeVisibilityRequest,
  CLAUDE_EVIDENCE_READY,
  CLAUDE_WEB_SEARCH_POLICY,
} from "./claude.js";
export {
  buildMultiProviderDryRunReport,
  validateFingerprintProviderIsolation,
  auditSemanticPromptParity,
  findSampleExecution,
} from "./multi-provider-dry-run.js";
export {
  buildControlledValidationPlan,
  CONTROLLED_VALIDATION_PROMPT_IDS,
  VALIDATION_COST_ESTIMATE,
  FULL_MULTIPROVIDER_BASELINE_PLAN,
  BASELINE_TIMING_RULE,
} from "./validation-plan.js";
export {
  CROSS_PROVIDER_SIGNAL_FOUNDATION_READY,
  CROSS_PROVIDER_COMPARABILITY_KEY,
  CITATION_RATE_COMPATIBILITY,
  PROVIDER_METRIC_COMPATIBILITY,
  SOURCE_ANALYSIS_FOUNDATION,
  EARLY_DISCOVERABILITY_PHASE_RETAINED,
} from "./cross-provider-signals.js";
export { auditRawTextRepair, applyRawTextRepair } from "./raw-text-repair.js";

const PROVIDERS = {
  openai: openaiProvider,
  gemini: geminiProvider,
  perplexity: perplexityProvider,
  claude: claudeProvider,
};

export { openaiProvider, geminiProvider, perplexityProvider, claudeProvider, PROVIDERS };

/**
 * @returns {string[]}
 */
export function listRegisteredProviders() {
  return [...CANONICAL_PROVIDER_IDS];
}

/**
 * @param {string} providerId
 */
export function getProviderAdapter(providerId) {
  const id = String(providerId || "").trim().toLowerCase();
  return PROVIDERS[id] || null;
}

/**
 * Build request without executing (dry-run).
 * @param {{ provider: string, prompt: object, model?: string, context?: object }} args
 */
export function buildProviderVisibilityRequest(args) {
  const provider = getProviderAdapter(args.provider);
  if (!provider?.buildRequest) {
    throw new ProviderError(`Unknown AI Visibility provider: ${args.provider}`, {
      type: "config_error",
      retryable: false,
    });
  }
  return provider.buildRequest(args);
}

/**
 * @param {{ provider?: string, prompt: object, model?: string, context?: object }} args
 */
export async function runVisibilityPrompt(args) {
  const providerId = String(args.provider || process.env.AI_VISIBILITY_PROVIDER || "openai")
    .trim()
    .toLowerCase();
  const provider = PROVIDERS[providerId];
  if (!provider) {
    throw new ProviderError(`Unknown AI Visibility provider: ${providerId}`, {
      type: "config_error",
      retryable: false,
    });
  }
  return provider.runVisibilityPrompt(args);
}

export { ProviderError } from "./base-provider.js";
