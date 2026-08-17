/**
 * Period-to-period comparability keys (Phase 3B.6).
 * Semantic logical fingerprint excludes period; comparability key includes model + execution config.
 */

import { METRIC_VERSION } from "./config.js";
import { PEER_SET_ID_V2 } from "./peer-sets.js";
import { finalizeClaudeWebSearchTool } from "./providers/claude-tool-audit.js";
import { TREND_COMPARABILITY_VERSION } from "./trend-comparability.js";

export const RECURRING_COMPARABILITY_VERSION = "ai_visibility_recurring_comparability_v1";

export const MODEL_CHANGE_GOVERNANCE = Object.freeze({
  READY: true,
  RULE:
    "Model or material tool-config change closes prior series; new validation + new series required; no silent substitution",
});

/**
 * Provider execution config fingerprints for comparability.
 */
export function buildProviderExecutionConfigFingerprint(provider) {
  const id = String(provider || "").toLowerCase();
  const claudeTool = finalizeClaudeWebSearchTool();

  const configs = {
    openai: {
      webSearch: true,
      configVersion: "openai_web_search_v1",
    },
    gemini: {
      grounding: "google_search",
      configVersion: "gemini_google_search_grounding_v1",
    },
    perplexity: {
      mode: "sonar",
      configVersion: "perplexity_sonar_v1",
    },
    claude: {
      webSearchTool: claudeTool.SELECTED_TOOL_VERSION,
      allowedCallers: claudeTool.SELECTED_ALLOWED_CALLERS,
      maxUses: claudeTool.MAX_USES,
      timeoutMs: claudeTool.TIMEOUT_MS,
      configVersion: `claude_${claudeTool.SELECTED_TOOL_VERSION}_direct_${claudeTool.MAX_USES}`,
    },
  };

  return configs[id] || { configVersion: "unknown" };
}

/**
 * Full comparability key for period-to-period metric comparison.
 */
export function buildPeriodComparabilityKey(parts = {}) {
  const provider = String(parts.provider || "").toLowerCase();
  return {
    version: RECURRING_COMPARABILITY_VERSION,
    trendComparabilityVersion: TREND_COMPARABILITY_VERSION,
    provider,
    providerModel: parts.providerModel || parts.model || null,
    promptId: parts.promptId || null,
    promptVersion: parts.promptVersion != null ? String(parts.promptVersion) : null,
    promptFamily: parts.promptFamily || null,
    geographyKey: parts.geographyKey || parts.geography || null,
    language: parts.language || null,
    intent: parts.intent || parts.intentTerritory || null,
    semanticPairId: parts.semanticPairId || null,
    peerSetId: parts.peerSetId || PEER_SET_ID_V2,
    peerSetVersion: parts.peerSetVersion != null ? String(parts.peerSetVersion) : "2",
    metricVersion: parts.metricVersion || METRIC_VERSION,
    entityUniverseVersion: parts.entityUniverseVersion || "peer_v2_15",
    executionConfig: buildProviderExecutionConfigFingerprint(provider),
  };
}

export function comparabilityKeyString(key) {
  const k = buildPeriodComparabilityKey(key);
  return [
    k.provider,
    k.providerModel,
    k.promptId,
    k.promptVersion,
    k.promptFamily,
    k.geographyKey,
    k.language,
    k.intent,
    k.semanticPairId,
    k.peerSetId,
    k.peerSetVersion,
    k.metricVersion,
    k.executionConfig?.configVersion || "",
  ].join("|");
}

/**
 * @returns {{ comparable: boolean, reasonCode: string|null }}
 */
export function comparePeriodObservations(current, prior) {
  const curKey = buildPeriodComparabilityKey(current);
  const priKey = buildPeriodComparabilityKey(prior);

  if (curKey.providerModel !== priKey.providerModel) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_MODEL" };
  }
  if (curKey.executionConfig?.configVersion !== priKey.executionConfig?.configVersion) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_EXECUTION_CONFIG" };
  }
  if (curKey.promptVersion !== priKey.promptVersion) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_PROMPT_VERSION" };
  }
  if (curKey.peerSetVersion !== priKey.peerSetVersion) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_PEER_SET" };
  }
  if (curKey.metricVersion !== priKey.metricVersion) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_METRIC_VERSION" };
  }
  if (curKey.language !== priKey.language) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_LANGUAGE" };
  }
  if (curKey.geographyKey !== priKey.geographyKey) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_GEOGRAPHY" };
  }
  if (curKey.promptId !== priKey.promptId) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_PROMPT" };
  }
  return { comparable: true, reasonCode: null };
}

export const FULL_PERIOD_COMPARABILITY_RULE = Object.freeze({
  RULE: "Both provider-periods must have 84/84 successful governed observations for full-period metric comparison",
  PARTIAL_NOT_COMPARABLE_AS_FULL: true,
  METRIC_DENOMINATOR: 84,
});

export const PROVIDER_EXECUTION_CONFIG_VERSIONING_REQUIRED = Object.freeze({
  REQUIRED: "YES",
  DESIGN:
    "Include providerModel + executionConfig.configVersion in comparability key; material tool/search changes break series continuity",
});
