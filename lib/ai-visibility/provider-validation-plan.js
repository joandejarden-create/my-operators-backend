/**
 * Build governed 12-prompt validation execution plan (Phase 3B.2).
 */

import {
  CONTROLLED_VALIDATION_PROMPT_IDS,
  VALIDATION_PROMPTS_PER_PROVIDER,
} from "./providers/validation-plan.js";
import {
  loadWave1ShowcasePrompts,
  buildWave1ExecutionFingerprint,
  WAVE1_PEER_SET_ID,
  geographyKeyFromPrompt,
} from "./wave1-showcase-plan.js";
import { METRIC_VERSION } from "./config.js";

export const PROVIDER_VALIDATION_PLAN_VERSION = "ai_visibility_provider_validation_plan_v1";

const DEFAULT_MODELS = Object.freeze({
  gemini: "gemini-3.6-flash",
  perplexity: "sonar",
  claude: "claude-sonnet-4-6",
});

/**
 * @param {object} [options]
 */
export function buildProviderValidationExecutionPlan(options = {}) {
  const { prompts } = loadWave1ShowcasePrompts(options.seedPath);
  const byId = new Map(prompts.map((p) => [p.promptId, p]));
  const errors = [];
  const executions = [];

  for (const promptId of CONTROLLED_VALIDATION_PROMPT_IDS) {
    const p = byId.get(promptId);
    if (!p) {
      errors.push(`missing_prompt:${promptId}`);
      continue;
    }
    const geographyKey = geographyKeyFromPrompt(p);
    executions.push({
      promptId: p.promptId,
      version: p.version,
      language: p.language,
      intent: p.intentTerritory,
      promptFamily: p.promptFamily,
      geographyKey,
      semanticPairId: p.semanticPairId || null,
      peerSet: WAVE1_PEER_SET_ID,
      peerSetVersion: "2",
      metricVersion: METRIC_VERSION,
      promptText: p.promptText,
      geographyScope: p.geographyScope,
      commercialRegion: p.commercialRegion || null,
      country: p.country || null,
    });
  }

  if (executions.length !== VALIDATION_PROMPTS_PER_PROVIDER) {
    errors.push(`execution_count_${executions.length}_expected_${VALIDATION_PROMPTS_PER_PROVIDER}`);
  }

  return {
    planVersion: PROVIDER_VALIDATION_PLAN_VERSION,
    ok: errors.length === 0,
    errors,
    promptIds: [...CONTROLLED_VALIDATION_PROMPT_IDS],
    EXECUTIONS: executions,
    DEFAULT_MODELS,
  };
}

/**
 * Attach provider-specific fingerprint to each execution row.
 */
export function attachProviderFingerprints(executions, provider) {
  return executions.map((exec) => {
    const fp = buildWave1ExecutionFingerprint({
      provider,
      promptId: exec.promptId,
      promptVersion: exec.version,
      semanticPairId: exec.semanticPairId,
      geographyKey: exec.geographyKey,
      language: exec.language,
      intent: exec.intent,
      promptFamily: exec.promptFamily,
      peerSetId: WAVE1_PEER_SET_ID,
      peerSetVersion: "2",
      metricVersion: METRIC_VERSION,
    });
    return { ...exec, provider, fingerprint: fp.fingerprint, fingerprintFields: fp };
  });
}
