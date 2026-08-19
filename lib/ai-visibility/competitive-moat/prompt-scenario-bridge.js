/**
 * Governed prompt → scenario bridge for provider-scoped Owner Intent presence.
 * Uses promptId metadata and registry bindings only — no free-form prompt-text inference.
 */

import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
} from "../scenario-registry.js";

let cachedBridgeIndex = null;

/**
 * Enrich one observation/evidence row with governed prompt metadata before scenario resolution.
 * @param {object} row
 * @param {Map<string, object>} [promptMap]
 */
export function enrichObservationForScenarioResolution(row = {}, promptMap = null) {
  const map = promptMap || buildPromptMetadataById();
  const meta = row.promptId ? map.get(row.promptId) : null;
  return {
    ...row,
    promptFamily:
      meta?.promptFamily ||
      row.promptFamily ||
      row.PROMPT_FAMILY ||
      null,
    intentTerritory:
      meta?.intentTerritory ||
      row.intentTerritory ||
      row.PROMPT_FAMILY ||
      null,
  };
}

/**
 * Resolve governed scenario for one observation row.
 * Preference: promptId → explicit registry mapping; then internal promptFamily; no guess.
 */
export function resolveObservationScenario(row = {}, opts = {}) {
  const scenarioIndex =
    opts.scenarioIndex || buildScenarioRegistryIndex(opts.registry || loadScenarioRegistry());
  const promptMap = opts.promptMap || buildPromptMetadataById();
  const enriched = enrichObservationForScenarioResolution(row, promptMap);
  const resolved = resolvePromptScenario(
    {
      promptId: enriched.promptId,
      promptFamily: enriched.promptFamily,
      intentTerritory: enriched.intentTerritory,
    },
    scenarioIndex
  );
  return {
    ...resolved,
    promptFamily: enriched.promptFamily,
    intentTerritory: enriched.intentTerritory,
  };
}

/**
 * Audit mapping coverage for a set of observations (internal diagnostics).
 */
export function auditObservationScenarioMapping(observations = [], opts = {}) {
  const scenarioIndex =
    opts.scenarioIndex || buildScenarioRegistryIndex(opts.registry || loadScenarioRegistry());
  const promptMap = opts.promptMap || buildPromptMetadataById();
  let mapped = 0;
  let unmapped = 0;
  const unmappedSamples = [];
  for (const obs of observations || []) {
    const resolved = resolveObservationScenario(obs, { scenarioIndex, promptMap });
    if (resolved.scenarioId) {
      mapped += 1;
    } else {
      unmapped += 1;
      if (unmappedSamples.length < 5) {
        unmappedSamples.push({
          promptId: obs.promptId || null,
          promptFamily: obs.promptFamily || obs.PROMPT_FAMILY || null,
          intentTerritory: obs.intentTerritory || null,
        });
      }
    }
  }
  return {
    total: (observations || []).length,
    mapped,
    unmapped,
    unmappedSamples,
    ok: unmapped === 0,
  };
}

export function getPromptScenarioBridgeIndex(opts = {}) {
  if (cachedBridgeIndex && !opts.refresh) return cachedBridgeIndex;
  cachedBridgeIndex = buildScenarioRegistryIndex(opts.registry || loadScenarioRegistry());
  return cachedBridgeIndex;
}

export function resetPromptScenarioBridgeCache() {
  cachedBridgeIndex = null;
}
