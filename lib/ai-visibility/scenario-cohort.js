/**
 * Optional scenario metadata enrichment for prompt cohorts (P0A).
 * Does not alter cohort fingerprint, filters, or certified metric denominators.
 */

import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
  SCENARIO_REGISTRY_VERSION,
} from "./scenario-registry.js";

export const SCENARIO_COHORT_METADATA_VERSION = "ai_visibility_scenario_cohort_metadata_v1";

function promptByIdMap(prompts = []) {
  const map = new Map();
  for (const p of prompts) {
    if (p?.promptId) map.set(p.promptId, p);
  }
  return map;
}

/**
 * Attach scenario metadata to an existing cohort result without changing metric inputs.
 * @param {object} cohort result from buildPromptCohort
 * @param {object[]} prompts full or partial prompt rows keyed by promptId
 * @param {{ registry?: object, index?: object }} [options]
 */
export function enrichPromptCohortWithScenarioMetadata(cohort, prompts = [], options = {}) {
  if (!cohort || cohort.ok === false) {
    return {
      ...cohort,
      scenarioMetadataVersion: SCENARIO_COHORT_METADATA_VERSION,
      scenarioRegistryVersion: SCENARIO_REGISTRY_VERSION,
      scenarioSummary: null,
    };
  }

  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const index = options.index || buildScenarioRegistryIndex(registry);
  const byId = promptByIdMap(prompts);

  const members = (cohort.members || []).map((member) => {
    const prompt = byId.get(member.promptId) || member;
    const scenario = resolvePromptScenario(prompt, index);
    return {
      ...member,
      scenarioId: scenario.scenarioId,
      scenarioStatus: scenario.scenarioStatus,
      variantGroupId: scenario.variantGroupId,
      ownerPriority: scenario.ownerPriority,
      commercialPriority: scenario.commercialPriority,
      monitoringPanel: scenario.monitoringPanel,
    };
  });

  const mapped = members.filter((m) => m.scenarioStatus === "MAPPED");
  const unmapped = members.filter((m) => m.scenarioStatus === "UNMAPPED");
  const scenarioIds = [...new Set(mapped.map((m) => m.scenarioId).filter(Boolean))];

  return {
    ...cohort,
    members,
    scenarioMetadataVersion: SCENARIO_COHORT_METADATA_VERSION,
    scenarioRegistryVersion: registry.registryVersion || SCENARIO_REGISTRY_VERSION,
    scenarioSummary: {
      mappedCount: mapped.length,
      unmappedCount: unmapped.length,
      distinctScenarioIds: scenarioIds,
      scenarioIds,
    },
  };
}
