/**
 * Canonical Owner Decision Intent bridge — maps Brand + Operator scenario registries
 * without creating a parallel customer-facing taxonomy.
 */

import { loadScenarioRegistry } from "../scenario-registry.js";
import { OPERATOR_DECISION_SCENARIOS } from "../operator-intelligence/scenarios.js";

export const CANONICAL_INTENT_VERSION = "dealality_canonical_intent_v1";
export const PARALLEL_TAXONOMY_CREATED = false;

const BRAND_INTENT_FAMILY_TO_CATEGORY = Object.freeze({
  Conversion: "CONVERSION",
  "Brand Selection": "BRAND_SELECTION",
  "Collection / Soft Brand": "SOFT_BRAND_COLLECTION",
  "Owner Flexibility": "OWNER_CONTROL",
  "Owner Economics": "OWNER_ECONOMICS",
  "Lifestyle Positioning": "LIFESTYLE_POSITIONING",
  "Upper-Upscale Positioning": "POSITIONING",
  "Branded Residences": "BRANDED_RESIDENCES",
  "HMA vs Franchise": "FRANCHISE_VS_MANAGEMENT",
});

const OPERATOR_INTENT_CATEGORY = "OPERATOR_SELECTION";

/**
 * Build canonical intent ID from scenario.
 */
export function buildCanonicalIntentId(scenarioId, entityType = "BRAND") {
  const prefix = entityType === "OPERATOR" ? "ci_op" : "ci_br";
  return `${prefix}_${String(scenarioId || "").replace(/^scenario_|^op_scenario_/i, "")}`;
}

function mapBrandScenarioToIntent(scenario) {
  return Object.freeze({
    canonicalIntentId: buildCanonicalIntentId(scenario.scenarioId, "BRAND"),
    intentName: scenario.scenarioName,
    intentCategory: BRAND_INTENT_FAMILY_TO_CATEGORY[scenario.intentFamily] || "OTHER",
    decisionDomain: "BRAND",
    ownerObjective: scenario.ownerPriority || null,
    assetType: scenario.defaultDimensions?.brandFamily || null,
    marketRegion: null,
    chainScale: scenario.defaultDimensions?.keyCountBand || null,
    developmentContext: scenario.defaultDimensions?.existingAffiliation || null,
    conversionContext:
      scenario.intentFamily === "Conversion" ? "CONVERSION_REFLAG" : null,
    promptFamily: scenario.intentFamily || null,
    promptVersion: scenario.scenarioVersion || "1",
    promptMutationId: null,
    language: null,
    dateIntroduced: null,
    status: scenario.status || "ACTIVE",
    sourceOrigin: "SCENARIO",
    scenarioId: scenario.scenarioId,
    entityType: "BRAND",
    customerExposure: "CONTROLLED",
  });
}

function mapOperatorScenarioToIntent(scenario) {
  return Object.freeze({
    canonicalIntentId: buildCanonicalIntentId(scenario.scenarioId, "OPERATOR"),
    intentName: scenario.name,
    intentCategory: OPERATOR_INTENT_CATEGORY,
    decisionDomain: "OPERATOR",
    ownerObjective: scenario.ownerDecision || null,
    assetType: null,
    marketRegion: scenario.regionalScope || null,
    chainScale: null,
    developmentContext: null,
    conversionContext: null,
    promptFamily: scenario.scenarioId,
    promptVersion: "1",
    promptMutationId: null,
    language: "en",
    dateIntroduced: null,
    status: "ACTIVE",
    sourceOrigin: "SCENARIO",
    scenarioId: scenario.scenarioId,
    entityType: "OPERATOR",
    customerExposure: "CONTROLLED",
  });
}

/**
 * Load unified canonical intent index from existing registries.
 */
export function buildCanonicalIntentIndex() {
  const brandRegistry = loadScenarioRegistry();
  const brandIntents = (brandRegistry.scenarios || [])
    .filter((s) => s.status === "ACTIVE" || !s.status)
    .map(mapBrandScenarioToIntent);

  const operatorIntents = OPERATOR_DECISION_SCENARIOS.map(mapOperatorScenarioToIntent);

  const byId = new Map();
  for (const intent of [...brandIntents, ...operatorIntents]) {
    byId.set(intent.canonicalIntentId, intent);
  }

  return {
    version: CANONICAL_INTENT_VERSION,
    parallelTaxonomyCreated: PARALLEL_TAXONOMY_CREATED,
    customerExposure: "CONTROLLED",
    totalCanonicalIntents: byId.size,
    brandMappings: brandIntents.length,
    operatorMappings: operatorIntents.length,
    intents: [...byId.values()],
    byScenarioId: Object.fromEntries(
      [...byId.values()].map((i) => [i.scenarioId, i.canonicalIntentId])
    ),
  };
}

/**
 * Resolve canonical intent for a prompt row via scenarioId.
 */
export function resolveCanonicalIntent(scenarioId, entityType = "BRAND") {
  const index = buildCanonicalIntentIndex();
  const match = index.intents.find(
    (i) => i.scenarioId === scenarioId && i.entityType === entityType
  );
  return match || null;
}
