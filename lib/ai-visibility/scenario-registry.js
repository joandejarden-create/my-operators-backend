/**
 * Owner Decision Scenario registry (Hotel Brand AI Intelligence P0A).
 * Governed sidecar — distinct from Prompt ID / Prompt Family / Semantic Pair ID.
 * No provider calls. No Airtable writes. No measurement logic changes.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { validateSemanticPairMembers } from "./semantic-pair.js";
import { validatePromptRow } from "./prompt-validation.js";
import { resolveRecordLanguage } from "./language-dimension.js";

export const SCENARIO_REGISTRY_VERSION = "ai_visibility_scenario_registry_v1";

export const COMMERCIAL_PRIORITIES = Object.freeze([
  "CRITICAL",
  "HIGH",
  "STANDARD",
  "INVESTIGATION",
]);

export const MONITORING_PANELS = Object.freeze(["CORE", "INVESTIGATION", "TRIGGERED"]);

/** Governed owner-value taxonomy — not Intent Territory and not a score. */
export const OWNER_PRIORITIES = Object.freeze([
  "Flexibility / Control",
  "Economics",
  "Distribution",
  "Loyalty",
  "Conversion Suitability",
  "Design Individuality",
  "Development Support",
  "Market Fit",
  "Positioning",
  "Branded Residences Capability",
]);

export const SCENARIO_STATUSES = Object.freeze([
  "ACTIVE",
  "PLANNED",
  "PLANNED_NO_PROMPTS",
  "RETIRED",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REGISTRY_PATH = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "scenario-registry-v1.json"
);

/** Comparable dimensions for variant validation (geo/lang/version may differ). */
const VARIANT_COMPARABLE_FIELDS = Object.freeze([
  "intentTerritory",
  "entityScope",
  "developmentType",
  "chainScale",
  "assetType",
  "hotelType",
]);

function normStr(v) {
  return v == null || v === "" ? null : String(v).trim();
}

/**
 * @param {string} [registryPath]
 */
export function loadScenarioRegistry(registryPath = DEFAULT_REGISTRY_PATH) {
  const raw = JSON.parse(fs.readFileSync(registryPath, "utf8"));
  return {
    ...raw,
    registryVersion: raw.registryVersion || SCENARIO_REGISTRY_VERSION,
    registryPath,
  };
}

/**
 * @param {object} registry
 * @returns {{ ok: boolean, errors: string[], warnings: string[] }}
 */
export function validateScenarioRegistry(registry = {}) {
  const errors = [];
  const warnings = [];
  const scenarios = registry.scenarios || [];
  const variantGroups = registry.variantGroups || [];
  const seenScenarioIds = new Set();

  for (const s of scenarios) {
    const id = normStr(s.scenarioId);
    if (!id) {
      errors.push("missing_scenario_id");
      continue;
    }
    if (seenScenarioIds.has(id)) errors.push(`duplicate_scenario_id:${id}`);
    seenScenarioIds.add(id);

    if (!normStr(s.scenarioName)) errors.push(`missing_scenario_name:${id}`);
    if (!normStr(s.intentFamily)) errors.push(`missing_intent_family:${id}`);
    if (!normStr(s.scenarioVersion)) errors.push(`missing_scenario_version:${id}`);

    if (!OWNER_PRIORITIES.includes(s.ownerPriority)) {
      errors.push(`invalid_owner_priority:${id}:${s.ownerPriority}`);
    }
    if (!COMMERCIAL_PRIORITIES.includes(s.commercialPriority)) {
      errors.push(`invalid_commercial_priority:${id}:${s.commercialPriority}`);
    }
    if (!MONITORING_PANELS.includes(s.monitoringPanel)) {
      errors.push(`invalid_monitoring_panel:${id}:${s.monitoringPanel}`);
    }
    if (s.status && !SCENARIO_STATUSES.includes(s.status)) {
      errors.push(`invalid_scenario_status:${id}:${s.status}`);
    }
  }

  const scenarioById = new Map(scenarios.map((s) => [s.scenarioId, s]));
  const seenVariantGroupIds = new Set();
  const promptFamilyToScenario = new Map();

  for (const vg of variantGroups) {
    const vgId = normStr(vg.variantGroupId);
    const scenarioId = normStr(vg.scenarioId);
    if (!vgId) {
      errors.push("missing_variant_group_id");
      continue;
    }
    if (seenVariantGroupIds.has(vgId)) errors.push(`duplicate_variant_group_id:${vgId}`);
    seenVariantGroupIds.add(vgId);
    if (!scenarioById.has(scenarioId)) {
      errors.push(`variant_group_unknown_scenario:${vgId}:${scenarioId}`);
    }
    for (const family of vg.promptFamilies || []) {
      const f = normStr(family);
      if (!f) continue;
      if (promptFamilyToScenario.has(f)) {
        errors.push(
          `prompt_family_assigned_to_conflicting_scenarios:${f}:${promptFamilyToScenario.get(f)}:${scenarioId}`
        );
      } else {
        promptFamilyToScenario.set(f, scenarioId);
      }
    }
  }

  for (const m of registry.promptMappings || []) {
    const promptId = normStr(m.promptId);
    const scenarioId = normStr(m.scenarioId);
    if (!promptId || !scenarioId) {
      errors.push("invalid_prompt_mapping_entry");
      continue;
    }
    if (!scenarioById.has(scenarioId)) {
      errors.push(`prompt_mapping_unknown_scenario:${promptId}:${scenarioId}`);
    }
  }

  const coreCount = scenarios.filter((s) => s.monitoringPanel === "CORE").length;
  if (coreCount !== 12 && registry.requireTwelveCore !== false) {
    warnings.push(`core_scenario_count:${coreCount}_expected_12`);
  }

  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Build lookup indexes from validated registry.
 * @param {object} registry
 */
export function buildScenarioRegistryIndex(registry = {}) {
  const validation = validateScenarioRegistry(registry);
  const scenarioById = new Map((registry.scenarios || []).map((s) => [s.scenarioId, s]));
  const variantGroupById = new Map(
    (registry.variantGroups || []).map((vg) => [vg.variantGroupId, vg])
  );
  const promptFamilyToBinding = new Map();
  for (const vg of registry.variantGroups || []) {
    for (const family of vg.promptFamilies || []) {
      const f = normStr(family);
      if (!f) continue;
      promptFamilyToBinding.set(f, {
        scenarioId: vg.scenarioId,
        variantGroupId: vg.variantGroupId,
      });
    }
  }
  const explicitPromptMap = new Map(
    (registry.promptMappings || []).map((m) => [
      m.promptId,
      {
        scenarioId: m.scenarioId,
        variantGroupId: m.variantGroupId || null,
      },
    ])
  );
  return {
    validation,
    scenarioById,
    variantGroupById,
    promptFamilyToBinding,
    explicitPromptMap,
  };
}

/**
 * Resolve governed scenario metadata for one prompt row.
 * @param {object} prompt
 * @param {ReturnType<typeof buildScenarioRegistryIndex>} index
 */
export function resolvePromptScenario(prompt, index) {
  const promptId = normStr(prompt?.promptId);
  const promptFamily = normStr(prompt?.promptFamily);

  let binding = null;
  if (promptId && index.explicitPromptMap.has(promptId)) {
    binding = index.explicitPromptMap.get(promptId);
  } else if (promptFamily && index.promptFamilyToBinding.has(promptFamily)) {
    binding = index.promptFamilyToBinding.get(promptFamily);
  }

  if (!binding) {
    return {
      scenarioStatus: "UNMAPPED",
      scenarioId: null,
      variantGroupId: null,
      scenarioName: null,
      intentFamily: prompt?.intentTerritory || null,
      ownerPriority: null,
      commercialPriority: null,
      monitoringPanel: null,
      scenarioVersion: null,
    };
  }

  const scenario = index.scenarioById.get(binding.scenarioId);

  let resolvedVariantGroupId = binding.variantGroupId || null;
  if (!resolvedVariantGroupId && promptFamily) {
    const vg = [...index.variantGroupById.values()].find(
      (g) =>
        g.scenarioId === binding.scenarioId &&
        (g.promptFamilies || []).includes(promptFamily)
    );
    resolvedVariantGroupId = vg?.variantGroupId || null;
  }

  return {
    scenarioStatus: "MAPPED",
    scenarioId: scenario?.scenarioId || binding.scenarioId,
    variantGroupId: resolvedVariantGroupId,
    scenarioName: scenario?.scenarioName || null,
    intentFamily: scenario?.intentFamily || prompt?.intentTerritory || null,
    ownerPriority: scenario?.ownerPriority || null,
    commercialPriority: scenario?.commercialPriority || null,
    monitoringPanel: scenario?.monitoringPanel || null,
    scenarioVersion: scenario?.scenarioVersion || null,
    promptVersion: prompt?.version || null,
  };
}

/**
 * Validate variant groups against loaded prompt rows (deterministic).
 * @param {object[]} prompts
 * @param {object} registry
 */
export function validateScenarioPromptBindings(prompts = [], registry = {}) {
  const index = buildScenarioRegistryIndex(registry);
  const errors = [...index.validation.errors];
  const warnings = [...index.validation.warnings];

  const promptsByFamily = new Map();
  for (const p of prompts) {
    const family = normStr(p.promptFamily);
    if (!family) continue;
    if (!promptsByFamily.has(family)) promptsByFamily.set(family, []);
    promptsByFamily.get(family).push(p);
  }

  const assignedPromptIds = new Map();

  for (const p of prompts) {
    const resolved = resolvePromptScenario(p, index);
    if (resolved.scenarioStatus !== "MAPPED") continue;
    const pid = p.promptId;
    if (assignedPromptIds.has(pid)) {
      if (assignedPromptIds.get(pid) !== resolved.scenarioId) {
        errors.push(`prompt_assigned_to_conflicting_scenarios:${pid}`);
      }
    } else {
      assignedPromptIds.set(pid, resolved.scenarioId);
    }
  }

  for (const vg of registry.variantGroups || []) {
    for (const family of vg.promptFamilies || []) {
      const rows = promptsByFamily.get(family) || [];
      if (!rows.length) {
        warnings.push(`variant_group_no_prompts:${vg.variantGroupId}:${family}`);
        continue;
      }
      const mismatch = validateVariantDimensionalParity(rows, vg.variantGroupId);
      errors.push(...mismatch.errors);
      warnings.push(...mismatch.warnings);

      const bias = validateVariantBiasParity(rows, vg.variantGroupId);
      errors.push(...bias.errors);
    }
  }

  const pairErrors = validateScenarioSemanticPairs(prompts);
  errors.push(...pairErrors);

  return { ok: errors.length === 0, errors, warnings, index };
}

function validateVariantDimensionalParity(rows, variantGroupId) {
  const errors = [];
  const warnings = [];
  const baseline = rows[0];
  for (const field of VARIANT_COMPARABLE_FIELDS) {
    const baseVal = normStr(baseline[field]);
    if (!baseVal) continue;
    for (const row of rows.slice(1)) {
      const val = normStr(row[field]);
      if (val && val !== baseVal) {
        errors.push(
          `variant_dimensional_mismatch:${variantGroupId}:${field}:${baseline.promptId}:${row.promptId}`
        );
      }
    }
  }
  return { errors, warnings };
}

function validateVariantBiasParity(rows, variantGroupId) {
  const errors = [];
  for (const row of rows) {
    const v = validatePromptRow(row);
    for (const e of v.errors) {
      if (String(e).startsWith("non_neutral_wording")) {
        errors.push(`variant_brand_bias:${variantGroupId}:${row.promptId}:${e}`);
      }
    }
  }
  return { errors };
}

function validateScenarioSemanticPairs(prompts) {
  const errors = [];
  const byPairId = new Map();
  for (const p of prompts) {
    const pairId = normStr(p.semanticPairId);
    if (!pairId) continue;
    const lang = resolveRecordLanguage(p, { treatMissingAsEn: true });
    if (!byPairId.has(pairId)) byPairId.set(pairId, {});
    byPairId.get(pairId)[lang] = p;
  }
  for (const [pairId, members] of byPairId) {
    if (members.en && members.es) {
      const result = validateSemanticPairMembers(members.en, members.es);
      if (!result.ok) {
        errors.push(`invalid_semantic_pair:${pairId}:${result.errors.join(",")}`);
      }
    }
  }
  return errors;
}

/**
 * Coverage audit for registry vs prompt corpus.
 * @param {object[]} prompts
 * @param {object} registry
 */
export function auditScenarioPromptCoverage(prompts = [], registry = {}) {
  const index = buildScenarioRegistryIndex(registry);
  const byScenario = new Map();

  for (const s of registry.scenarios || []) {
    byScenario.set(s.scenarioId, {
      scenarioId: s.scenarioId,
      scenarioName: s.scenarioName,
      monitoringPanel: s.monitoringPanel,
      status: s.status || "ACTIVE",
      matchingPromptIds: [],
      languages: new Set(),
      geographies: new Set(),
      variantGroups: new Set(),
    });
  }

  let mapped = 0;
  let unmapped = 0;

  for (const p of prompts) {
    const resolved = resolvePromptScenario(p, index);
    if (resolved.scenarioStatus === "UNMAPPED") {
      unmapped += 1;
      continue;
    }
    mapped += 1;
    const row = byScenario.get(resolved.scenarioId);
    if (!row) continue;
    row.matchingPromptIds.push(p.promptId);
    row.languages.add(resolveRecordLanguage(p, { treatMissingAsEn: true }));
    if (p.geographyScope === "Global") row.geographies.add("Global");
    else if (p.commercialRegion) row.geographies.add(p.commercialRegion);
    else if (p.country) row.geographies.add(p.country);
    if (resolved.variantGroupId) row.variantGroups.add(resolved.variantGroupId);
  }

  const scenarios = [...byScenario.values()].map((r) => ({
    scenarioId: r.scenarioId,
    scenarioName: r.scenarioName,
    monitoringPanel: r.monitoringPanel,
    status: r.status,
    existingPromptCount: r.matchingPromptIds.length,
    languages: [...r.languages].sort(),
    geographies: [...r.geographies].sort(),
    variantGroupCount: r.variantGroups.size,
    coverage:
      r.matchingPromptIds.length > 0
        ? "MAPPED"
        : r.status === "PLANNED_NO_PROMPTS"
          ? "PLANNED"
          : "GAP",
  }));

  const total = prompts.length;
  const reusePercent =
    total > 0 ? Math.round((mapped / total) * 1000) / 10 : 0;

  return {
    mappedPrompts: mapped,
    unmappedPrompts: unmapped,
    totalPrompts: total,
    reusePercent,
    scenarios,
  };
}

export { DEFAULT_REGISTRY_PATH };
