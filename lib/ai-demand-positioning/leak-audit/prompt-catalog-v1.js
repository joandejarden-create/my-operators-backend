/**
 * Limited free-audit prompt catalog — INTERNAL ONLY (ADP Lite).
 * promptSetVersion: leak_audit_lite_v1
 * Exactly 15 traveler scenarios × N providers = observations (default 15×4=60).
 * Client reports may show promptLabel + promptIntentSummary, never fullPromptText.
 */

import { territoryLabel } from "./territory-select-v1.js";
import {
  LEAK_AUDIT_LITE_COST_CONTROLS,
  PROMPT_SET_VERSION_LITE,
} from "./research-mode-lite-v1.js";

/**
 * 15 scenarios across 6 territories (2–3 each).
 * Categories: general stay · demand-specific · competitor/market · use-case fit · attribute check
 */
const LITE_SCENARIOS = Object.freeze([
  {
    territory: "leisure",
    promptId: "lite_leisure_general_01",
    promptLabel: "Leisure stay discovery",
    category: "general_stay",
    promptIntentSummary: "Leisure traveler asking where to stay in this market",
    fullPromptText: "INTERNAL — lite leisure general (not for client export)",
  },
  {
    territory: "leisure",
    promptId: "lite_leisure_demand_02",
    promptLabel: "Leisure beach / resort fit",
    category: "demand_specific",
    promptIntentSummary: "Leisure traveler seeking a beach or resort stay",
    fullPromptText: "INTERNAL — lite leisure demand (not for client export)",
  },
  {
    territory: "leisure",
    promptId: "lite_leisure_comp_03",
    promptLabel: "Leisure market recommendation",
    category: "competitor_comparison",
    promptIntentSummary: "Asking which hotels to consider for a leisure trip",
    fullPromptText: "INTERNAL — lite leisure competitor (not for client export)",
  },
  {
    territory: "couples",
    promptId: "lite_couples_general_01",
    promptLabel: "Romantic getaway discovery",
    category: "general_stay",
    promptIntentSummary: "Couples looking for a romantic hotel stay",
    fullPromptText: "INTERNAL — lite couples general (not for client export)",
  },
  {
    territory: "couples",
    promptId: "lite_couples_usecase_02",
    promptLabel: "Anniversary / couples use-case",
    category: "use_case_fit",
    promptIntentSummary: "Couples planning an anniversary or romantic escape",
    fullPromptText: "INTERNAL — lite couples use-case (not for client export)",
  },
  {
    territory: "couples",
    promptId: "lite_couples_attr_03",
    promptLabel: "Couples privacy / amenity check",
    category: "attribute_check",
    promptIntentSummary: "Checking whether private cottages or romantic amenities are mentioned",
    fullPromptText: "INTERNAL — lite couples attribute (not for client export)",
  },
  {
    territory: "business",
    promptId: "lite_business_general_01",
    promptLabel: "Business travel discovery",
    category: "general_stay",
    promptIntentSummary: "Business traveler asking for hotel recommendations",
    fullPromptText: "INTERNAL — lite business general (not for client export)",
  },
  {
    territory: "business",
    promptId: "lite_business_usecase_02",
    promptLabel: "Business stay use-case",
    category: "use_case_fit",
    promptIntentSummary: "Business traveler needing a productive stay with workspace",
    fullPromptText: "INTERNAL — lite business use-case (not for client export)",
  },
  {
    territory: "meetings_groups",
    promptId: "lite_meetings_general_01",
    promptLabel: "Meetings and retreats discovery",
    category: "general_stay",
    promptIntentSummary: "Planner asking for meeting or retreat hotel options",
    fullPromptText: "INTERNAL — lite meetings general (not for client export)",
  },
  {
    territory: "meetings_groups",
    promptId: "lite_meetings_demand_02",
    promptLabel: "Group / meetings demand fit",
    category: "demand_specific",
    promptIntentSummary: "Group organizer seeking meeting space and lodging together",
    fullPromptText: "INTERNAL — lite meetings demand (not for client export)",
  },
  {
    territory: "meetings_groups",
    promptId: "lite_meetings_comp_03",
    promptLabel: "Meetings competitor comparison",
    category: "competitor_comparison",
    promptIntentSummary: "Comparing hotels for a small corporate retreat",
    fullPromptText: "INTERNAL — lite meetings competitor (not for client export)",
  },
  {
    territory: "wellness",
    promptId: "lite_wellness_demand_01",
    promptLabel: "Wellness stay discovery",
    category: "demand_specific",
    promptIntentSummary: "Traveler seeking wellness-oriented lodging",
    fullPromptText: "INTERNAL — lite wellness demand (not for client export)",
  },
  {
    territory: "wellness",
    promptId: "lite_wellness_attr_02",
    promptLabel: "Spa / wellness attribute check",
    category: "attribute_check",
    promptIntentSummary: "Checking whether spa or wellness amenities are reflected",
    fullPromptText: "INTERNAL — lite wellness attribute (not for client export)",
  },
  {
    territory: "family",
    promptId: "lite_family_general_01",
    promptLabel: "Family travel discovery",
    category: "general_stay",
    promptIntentSummary: "Family looking for a suitable hotel",
    fullPromptText: "INTERNAL — lite family general (not for client export)",
  },
  {
    territory: "celebration",
    promptId: "lite_celebration_usecase_01",
    promptLabel: "Celebration / wedding stay",
    category: "use_case_fit",
    promptIntentSummary: "Guest planning a celebration or wedding stay",
    fullPromptText: "INTERNAL — lite celebration use-case (not for client export)",
  },
]);

export const LITE_SCENARIO_COUNT = LITE_SCENARIOS.length;

/**
 * Build observation plan for ADP Lite.
 * Caps scenarios and providers to cost controls (default 15 × 4 = 60).
 */
export function buildLitePromptPlan(territoryKeys, providers, options = {}) {
  const maxScenarios =
    options.maxScenarios != null
      ? Number(options.maxScenarios)
      : LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios;
  const maxProviders =
    options.maxProviders != null
      ? Number(options.maxProviders)
      : LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders;
  const maxObservations =
    options.maxObservations != null
      ? Number(options.maxObservations)
      : LEAK_AUDIT_LITE_COST_CONTROLS.maxObservations;

  const allowedTerritories = new Set(
    Array.isArray(territoryKeys) && territoryKeys.length
      ? territoryKeys
      : LITE_SCENARIOS.map((s) => s.territory)
  );

  let scenarios = LITE_SCENARIOS.filter((s) => allowedTerritories.has(s.territory));
  // If territory filter shrinks below target, backfill from full lite set
  if (scenarios.length < maxScenarios) {
    for (const s of LITE_SCENARIOS) {
      if (scenarios.length >= maxScenarios) break;
      if (!scenarios.find((x) => x.promptId === s.promptId)) scenarios.push(s);
    }
  }
  scenarios = scenarios.slice(0, maxScenarios);

  const providerList = (providers || []).slice(0, maxProviders);
  const plan = [];
  for (const prompt of scenarios) {
    for (const provider of providerList) {
      if (plan.length >= maxObservations) break;
      plan.push({
        provider,
        demandTerritory: prompt.territory,
        demandTerritoryLabel: territoryLabel(prompt.territory),
        promptId: prompt.promptId,
        promptLabel: prompt.promptLabel,
        promptIntentSummary: prompt.promptIntentSummary,
        scenarioCategory: prompt.category,
        promptSetVersion: PROMPT_SET_VERSION_LITE,
        _internalFullPromptText: prompt.fullPromptText,
      });
    }
    if (plan.length >= maxObservations) break;
  }

  return {
    plan,
    scenariosRun: scenarios.length,
    providersRun: providerList.length,
    observationsPlanned: plan.length,
    promptSetVersion: PROMPT_SET_VERSION_LITE,
  };
}

/** @deprecated Prefer buildLitePromptPlan for free audits. Kept for older callers. */
export function buildLimitedPromptPlan(territoryKeys, providers) {
  return buildLitePromptPlan(territoryKeys, providers).plan;
}

export function toClientSafePromptFields(planItem) {
  return {
    promptLabel: planItem.promptLabel,
    promptIntentSummary: planItem.promptIntentSummary,
    demandTerritory: planItem.demandTerritory,
    provider: planItem.provider,
    // promptId intentionally omitted from client-safe helper used in evidence paths
  };
}

export function assertNoFullPromptLibrary(planOrModuleSource) {
  const text =
    typeof planOrModuleSource === "string"
      ? planOrModuleSource
      : JSON.stringify(planOrModuleSource || {});
  const hits = [];
  if (/adp_full_prompt|full_adp_prompt_library|monthly_prompt_set_v/i.test(text)) {
    hits.push("full_adp_prompt_library_reference");
  }
  if (/promptSetVersion["']?\s*:\s*["']adp_pilot/i.test(text)) {
    hits.push("pilot_prompt_set_in_lite_plan");
  }
  return { ok: hits.length === 0, hits };
}
