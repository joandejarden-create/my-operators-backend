/**
 * Operator AI V1 prompt library — SCENARIO origin only.
 * OBSERVED=0 DERIVED=0. No DataForSEO. Shared prompt × provider grain.
 */

import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";

export const OPERATOR_PROMPT_LIBRARY_VERSION = "operator_ai_prompt_library_v1";
export const OPERATOR_PROMPT_ORIGIN = "SCENARIO";

const GEO = Object.freeze({
  region: "CALA",
  language: "en",
  geographyScope: "Region",
});

function p(partial) {
  return Object.freeze({
    origin: OPERATOR_PROMPT_ORIGIN,
    geography: GEO.region,
    language: GEO.language,
    geographyScope: GEO.geographyScope,
    entityScope: "Operator",
    intentTerritory: "Operator Selection",
    seedOperatorNames: false,
    ...partial,
  });
}

/**
 * 30 prompts: 12 CORE (one per scenario) + 18 EXTENDED.
 * Wording is owner/developer decision language. No ranked-recommendation asks.
 */
export const OPERATOR_PROMPTS_V1 = Object.freeze([
  p({
    promptId: "op_p_core_uu_full_service_cala_en_v1",
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "What hotel management companies are commonly considered by owners for an upper-upscale full-service hotel in Latin America?",
  }),
  p({
    promptId: "op_p_core_luxury_cala_en_v1",
    scenarioId: "op_scenario_luxury_operator_selection_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which operators are commonly considered when an owner is deciding who should operate a luxury hotel in the Caribbean and Latin America?",
  }),
  p({
    promptId: "op_p_core_lifestyle_boutique_cala_en_v1",
    scenarioId: "op_scenario_lifestyle_boutique_operator_selection_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which hotel operators are commonly considered for an independent lifestyle or boutique hotel where the owner wants a professional operating partner?",
  }),
  p({
    promptId: "op_p_core_owner_control_en_v1",
    scenarioId: "op_scenario_owner_control_flexibility_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which hotel management companies are commonly considered by owners who want greater strategic and operational control than a typical brand-managed path?",
  }),
  p({
    promptId: "op_p_core_third_party_cala_en_v1",
    scenarioId: "op_scenario_third_party_management_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which third-party hotel operators are commonly considered by owners for a branded full-service hotel in Latin America?",
  }),
  p({
    promptId: "op_p_core_brand_agnostic_en_v1",
    scenarioId: "op_scenario_brand_agnostic_operation_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which operators are commonly considered when an owner needs a management company that can operate hotels across more than one brand system?",
  }),
  p({
    promptId: "op_p_core_independent_en_v1",
    scenarioId: "op_scenario_independent_hotel_operation_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which management companies are commonly considered for operating an independent hotel that will not carry a major franchise flag?",
  }),
  p({
    promptId: "op_p_core_conversion_en_v1",
    scenarioId: "op_scenario_conversion_repositioning_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which operators are commonly considered when an owner is converting or repositioning an existing hotel and needs a new operating partner?",
  }),
  p({
    promptId: "op_p_core_commercial_en_v1",
    scenarioId: "op_scenario_commercial_revenue_capability_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which hotel operators are commonly associated with strong commercial, revenue-management, and distribution capability for owner-decision discussions?",
  }),
  p({
    promptId: "op_p_core_resort_cala_en_v1",
    scenarioId: "op_scenario_resort_operation_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which management companies are commonly considered by owners for a resort hotel in the Caribbean or Latin America?",
  }),
  p({
    promptId: "op_p_core_cala_regional_en_v1",
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which hotel operators are commonly considered for hotels in CALA / Latin America when owners evaluate regional operating capability?",
  }),
  p({
    promptId: "op_p_core_institutional_en_v1",
    scenarioId: "op_scenario_institutional_platform_alignment_v1",
    tier: "CORE",
    samplingPriority: "CRITICAL",
    text: "Which operators are commonly considered by institutional hotel owners seeking a scalable management platform?",
  }),
  p({
    promptId: "op_p_ext_uu_mexico_en_v1",
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "For a 200-room upper-upscale full-service hotel in Mexico, which hotel management companies do owners commonly consider as operating partners?",
  }),
  p({
    promptId: "op_p_ext_uu_urban_en_v1",
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "When owners shortlist who should operate an urban full-service hotel, which management companies typically appear in that conversation?",
  }),
  p({
    promptId: "op_p_ext_luxury_branded_en_v1",
    scenarioId: "op_scenario_luxury_operator_selection_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered for a luxury hotel that will remain under a luxury brand system but still needs an operating decision?",
  }),
  p({
    promptId: "op_p_ext_lifestyle_independent_en_v1",
    scenarioId: "op_scenario_lifestyle_boutique_operator_selection_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered for a small independent lifestyle hotel where brand affiliation is optional?",
  }),
  p({
    promptId: "op_p_ext_owner_control_hma_en_v1",
    scenarioId: "op_scenario_owner_control_flexibility_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which hotel management companies are commonly considered when an owner prefers a hotel management agreement with more owner influence over strategy?",
  }),
  p({
    promptId: "op_p_ext_third_party_institutional_en_v1",
    scenarioId: "op_scenario_third_party_management_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which third-party hotel operators are commonly considered for a branded full-service hotel where the owner wants institutional management capability?",
  }),
  p({
    promptId: "op_p_ext_third_party_caribbean_en_v1",
    scenarioId: "op_scenario_third_party_management_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which third-party operators are commonly considered by owners for hotels in the Caribbean?",
  }),
  p({
    promptId: "op_p_ext_multi_brand_en_v1",
    scenarioId: "op_scenario_brand_agnostic_operation_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered when a portfolio includes hotels under more than one franchise brand and needs one operating partner?",
  }),
  p({
    promptId: "op_p_ext_independent_soft_en_v1",
    scenarioId: "op_scenario_independent_hotel_operation_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which management companies are commonly considered for an independent hotel that might later join a collection or soft brand?",
  }),
  p({
    promptId: "op_p_ext_conversion_flag_en_v1",
    scenarioId: "op_scenario_conversion_repositioning_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "When converting an existing hotel to a new operating model, which operators are commonly considered by owners?",
  }),
  p({
    promptId: "op_p_ext_repositioning_resort_en_v1",
    scenarioId: "op_scenario_conversion_repositioning_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered for repositioning a tired resort asset rather than a new-build?",
  }),
  p({
    promptId: "op_p_ext_revenue_distribution_en_v1",
    scenarioId: "op_scenario_commercial_revenue_capability_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly discussed when owners ask who can strengthen revenue management and distribution for a hotel they already own?",
  }),
  p({
    promptId: "op_p_ext_all_inclusive_en_v1",
    scenarioId: "op_scenario_resort_operation_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which management companies are commonly considered for an all-inclusive resort in Latin America?",
  }),
  p({
    promptId: "op_p_ext_resort_meetings_en_v1",
    scenarioId: "op_scenario_resort_operation_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered for a meetings-capable resort hotel in CALA?",
  }),
  p({
    promptId: "op_p_ext_latam_in_market_en_v1",
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered when an owner wants in-market Latin America operating capability rather than a purely global platform?",
  }),
  p({
    promptId: "op_p_ext_cala_branded_en_v1",
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "For a branded hotel in Central America or the Caribbean, which operators are commonly on an owner shortlist?",
  }),
  p({
    promptId: "op_p_ext_institutional_reporting_en_v1",
    scenarioId: "op_scenario_institutional_platform_alignment_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which operators are commonly considered by institutional owners who need consistent owner reporting and a scalable operating platform?",
  }),
  p({
    promptId: "op_p_ext_institutional_multi_asset_en_v1",
    scenarioId: "op_scenario_institutional_platform_alignment_v1",
    tier: "EXTENDED",
    samplingPriority: "HIGH",
    text: "Which management platforms are commonly considered when an owner has several hotels and wants one operating partner across the set?",
  }),
]);

export function listOperatorPrompts({ tier = null } = {}) {
  if (!tier) return [...OPERATOR_PROMPTS_V1];
  return OPERATOR_PROMPTS_V1.filter((x) => x.tier === tier);
}

export function promptLibraryStats() {
  const core = listOperatorPrompts({ tier: "CORE" });
  const extended = listOperatorPrompts({ tier: "EXTENDED" });
  const scenarioIds = new Set(OPERATOR_PROMPTS_V1.map((p) => p.scenarioId));
  return {
    total: OPERATOR_PROMPTS_V1.length,
    scenario: OPERATOR_PROMPTS_V1.length,
    observed: 0,
    derived: 0,
    core: core.length,
    extended: extended.length,
    scenariosCovered: scenarioIds.size,
    registrySize: OPERATOR_DECISION_SCENARIOS.length,
    dataforseoCalls: 0,
  };
}
