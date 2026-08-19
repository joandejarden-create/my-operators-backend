/**
 * OPERATOR_DECISION_SCENARIO_REGISTRY_V1
 * Decision-driven. Not tailored to the current 9 operators.
 */

export const OPERATOR_SCENARIO_REGISTRY_VERSION = "operator_decision_scenario_registry_v1";

export const OPERATOR_DECISION_SCENARIOS = Object.freeze([
  Object.freeze({
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    name: "Full-service upper-upscale operator selection",
    ownerDecision: "Who should operate an upper-upscale full-service hotel?",
    regionalScope: "GLOBAL_WITH_CALA_VARIANT",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_luxury_operator_selection_v1",
    name: "Luxury hotel operator selection",
    ownerDecision: "Who is commonly considered to operate a luxury hotel?",
    regionalScope: "GLOBAL_WITH_CALA_VARIANT",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_lifestyle_boutique_operator_selection_v1",
    name: "Lifestyle / boutique operator selection",
    ownerDecision: "Which operators are considered for independent lifestyle or boutique hotels?",
    regionalScope: "GLOBAL_WITH_CALA_VARIANT",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_owner_control_flexibility_v1",
    name: "Owner control / flexibility",
    ownerDecision:
      "Which hotel management companies are suitable for owners wanting greater strategic or operational control?",
    regionalScope: "GLOBAL",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_third_party_management_v1",
    name: "Third-party management",
    ownerDecision: "Which third-party hotel operators are commonly considered by owners?",
    regionalScope: "GLOBAL_WITH_CALA_VARIANT",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_brand_agnostic_operation_v1",
    name: "Brand-agnostic operation",
    ownerDecision: "Which operators can manage hotels across multiple brand systems?",
    regionalScope: "GLOBAL",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_independent_hotel_operation_v1",
    name: "Independent hotel operation",
    ownerDecision: "Which management companies are considered for independent hotels?",
    regionalScope: "GLOBAL_WITH_CALA_VARIANT",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_conversion_repositioning_v1",
    name: "Conversion / repositioning",
    ownerDecision: "Which operators are considered when repositioning or converting an existing hotel?",
    regionalScope: "GLOBAL",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_commercial_revenue_capability_v1",
    name: "Commercial performance / revenue capability",
    ownerDecision:
      "Which operators are associated with strong commercial, revenue-management and distribution capability?",
    regionalScope: "GLOBAL",
    commercialRelevance: "MEDIUM",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_resort_operation_v1",
    name: "Resort operation",
    ownerDecision: "Which management companies are commonly considered for resort hotels?",
    regionalScope: "CALA_LATAM_EMPHASIS",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    name: "CALA / LATAM regional capability",
    ownerDecision: "Which operators are commonly considered for hotels in CALA / Latin America?",
    regionalScope: "CALA_LATAM",
    commercialRelevance: "HIGH",
    core: true,
  }),
  Object.freeze({
    scenarioId: "op_scenario_institutional_platform_alignment_v1",
    name: "Owner alignment / institutional platform",
    ownerDecision:
      "Which operators are commonly considered by institutional hotel owners seeking scalable management platforms?",
    regionalScope: "GLOBAL",
    commercialRelevance: "HIGH",
    core: true,
  }),
]);

export function getOperatorScenario(scenarioId) {
  return OPERATOR_DECISION_SCENARIOS.find((s) => s.scenarioId === scenarioId) || null;
}
