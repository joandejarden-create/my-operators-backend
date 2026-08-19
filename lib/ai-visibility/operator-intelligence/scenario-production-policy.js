/**
 * Operator scenario production tiers for Questions Missing and Competitive Gap.
 * Utility comes from the certified presence wave. Tiers are interpretation policy.
 */

import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";

export const OPERATOR_SCENARIO_PRODUCTION_POLICY_VERSION = "operator_scenario_production_policy_v2";

const POLICY = Object.freeze({
  op_scenario_full_service_uu_operator_selection_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CORE",
    customerEligible: "YES",
    notes:
      "Same-model CORE (brand-managed trio, or TPM×TPM). Brand-managed vs third-party is SECONDARY.",
  }),
  op_scenario_luxury_operator_selection_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CONDITIONAL",
    customerEligible: "NO",
    notes: "Capability truth incomplete for several operators. Questions Missing may be CORE; gaps stay CONDITIONAL.",
  }),
  op_scenario_lifestyle_boutique_operator_selection_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CONDITIONAL",
    customerEligible: "NO",
    notes: "Brand-managed independent-hotel eligibility is conditional.",
  }),
  op_scenario_owner_control_flexibility_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CORE",
    customerEligible: "YES",
    notes: "Brand-managed operators are out of scope. Third-party managers may be CORE comparable.",
  }),
  op_scenario_third_party_management_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CORE",
    customerEligible: "YES",
    notes:
      "Primary competitive-gap scenario for third-party managers. Brand-managed absence is out of scope, not a gap.",
  }),
  op_scenario_brand_agnostic_operation_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CORE",
    customerEligible: "YES",
    notes: "Brand-managed operators are out of scope. Multi-brand third-party managers may be CORE comparable.",
  }),
  op_scenario_independent_hotel_operation_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CORE",
    customerEligible: "YES",
    notes: "Third-party / independent-capable managers. Brand-managed pairs stay conditional.",
  }),
  op_scenario_conversion_repositioning_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CONDITIONAL",
    customerEligible: "NO",
    notes: "Conversion capability truth is incomplete. Do not client-promote gaps.",
  }),
  op_scenario_commercial_revenue_capability_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "DETAIL_ONLY",
    competitiveGapTier: "DETAIL_ONLY",
    customerEligible: "NO",
    notes:
      "Presence is observable. Do not convert appearance into stronger commercial capability. Remain DETAIL_ONLY, not CORE_GAP_ELIGIBLE.",
  }),
  op_scenario_resort_operation_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CONDITIONAL",
    customerEligible: "NO",
    notes: "Hotel Equities / Arbor resort eligibility is conditional.",
  }),
  op_scenario_cala_latam_regional_capability_v1: Object.freeze({
    utility: "HIGH",
    questionsMissingTier: "CORE",
    competitiveGapTier: "CORE",
    customerEligible: "YES",
    notes:
      "CALA/LATAM TPM×TPM may be CORE. GHL mixed platform vs TPM is SECONDARY. Brittain is out of scope. Brand-managed is conditional.",
  }),
  op_scenario_institutional_platform_alignment_v1: Object.freeze({
    utility: "MEDIUM",
    questionsMissingTier: "DETAIL_ONLY",
    competitiveGapTier: "DETAIL_ONLY",
    customerEligible: "NO",
    recommendedTier: "DETAIL_ONLY",
    notes:
      "Monitored-operator naming ~28.6%. Generic institutional/platform answers. Low presence is not competitive weakness.",
  }),
});

export function getOperatorScenarioProductionPolicy(scenarioId) {
  return (
    POLICY[scenarioId] || {
      utility: "UNKNOWN",
      questionsMissingTier: "RESEARCH_ONLY",
      competitiveGapTier: "RESEARCH_ONLY",
      customerEligible: "NO",
      notes: "unmapped",
    }
  );
}

export function listOperatorScenarioProductionPolicies() {
  return OPERATOR_DECISION_SCENARIOS.map((s) => ({
    scenarioId: s.scenarioId,
    customerOwnerIntent: s.ownerDecision,
    ...getOperatorScenarioProductionPolicy(s.scenarioId),
  }));
}

export function institutionalScenarioRecommendation() {
  const row = getOperatorScenarioProductionPolicy(
    "op_scenario_institutional_platform_alignment_v1"
  );
  return {
    currentUtility: row.utility,
    recommendedTier: "DETAIL_ONLY",
    rationale:
      "The scenario is a real owner decision, but the live corpus mostly returns generic institutional/platform answers rather than operator-discriminating names. Do not treat low Presence as a competitive weakness. Keep evidence. Do not delete. Do not keep as CORE for competitive gap.",
  };
}

export function commercialRevenueScenarioRecommendation() {
  return {
    recommendedTier: "DETAIL_ONLY",
    coreGapEligible: false,
    rationale:
      "Presence can remain observable. Converting appearance into stronger commercial/revenue capability is an association claim, not a Presence gap. Remain DETAIL_ONLY.",
  };
}
