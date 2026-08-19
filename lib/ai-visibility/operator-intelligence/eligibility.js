/**
 * Operator-decision commercial eligibility — governed facts only.
 * Do not infer from AI responses.
 */

import { OPERATOR_AI_UNIVERSE } from "./universe.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";

export const OPERATOR_ELIGIBILITY_VERSION = "operator_decision_eligibility_v1";

export const ELIGIBILITY = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  CONDITIONALLY_ELIGIBLE: "CONDITIONALLY_ELIGIBLE",
  OUT_OF_SCOPE: "OUT_OF_SCOPE",
  INSUFFICIENT_TRUTH: "INSUFFICIENT_TRUTH",
  UNKNOWN: "UNKNOWN",
});

const BY_SLUG = Object.freeze({
  "marriott-international-managed": {
    thirdPartyManagement: ELIGIBILITY.OUT_OF_SCOPE,
    brandAgnostic: ELIGIBILITY.OUT_OF_SCOPE,
    independentHotel: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    geographicFootprint: ELIGIBILITY.ELIGIBLE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    source: "Operator Master (Managed) lens + brand-managed Core 5 policy",
  },
  "ihg-managed": {
    thirdPartyManagement: ELIGIBILITY.OUT_OF_SCOPE,
    brandAgnostic: ELIGIBILITY.OUT_OF_SCOPE,
    independentHotel: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    geographicFootprint: ELIGIBILITY.ELIGIBLE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    source: "Operator Master (Managed) lens + brand-managed Core 5 policy",
  },
  "hilton-managed": {
    thirdPartyManagement: ELIGIBILITY.OUT_OF_SCOPE,
    brandAgnostic: ELIGIBILITY.OUT_OF_SCOPE,
    independentHotel: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    geographicFootprint: ELIGIBILITY.ELIGIBLE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    source: "Operator Master (Managed) lens + brand-managed Core 5 policy",
  },
  "aimbridge-latam": {
    thirdPartyManagement: ELIGIBILITY.ELIGIBLE,
    brandAgnostic: ELIGIBILITY.ELIGIBLE,
    independentHotel: ELIGIBILITY.ELIGIBLE,
    geographicFootprint: ELIGIBILITY.ELIGIBLE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.ELIGIBLE,
    source: "Aimbridge Hospitality (LATAM) Operator Master + aimbridgelatam.com governed packs",
  },
  "hotel-equities-cala": {
    thirdPartyManagement: ELIGIBILITY.ELIGIBLE,
    brandAgnostic: ELIGIBILITY.ELIGIBLE,
    independentHotel: ELIGIBILITY.ELIGIBLE,
    geographicFootprint: ELIGIBILITY.ELIGIBLE,
    resort: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    luxury: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.ELIGIBLE,
    source: "Hotel Equities (CALA) quality baseline",
  },
  "arbor-lodging-cala": {
    thirdPartyManagement: ELIGIBILITY.ELIGIBLE,
    brandAgnostic: ELIGIBILITY.ELIGIBLE,
    independentHotel: ELIGIBILITY.ELIGIBLE,
    geographicFootprint: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    resort: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    luxury: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    source: "Arbor Lodging (CALA) quality baseline — do not invent CALA managed counts",
  },
  "ghl-hoteles": {
    thirdPartyManagement: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    brandAgnostic: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    independentHotel: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    geographicFootprint: ELIGIBILITY.ELIGIBLE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.ELIGIBLE,
    source: "GHL Hoteles Operator Master — primarily proprietary brand-family platform",
  },
  "brittain-resorts-hotels": {
    thirdPartyManagement: ELIGIBILITY.ELIGIBLE,
    brandAgnostic: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    independentHotel: ELIGIBILITY.ELIGIBLE,
    geographicFootprint: ELIGIBILITY.OUT_OF_SCOPE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.INSUFFICIENT_TRUTH,
    institutionalPlatform: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.OUT_OF_SCOPE,
    source: "Brittain Resorts & Hotels Operator Master — US Southeast; CALA not established",
  },
  "remington-hospitality-cala": {
    thirdPartyManagement: ELIGIBILITY.ELIGIBLE,
    brandAgnostic: ELIGIBILITY.ELIGIBLE,
    independentHotel: ELIGIBILITY.ELIGIBLE,
    geographicFootprint: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    resort: ELIGIBILITY.ELIGIBLE,
    luxury: ELIGIBILITY.CONDITIONALLY_ELIGIBLE,
    institutionalPlatform: ELIGIBILITY.ELIGIBLE,
    calaLatamRegional: ELIGIBILITY.ELIGIBLE,
    source:
      "Remington Hospitality Operator Master rec6UB6RpMKSs2tAo — third-party manager with governed CALA expansion; label enterprise U.S. scale separately",
  },
});

const SCENARIO_DIMENSION = Object.freeze({
  op_scenario_third_party_management_v1: "thirdPartyManagement",
  op_scenario_brand_agnostic_operation_v1: "brandAgnostic",
  op_scenario_independent_hotel_operation_v1: "independentHotel",
  op_scenario_cala_latam_regional_capability_v1: "calaLatamRegional",
  op_scenario_resort_operation_v1: "resort",
  op_scenario_luxury_operator_selection_v1: "luxury",
  op_scenario_institutional_platform_alignment_v1: "institutionalPlatform",
  op_scenario_owner_control_flexibility_v1: "thirdPartyManagement",
  op_scenario_full_service_uu_operator_selection_v1: "geographicFootprint",
  op_scenario_lifestyle_boutique_operator_selection_v1: "independentHotel",
  op_scenario_conversion_repositioning_v1: "geographicFootprint",
  op_scenario_commercial_revenue_capability_v1: "institutionalPlatform",
});

export function eligibilityFor(operatorId, scenarioId) {
  const row = OPERATOR_AI_UNIVERSE.find((o) => o.canonicalId === operatorId);
  if (!row) return { status: ELIGIBILITY.UNKNOWN, reason: "not_in_primary_universe" };
  const pack = BY_SLUG[row.slug];
  const dim = SCENARIO_DIMENSION[scenarioId];
  if (!pack || !dim) return { status: ELIGIBILITY.UNKNOWN, reason: "unmapped", slug: row.slug };
  return {
    status: pack[dim],
    dimension: dim,
    slug: row.slug,
    source: pack.source,
  };
}

export function listEligibilityByOperator() {
  return OPERATOR_AI_UNIVERSE.map((o) => ({
    canonicalId: o.canonicalId,
    slug: o.slug,
    ...BY_SLUG[o.slug],
  }));
}

export function listScenarioEligibilityMatrix() {
  return OPERATOR_AI_UNIVERSE.flatMap((op) =>
    OPERATOR_DECISION_SCENARIOS.map((scenario) => {
      const row = eligibilityFor(op.canonicalId, scenario.scenarioId);
      return {
        operatorId: op.canonicalId,
        canonicalName: op.canonicalName,
        scenarioId: scenario.scenarioId,
        status: row.status,
        dimension: row.dimension,
        source: row.source,
      };
    })
  );
}

export function summarizeScenarioEligibilityMatrix(matrix = listScenarioEligibilityMatrix()) {
  const count = (status) => matrix.filter((r) => r.status === status).length;
  return {
    totalOperatorScenarioPairs: matrix.length,
    eligible: count(ELIGIBILITY.ELIGIBLE),
    conditionallyEligible: count(ELIGIBILITY.CONDITIONALLY_ELIGIBLE),
    outOfScope: count(ELIGIBILITY.OUT_OF_SCOPE),
    insufficientTruth: count(ELIGIBILITY.INSUFFICIENT_TRUTH),
    unknown: count(ELIGIBILITY.UNKNOWN),
  };
}
