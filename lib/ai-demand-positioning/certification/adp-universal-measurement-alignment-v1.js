/**
 * ADP_UNIVERSAL_MEASUREMENT_ALIGNMENT_V1
 *
 * Permanent Core ADP + Loyalty BPP measurement-family classification.
 * Used by Full E2E and portfolio alignment audits.
 *
 * Families are not interchangeable:
 * - CORE_ADP: STANDARD 60–65 (target 63) × 4 providers
 * - LOYALTY_BPP: STANDARD 21–32 (target 28) × 4 providers
 */

import { ADP_SCENARIO_DENSITY_TIER_STANDARD_V1 } from "./adp-scenario-universe-certification-v1.js";
import {
  LOYALTY_BPP_SCENARIO_STANDARD_V1,
  LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1,
  LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1,
  MEASUREMENT_FAMILY_CORE_ADP,
  MEASUREMENT_FAMILY_LOYALTY_BPP,
  classifyLoyaltyDensityTier,
} from "../brand-portfolio/loyalty-bpp-scenario-standard-v1.js";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";

export const ADP_UNIVERSAL_MEASUREMENT_ALIGNMENT_V1 =
  "ADP_UNIVERSAL_MEASUREMENT_ALIGNMENT_V1";

export { MEASUREMENT_FAMILY_CORE_ADP, MEASUREMENT_FAMILY_LOYALTY_BPP };

/** Gap / readiness classification (binding vocabulary) */
export const MEASUREMENT_CLASS = Object.freeze({
  STANDARD_CERTIFIED: "STANDARD_CERTIFIED",
  COMPATIBLE_EXISTING_EVIDENCE: "COMPATIBLE_EXISTING_EVIDENCE",
  LEGACY_VALID_REMEASUREMENT_REQUIRED: "LEGACY_VALID_REMEASUREMENT_REQUIRED",
  PARTIALLY_MEASURED: "PARTIALLY_MEASURED",
  MISSING_ORIGINAL_RESPONSES: "MISSING_ORIGINAL_RESPONSES",
  NOT_MEASURED: "NOT_MEASURED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  AUTHORIZATION_REQUIRED: "AUTHORIZATION_REQUIRED",
});

/** Batch 1 Core — do not remonitor without explicit invalidation */
export const BATCH_1_CORE_PRESERVE = Object.freeze({
  adp_ac_hotel_a_coruna: Object.freeze({
    periodId: "adp_period_adp_ac_hotel_a_coruna_20261019220742_472e0c",
    scenarios: 63,
    observations: 252,
    spendUsd: 8.19,
    note: "Batch 1 Core STANDARD — preserve",
  }),
  adp_w_rome: Object.freeze({
    periodId: "adp_period_adp_w_rome_20261019224104_a23f6f",
    scenarios: 63,
    observations: 252,
    spendUsd: 8.19,
    note: "Batch 1 Core STANDARD — preserve",
  }),
});

export function costPerScenarioAllProviders() {
  return Object.values(PROVIDER_CONFIGS).reduce(
    (s, c) => s + (c.costPerCall || 0),
    0
  );
}

export function estimateFamilyCostUsd(missingScenarios) {
  const n = Math.max(0, Number(missingScenarios) || 0);
  const unit = costPerScenarioAllProviders();
  const api = Math.round(n * unit * 100) / 100;
  const retryReserve = Math.round(api * 0.12 * 100) / 100;
  return {
    missingScenarios: n,
    usdPerScenarioAllProviders: unit,
    apiUsd: api,
    retryReserveUsd: retryReserve,
    totalWithReserveUsd: Math.round((api + retryReserve) * 100) / 100,
    expectedObservations: n * 4,
  };
}

/**
 * Classify Core family status from observed scenario/observation counts.
 */
export function classifyCoreMeasurement({
  scenarioCount = 0,
  observationCount = 0,
  withOriginalResponse = null,
  preserveBatch1 = false,
  propertyId = null,
} = {}) {
  const sc = Number(scenarioCount) || 0;
  const obs = Number(observationCount) || 0;
  const expectedObs = sc * 4;
  const batch1 = propertyId && BATCH_1_CORE_PRESERVE[propertyId];

  if (preserveBatch1 || batch1) {
    return {
      family: MEASUREMENT_FAMILY_CORE_ADP,
      class: MEASUREMENT_CLASS.STANDARD_CERTIFIED,
      preserve: true,
      scenarioCount: batch1?.scenarios || sc,
      observationCount: batch1?.observations || obs,
      remonitorRequired: false,
      missingScenarios: 0,
      note: batch1?.note || "Batch 1 / STANDARD Core preserved",
    };
  }

  if (sc <= 0) {
    return {
      family: MEASUREMENT_FAMILY_CORE_ADP,
      class: MEASUREMENT_CLASS.NOT_MEASURED,
      remonitorRequired: true,
      missingScenarios: ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.targetScenarioCount,
      scenarioCount: 0,
      observationCount: 0,
    };
  }

  const inBand =
    sc >= ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.allowedMin &&
    sc <= ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.allowedMax;
  const obsComplete = obs >= expectedObs && expectedObs > 0;

  if (
    withOriginalResponse != null &&
    withOriginalResponse < obs * 0.95 &&
    obs > 0
  ) {
    return {
      family: MEASUREMENT_FAMILY_CORE_ADP,
      class: MEASUREMENT_CLASS.MISSING_ORIGINAL_RESPONSES,
      remonitorRequired: true,
      missingScenarios: inBand
        ? 0
        : Math.max(0, ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.targetScenarioCount - sc),
      scenarioCount: sc,
      observationCount: obs,
      withOriginalResponse,
      note: "Original-response archive incomplete — recovery or remonitor required",
    };
  }

  if (inBand && obsComplete) {
    return {
      family: MEASUREMENT_FAMILY_CORE_ADP,
      class: MEASUREMENT_CLASS.STANDARD_CERTIFIED,
      remonitorRequired: false,
      missingScenarios: 0,
      scenarioCount: sc,
      observationCount: obs,
    };
  }

  if (inBand && !obsComplete) {
    return {
      family: MEASUREMENT_FAMILY_CORE_ADP,
      class: MEASUREMENT_CLASS.PARTIALLY_MEASURED,
      remonitorRequired: true,
      missingScenarios: 0,
      missingObservations: Math.max(0, expectedObs - obs),
      scenarioCount: sc,
      observationCount: obs,
    };
  }

  if (sc > 0 && sc < ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.allowedMin) {
    return {
      family: MEASUREMENT_FAMILY_CORE_ADP,
      class: MEASUREMENT_CLASS.PARTIALLY_MEASURED,
      remonitorRequired: true,
      missingScenarios: Math.max(
        0,
        ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.targetScenarioCount - sc
      ),
      scenarioCount: sc,
      observationCount: obs,
    };
  }

  return {
    family: MEASUREMENT_FAMILY_CORE_ADP,
    class: MEASUREMENT_CLASS.COMPATIBLE_EXISTING_EVIDENCE,
    remonitorRequired: sc < ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.allowedMin,
    missingScenarios: Math.max(
      0,
      ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.targetScenarioCount - sc
    ),
    scenarioCount: sc,
    observationCount: obs,
  };
}

/**
 * Classify Loyalty BPP family status.
 */
export function classifyLoyaltyMeasurement({
  scenarioCount = 0,
  observationCount = 0,
  notApplicable = false,
  withOriginalResponse = null,
} = {}) {
  if (notApplicable) {
    return {
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      class: MEASUREMENT_CLASS.NOT_APPLICABLE,
      remonitorRequired: false,
      missingScenarios: 0,
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      note: "Independent / no loyalty ecosystem — use Independent Positioning BPP path",
    };
  }

  const sc = Number(scenarioCount) || 0;
  const obs = Number(observationCount) || 0;
  const density = classifyLoyaltyDensityTier(sc);

  if (sc <= 0) {
    return {
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      class: MEASUREMENT_CLASS.NOT_MEASURED,
      remonitorRequired: true,
      missingScenarios: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount,
      density,
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
    };
  }

  if (
    withOriginalResponse != null &&
    obs > 0 &&
    withOriginalResponse < obs * 0.95
  ) {
    return {
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      class: MEASUREMENT_CLASS.MISSING_ORIGINAL_RESPONSES,
      remonitorRequired: true,
      missingScenarios: density.standardCertified
        ? 0
        : Math.max(
            0,
            LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount - sc
          ),
      density,
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
    };
  }

  if (density.standardCertified) {
    return {
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      class: MEASUREMENT_CLASS.STANDARD_CERTIFIED,
      remonitorRequired: false,
      missingScenarios: 0,
      density,
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      scenarioCount: sc,
      observationCount: obs,
    };
  }

  if (density.tier === LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1.id) {
    return {
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      class: MEASUREMENT_CLASS.LEGACY_VALID_REMEASUREMENT_REQUIRED,
      remonitorRequired: true,
      missingScenarios: Math.max(
        0,
        LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount - sc
      ),
      density,
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      note: "Legacy 8–12 pulse preserved historically; does not satisfy STANDARD 28",
      scenarioCount: sc,
      observationCount: obs,
    };
  }

  if (sc > 0 && sc < LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMin) {
    return {
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      class: MEASUREMENT_CLASS.PARTIALLY_MEASURED,
      remonitorRequired: true,
      missingScenarios: Math.max(
        0,
        LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount - sc
      ),
      density,
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      scenarioCount: sc,
      observationCount: obs,
    };
  }

  return {
    family: MEASUREMENT_FAMILY_LOYALTY_BPP,
    class: MEASUREMENT_CLASS.AUTHORIZATION_REQUIRED,
    remonitorRequired: true,
    missingScenarios: Math.max(
      0,
      LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount - sc
    ),
    density,
    contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
    scenarioCount: sc,
    observationCount: obs,
  };
}

/**
 * Assert Full E2E permanently separates Core vs Loyalty.
 */
export function assertMeasurementFamilySeparation(workflowBlob) {
  const failures = [];
  const s = typeof workflowBlob === "string" ? workflowBlob : JSON.stringify(workflowBlob);
  if (!s.includes("CORE_ADP") && !s.includes("measurementFamily")) {
    failures.push("CORE_FAMILY_NOT_REFERENCED");
  }
  if (!s.includes("LOYALTY_BPP")) {
    failures.push("LOYALTY_FAMILY_NOT_REFERENCED");
  }
  if (!s.includes("loyalty_scenario_universe_certification")) {
    failures.push("LOYALTY_SCENARIO_GATE_MISSING");
  }
  if (!s.includes("scenario_universe_certification")) {
    failures.push("CORE_SCENARIO_GATE_MISSING");
  }
  return { pass: failures.length === 0, failures };
}
