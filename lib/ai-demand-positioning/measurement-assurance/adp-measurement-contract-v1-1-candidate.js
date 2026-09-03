/**
 * ADP_MEASUREMENT_CONTRACT_V1_1 — candidate (not silently activated).
 *
 * Smallest governed version change from V1:
 * - Same formulas / grains / provider rules
 * - NEW: CORE_SCENARIO_CLASS_ELIGIBILITY = NEUTRAL_DEMAND only
 *
 * Do not replace V1 hash baseline until founder approves re-publication under V1.1.
 */

import {
  buildMeasurementContractCanonicalBody,
  MEASUREMENT_CONTRACT_VERSION as V1,
  hashMeasurementContract,
} from "../contracts/adp-measurement-contract-v1.js";
import { SCENARIO_CLASSES, CORE_KPI_ELIGIBLE_CLASSES } from "./prompt-integrity-contract-v1.js";

export const MEASUREMENT_CONTRACT_V1_1 = "ADP_MEASUREMENT_CONTRACT_V1_1";
export const MEASUREMENT_CONTRACT_V2 = "ADP_MEASUREMENT_CONTRACT_V2";

export const CORE_ELIGIBILITY_CHANGE_V1_1 = Object.freeze({
  OLD: "mixed_neutral_and_special_scenario_eligibility (all comparable observations in core KPIs)",
  NEW: "neutral_demand_only_core_measurement",
  CORE_SCENARIO_CLASS_ELIGIBILITY: [...CORE_KPI_ELIGIBLE_CLASSES],
  EXCLUDED_FROM_CORE: [
    SCENARIO_CLASSES.PROPERTY_SPECIFIC,
    SCENARIO_CLASSES.BRAND_SPECIFIC,
    SCENARIO_CLASSES.COMPETITOR_SPECIFIC,
    SCENARIO_CLASSES.OTHER_GOVERNED_SPECIAL,
  ],
  SPECIAL_INTELLIGENCE_RETENTION: true,
  CORRECTED_SCENARIO_SINGLE_MEASUREMENT_SLOT: true,
});

export function buildMeasurementContractV1_1CanonicalBody() {
  const v1 = buildMeasurementContractCanonicalBody();
  return {
    ...v1,
    CONTRACT_VERSION: MEASUREMENT_CONTRACT_V1_1,
    SUPERSEDES: V1,
    CORE_SCENARIO_CLASS_ELIGIBILITY: CORE_ELIGIBILITY_CHANGE_V1_1.CORE_SCENARIO_CLASS_ELIGIBILITY,
    CORE_SCENARIO_EXCLUSIONS: CORE_ELIGIBILITY_CHANGE_V1_1.EXCLUDED_FROM_CORE,
    COMPARABLE_OBSERVATION_RULE:
      `${v1.COMPARABLE_OBSERVATION_RULE} Core KPIs further require scenarioClass===NEUTRAL_DEMAND (or replacement slot with measurementEligible=true under V1.1).`,
    SPECIAL_INTELLIGENCE_RULE:
      "PROPERTY_SPECIFIC / BRAND_SPECIFIC / COMPETITOR_SPECIFIC / OTHER_GOVERNED_SPECIAL may be retained as SPECIAL_INTELLIGENCE and must not enter core neutral-demand KPIs.",
    CORRECTED_SCENARIO_SINGLE_MEASUREMENT_SLOT:
      "Original non-neutral observation + neutral replacement share one measurement slot; never double-count.",
    PROMPT_INTEGRITY_CONTRACT_VERSION: "ADP_PROMPT_INTEGRITY_CONTRACT_V1",
  };
}

export function recommendMeasurementContractVersion() {
  return {
    recommendation: MEASUREMENT_CONTRACT_V1_1,
    rejectV2Reason:
      "Formulas unchanged; only core eligibility narrows. V2 reserved for formula/grain redesign.",
    activateOnlyAfter: [
      "founder_approval_of_V1_1_body",
      "neutral_replacement_execution_complete",
      "five_property_recertification_PASS",
      "corrected_report_publication_approved",
    ],
    v1Hash: hashMeasurementContract(buildMeasurementContractCanonicalBody()),
    v1_1Hash: hashMeasurementContract(buildMeasurementContractV1_1CanonicalBody()),
    changeSummary: CORE_ELIGIBILITY_CHANGE_V1_1,
  };
}

/** Live filter helper — available for recovery path; do not wire into production metrics until V1.1 activation. */
export function isCoreMeasurementEligibleObservation(obs, scenarioMeta) {
  if (obs?.measurementEligible === false) return false;
  if (obs?.correctionSlot?.role === "ORIGINAL_NON_NEUTRAL") return false;
  const cls = scenarioMeta?.scenarioClass || obs?.scenarioClass;
  if (cls && cls !== SCENARIO_CLASSES.NEUTRAL_DEMAND) return false;
  if (obs?.measurementEligible === true && cls === SCENARIO_CLASSES.NEUTRAL_DEMAND) return true;
  if (!cls) return false; // fail closed under V1.1 once activated
  return cls === SCENARIO_CLASSES.NEUTRAL_DEMAND;
}
