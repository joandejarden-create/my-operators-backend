/**
 * V1.1 core eligibility filters + period projection for corrected measurement.
 */

import { isComparableObservation } from "../metrics/grain-governance.js";
import { SCENARIO_CLASSES } from "./prompt-integrity-contract-v1.js";
import { isCoreMeasurementEligibleObservation } from "./adp-measurement-contract-v1-1-candidate.js";

export const CORRECTION_TYPE = "PROMPT_MEASUREMENT_CONTRACT_CORRECTION";
export const CORRECTION_VERSION = "ADP_NEUTRAL_DEMAND_CORRECTION_V1_1";

/**
 * Observation contributes to ADP_MEASUREMENT_CONTRACT_V1_1 core KPIs.
 */
export function isV11CoreComparableObservation(obs) {
  if (!isComparableObservation(obs)) return false;
  if (obs.metricInclusion === false) return false;
  if (obs.measurementEligible === false || obs.measurementEligibility === false) return false;
  if (obs.correctionSlot?.role === "ORIGINAL_NON_NEUTRAL") return false;
  if (obs.coreMeasurementStatus === "EXCLUDED_FROM_CORE") return false;

  const cls =
    obs.scenarioClass ||
    obs.correctionClassification?.newClass ||
    obs.correctionSlot?.replacementScenarioClass ||
    null;

  if (obs.correctionSlot?.role === "REPLACEMENT_NEUTRAL") {
    return true;
  }
  if (obs.reclassification?.newClass === SCENARIO_CLASSES.NEUTRAL_DEMAND) {
    return isComparableObservation(obs);
  }
  if (cls === SCENARIO_CLASSES.NEUTRAL_DEMAND) return true;
  // Fail closed under V1.1 when class is known and non-neutral
  if (cls && cls !== SCENARIO_CLASSES.NEUTRAL_DEMAND) return false;
  // Legacy unmarked comparable: treat as eligible only if explicitly marked measurementEligible
  return obs.measurementEligible === true || obs.measurementEligibility === true;
}

/**
 * Project a period for V1.1 owner-payload / metrics (does not mutate storage).
 * Keeps all observations for evidence ledgers when options.includeAllForEvidence=true
 * but marks non-core; for metric functions we replace observations with core-only set.
 */
export function projectPeriodForV11CoreMetrics(period, { includeNonCoreInEvidenceLedger = false } = {}) {
  const all = period.observations || [];
  const core = all.filter(isV11CoreComparableObservation);
  if (!includeNonCoreInEvidenceLedger) {
    return {
      ...period,
      observations: core,
      measurementContractVersion: "ADP_MEASUREMENT_CONTRACT_V1_1",
      correctionProjection: {
        type: CORRECTION_TYPE,
        version: CORRECTION_VERSION,
        coreObservationCount: core.length,
        totalObservationCount: all.length,
      },
    };
  }
  // Evidence path needs scheduled/failed — keep full list but tag
  return {
    ...period,
    observations: all.map((o) => ({
      ...o,
      _v11CoreEligible: isV11CoreComparableObservation(o),
    })),
    measurementContractVersion: "ADP_MEASUREMENT_CONTRACT_V1_1",
  };
}

export { isCoreMeasurementEligibleObservation };
