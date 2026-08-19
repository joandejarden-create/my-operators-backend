/**
 * Longitudinal comparability contract — trend vs stability separation.
 */

import { buildPeriodComparabilityKey, comparePeriodObservations } from "../recurring-comparability.js";
import { STAGE_B_AUTHORITATIVE_WAVE_ID, STAGE_B_NON_AUTHORITATIVE_WAVE_IDS } from "../stability-policy.js";

export const BRAND_LONGITUDINAL_COMPARABILITY_VERSION =
  "brand_longitudinal_comparability_v1";

export const PROMPT_VERSION_STATES = Object.freeze({
  DIRECTLY_COMPARABLE: "DIRECTLY_COMPARABLE",
  COMPATIBLE_MINOR_CHANGE: "COMPATIBLE_MINOR_CHANGE",
  NEW_SERIES_REQUIRED: "NEW_SERIES_REQUIRED",
});

export const MODEL_CHANGE_BOUNDARY = "MODEL_CHANGE_BOUNDARY";

export const MOVEMENT_LABELS = Object.freeze({
  INCREASED: "INCREASED",
  DECREASED: "DECREASED",
  UNCHANGED: "UNCHANGED",
  NEWLY_OBSERVED: "NEWLY_OBSERVED",
  NO_LONGER_OBSERVED: "NO_LONGER_OBSERVED",
  INSUFFICIENT_HISTORY: "INSUFFICIENT_HISTORY",
});

/** Forbidden directional copy without commercial interpretation. */
export const FORBIDDEN_MOVEMENT_LABELS = Object.freeze(["IMPROVED", "WORSENED"]);

/**
 * Classify prompt version compatibility between periods.
 */
export function classifyPromptVersionCompatibility(current, prior) {
  if (!current || !prior) {
    return { state: PROMPT_VERSION_STATES.NEW_SERIES_REQUIRED, reason: "missing_version" };
  }
  if (current.promptId !== prior.promptId) {
    return { state: PROMPT_VERSION_STATES.NEW_SERIES_REQUIRED, reason: "prompt_id_changed" };
  }
  const curV = String(current.promptVersion ?? "");
  const priV = String(prior.promptVersion ?? "");
  if (curV === priV) {
    return { state: PROMPT_VERSION_STATES.DIRECTLY_COMPARABLE, reason: null };
  }
  if (current.promptTextHash && prior.promptTextHash && current.promptTextHash === prior.promptTextHash) {
    return {
      state: PROMPT_VERSION_STATES.COMPATIBLE_MINOR_CHANGE,
      reason: "version_bump_same_text_hash",
    };
  }
  return { state: PROMPT_VERSION_STATES.NEW_SERIES_REQUIRED, reason: "prompt_text_or_version_changed" };
}

/**
 * Two observations are trend-comparable when required dimensions match.
 */
export function areTrendComparable(current, prior) {
  if (!current || !prior) {
    return { comparable: false, reasonCode: "INSUFFICIENT_HISTORY", comparabilityState: "NOT_COMPARABLE" };
  }

  const curDate = String(current.measurementDate || "").slice(0, 10);
  const priDate = String(prior.measurementDate || "").slice(0, 10);
  if (curDate && priDate && curDate === priDate) {
    return {
      comparable: false,
      reasonCode: "SAME_MEASUREMENT_DATE",
      comparabilityState: "NOT_COMPARABLE",
      note: "Same-day repeats support stability, not separate trend periods.",
    };
  }

  if (current.waveId && STAGE_B_NON_AUTHORITATIVE_WAVE_IDS.includes(current.waveId)) {
    return {
      comparable: false,
      reasonCode: "ARCHIVED_STAGE_B_WAVE",
      comparabilityState: "NOT_COMPARABLE",
    };
  }

  const periodCmp = comparePeriodObservations(
    {
      provider: current.provider,
      providerModel: current.providerModel,
      promptId: current.promptId,
      promptVersion: current.promptVersion,
      geographyKey: current.geographyKey,
      language: current.language,
    },
    {
      provider: prior.provider,
      providerModel: prior.providerModel,
      promptId: prior.promptId,
      promptVersion: prior.promptVersion,
      geographyKey: prior.geographyKey,
      language: prior.language,
    }
  );

  if (!periodCmp.comparable) {
    const isModel =
      periodCmp.reasonCode === "NON_COMPARABLE_MODEL" ||
      periodCmp.reasonCode === "NON_COMPARABLE_EXECUTION_CONFIG";
    return {
      comparable: false,
      reasonCode: periodCmp.reasonCode,
      comparabilityState: isModel ? MODEL_CHANGE_BOUNDARY : "NOT_COMPARABLE",
    };
  }

  if (current.brandId && prior.brandId && current.brandId !== prior.brandId) {
    return { comparable: false, reasonCode: "NON_COMPARABLE_BRAND", comparabilityState: "NOT_COMPARABLE" };
  }

  const promptCompat = classifyPromptVersionCompatibility(current, prior);
  if (promptCompat.state === PROMPT_VERSION_STATES.NEW_SERIES_REQUIRED) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_PROMPT_VERSION",
      comparabilityState: "NOT_COMPARABLE",
      promptVersionState: promptCompat.state,
    };
  }

  return {
    comparable: true,
    reasonCode: null,
    comparabilityState: "COMPARABLE",
    promptVersionState: promptCompat.state,
    comparabilityKey: buildPeriodComparabilityKey(current),
  };
}

/**
 * Label numeric movement without improved/worsened semantics.
 */
export function labelLongitudinalMovement(currentValue, priorValue) {
  if (currentValue == null && priorValue == null) return MOVEMENT_LABELS.UNCHANGED;
  if (currentValue != null && priorValue == null) return MOVEMENT_LABELS.NEWLY_OBSERVED;
  if (currentValue == null && priorValue != null) return MOVEMENT_LABELS.NO_LONGER_OBSERVED;
  if (!Number.isFinite(currentValue) || !Number.isFinite(priorValue)) {
    return MOVEMENT_LABELS.INSUFFICIENT_HISTORY;
  }
  if (currentValue > priorValue) return MOVEMENT_LABELS.INCREASED;
  if (currentValue < priorValue) return MOVEMENT_LABELS.DECREASED;
  return MOVEMENT_LABELS.UNCHANGED;
}

export { STAGE_B_AUTHORITATIVE_WAVE_ID, STAGE_B_NON_AUTHORITATIVE_WAVE_IDS };
