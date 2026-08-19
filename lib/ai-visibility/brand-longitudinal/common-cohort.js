/**
 * Common-cohort intersection logic for longitudinal trend denominators.
 */

export const BRAND_LONGITUDINAL_COMMON_COHORT_VERSION = "brand_longitudinal_common_cohort_v1";

/**
 * Build grain intersection key for common-cohort matching.
 */
export function commonCohortGrainKey(grain = {}) {
  return [
    grain.promptId || "",
    grain.promptVersion != null ? String(grain.promptVersion) : "",
    grain.brandId || "",
    String(grain.provider || "").toLowerCase(),
    grain.providerModel || "",
    grain.geographyKey || "",
    grain.language || "",
  ].join("|");
}

/**
 * Compute intersection of comparable grains across two measurement periods.
 */
export function computeCommonCohort(periodAGrains = [], periodBGrains = []) {
  const keysA = new Set(periodAGrains.map((g) => commonCohortGrainKey(g)));
  const keysB = new Set(periodBGrains.map((g) => commonCohortGrainKey(g)));
  const commonKeys = [...keysA].filter((k) => keysB.has(k));
  return {
    commonCount: commonKeys.length,
    periodACount: keysA.size,
    periodBCount: keysB.size,
    commonKeys,
    cohortChanged: keysA.size !== keysB.size || commonKeys.length < Math.min(keysA.size, keysB.size),
    label:
      commonKeys.length > 0
        ? `${commonKeys.length} comparable prompt-provider-brand grains`
        : "COHORT_CHANGED",
  };
}

/**
 * Filter metric observations to common cohort only.
 */
export function filterToCommonCohort(observations = [], commonKeys = []) {
  const set = new Set(commonKeys);
  return observations.filter((o) => set.has(commonCohortGrainKey(o)));
}

/**
 * Compare period-level rates on common cohort intersection.
 */
export function compareCommonCohortRates(periodA, periodB, valueFn) {
  const common = computeCommonCohort(periodA.grains || [], periodB.grains || []);
  if (!common.commonCount) {
    return {
      ok: false,
      comparabilityState: "COHORT_CHANGED",
      commonCohort: common,
      currentValue: null,
      priorValue: null,
    };
  }
  const fn = valueFn || ((o) => o.value);
  const aObs = filterToCommonCohort(periodA.observations || [], common.commonKeys);
  const bObs = filterToCommonCohort(periodB.observations || [], common.commonKeys);
  const avg = (arr) => {
    const vals = arr.map(fn).filter((v) => typeof v === "number" && Number.isFinite(v));
    if (!vals.length) return null;
    return vals.reduce((s, v) => s + v, 0) / vals.length;
  };
  return {
    ok: true,
    comparabilityState: common.cohortChanged ? "COHORT_CHANGED" : "COMMON_COHORT",
    commonCohort: common,
    currentValue: avg(bObs),
    priorValue: avg(aObs),
    sampleDenominatorCurrent: bObs.length,
    sampleDenominatorPrior: aObs.length,
  };
}
