/**
 * Canonical current-vs-prior comparable period helper.
 */

import { areTrendComparable, labelLongitudinalMovement } from "./comparability.js";
import { compareCommonCohortRates } from "./common-cohort.js";
import { normalizeMeasurementDate } from "./grain.js";
import { qualifyMeasurementPeriod } from "./measurement-period.js";

export const BRAND_LONGITUDINAL_CURRENT_VS_PRIOR_VERSION =
  "brand_longitudinal_current_vs_prior_v1";

/**
 * Extract distinct valid measurement dates from period list.
 * Excludes same-day duplicates and non-qualifying partial periods.
 */
export function extractDistinctMeasurementDates(periods = []) {
  const dates = new Set();
  for (const period of periods) {
    const q = qualifyMeasurementPeriod(period);
    if (!q.valid) continue;
    const d = normalizeMeasurementDate(period.completedAt || period.startedAt);
    if (d) dates.add(d);
  }
  return [...dates].sort();
}

/**
 * Build current vs prior comparison for a metric on qualified periods.
 */
export function buildCurrentVsPriorComparison(opts = {}) {
  const {
    currentPeriod = null,
    priorPeriod = null,
    metricKey = "aiPresenceRate",
    valueFn = null,
  } = opts;

  const curQ = qualifyMeasurementPeriod(currentPeriod);
  const priQ = qualifyMeasurementPeriod(priorPeriod);

  if (!currentPeriod || !priorPeriod) {
    return {
      ok: false,
      comparabilityState: "INSUFFICIENT_HISTORY",
      movement: labelLongitudinalMovement(null, null),
      currentMeasurementDate: normalizeMeasurementDate(currentPeriod?.completedAt),
      priorMeasurementDate: normalizeMeasurementDate(priorPeriod?.completedAt),
    };
  }

  if (!curQ.valid || !priQ.valid) {
    return {
      ok: false,
      comparabilityState: "PARTIAL_PERIOD",
      movement: labelLongitudinalMovement(null, null),
      currentMeasurementDate: normalizeMeasurementDate(currentPeriod.completedAt),
      priorMeasurementDate: normalizeMeasurementDate(priorPeriod.completedAt),
      note: "Partial periods excluded from headline comparison",
    };
  }

  const currentMeasurementDate = normalizeMeasurementDate(currentPeriod.completedAt);
  const priorMeasurementDate = normalizeMeasurementDate(priorPeriod.completedAt);

  const cohortCmp = compareCommonCohortRates(priorPeriod, currentPeriod, valueFn);
  if (!cohortCmp.ok) {
    return {
      ok: false,
      comparabilityState: cohortCmp.comparabilityState,
      currentMeasurementDate,
      priorMeasurementDate,
      movement: labelLongitudinalMovement(null, null),
      commonCohort: cohortCmp.commonCohort,
    };
  }

  const { currentValue, priorValue, sampleDenominatorCurrent, sampleDenominatorPrior } = cohortCmp;
  let relativeChange = null;
  if (
    typeof currentValue === "number" &&
    typeof priorValue === "number" &&
    priorValue !== 0
  ) {
    relativeChange = Number(((currentValue - priorValue) / Math.abs(priorValue)).toFixed(4));
  }

  return {
    ok: true,
    version: BRAND_LONGITUDINAL_CURRENT_VS_PRIOR_VERSION,
    metricKey,
    currentMeasurementDate,
    priorMeasurementDate,
    currentValue,
    priorValue,
    absoluteChange:
      typeof currentValue === "number" && typeof priorValue === "number"
        ? Number((currentValue - priorValue).toFixed(6))
        : null,
    relativeChange,
    comparabilityState: cohortCmp.comparabilityState,
    movement: labelLongitudinalMovement(currentValue, priorValue),
    sampleDenominatorCurrent,
    sampleDenominatorPrior,
    commonCohort: cohortCmp.commonCohort,
  };
}

/**
 * Build date series metadata for trend API foundation.
 */
export function buildDateSeriesFoundation(periods = []) {
  const dates = extractDistinctMeasurementDates(periods);
  return {
    distinctPeriodCount: dates.length,
    measurementDates: dates,
    syntheticPoints: 0,
    FAKE_INTERMEDIATE_POINTS: "NONE",
  };
}
