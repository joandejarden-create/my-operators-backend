/**
 * Canonical comparable-period resolver for Core ADP.
 *
 * Current / Prior Run / Trends must resolve the same certified comparable lineage.
 * Do not invent ad hoc period lists for Trends.
 */

import { isTargetedMeasurementPeriod } from "../data-model.js";
import {
  filterCustomerTrendPeriods,
  isCustomerTrendEligible,
} from "../period-eligibility-v1.js";
import {
  arePeriodsComparable,
  selectPriorComparablePeriod,
} from "./longitudinal-comparability.js";
import { computePropertyRealityCoverage } from "../customer/executive-read-v1.js";
import { buildOptionalExecutiveMetrics } from "./optional-executive-metrics.js";
import { projectPeriodForV11CoreMetrics } from "../measurement-assurance/v1-1-core-eligibility.js";
import { MEASUREMENT_CONTRACT_V1_1 } from "../measurement-assurance/adp-measurement-contract-v1-1-candidate.js";

export const CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER =
  "CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER";

export const TREND_PERIOD_COUNT_MATCHES_COMPARABLE_HISTORY =
  "TREND_PERIOD_COUNT_MATCHES_COMPARABLE_HISTORY";

export const TREND_METRIC_VALUES_MATCH_CERTIFIED_PERIOD_METRICS =
  "TREND_METRIC_VALUES_MATCH_CERTIFIED_PERIOD_METRICS";

export const TREND_CURRENT_PRIOR_RECONCILIATION = "TREND_CURRENT_PRIOR_RECONCILIATION";

export const TREND_NO_STALE_SINGLE_PERIOD_AFTER_CERTIFIED_PUBLICATION =
  "TREND_NO_STALE_SINGLE_PERIOD_AFTER_CERTIFIED_PUBLICATION";

function sortByExecutionDate(periods) {
  return [...(periods || [])].sort((a, b) =>
    String(a.executionDate || "").localeCompare(String(b.executionDate || ""))
  );
}

function usesV11Metrics(period, measurementContractVersion = null) {
  return (
    measurementContractVersion === MEASUREMENT_CONTRACT_V1_1 ||
    measurementContractVersion === "ADP_MEASUREMENT_CONTRACT_V1_1" ||
    period?.measurementContractVersionActiveForCorrection === MEASUREMENT_CONTRACT_V1_1 ||
    period?.measurementContractVersion === MEASUREMENT_CONTRACT_V1_1
  );
}

/**
 * Resolve the canonical set of customer-comparable certified periods for a property.
 *
 * @param {object} args
 * @param {object[]} args.allPeriods
 * @param {object[]} args.scenarios
 * @param {string|null} [args.currentPeriodId]
 */
export function resolveCanonicalComparablePeriods({
  allPeriods,
  scenarios,
  currentPeriodId = null,
}) {
  const customer = filterCustomerTrendPeriods(allPeriods);
  const pool = customer.length
    ? customer
    : (allPeriods || []).filter((p) => !isTargetedMeasurementPeriod(p) && isCustomerTrendEligible(p));

  const sorted = sortByExecutionDate(pool);
  if (!sorted.length) {
    return {
      gate: CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER,
      comparablePeriods: [],
      currentPeriod: null,
      priorPeriod: null,
      currentPeriodId: null,
      priorPeriodId: null,
      periodCount: 0,
    };
  }

  const current =
    (currentPeriodId && sorted.find((p) => p.periodId === currentPeriodId)) ||
    sorted[sorted.length - 1];

  const comparablePeriods = sorted.filter(
    (p) => arePeriodsComparable(current, p, scenarios).comparable
  );

  const { priorPeriod } = selectPriorComparablePeriod(current, allPeriods, scenarios);

  return {
    gate: CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER,
    comparablePeriods,
    currentPeriod: current,
    priorPeriod: priorPeriod || null,
    currentPeriodId: current?.periodId || null,
    priorPeriodId: priorPeriod?.periodId || null,
    periodCount: comparablePeriods.length,
  };
}

/**
 * Certified metric point for one comparable period (Trends SoT).
 */
export function buildCertifiedTrendMetricPoint({
  period,
  scenarios,
  propertyProfile,
  measurementContractVersion = null,
}) {
  if (!period || !propertyProfile) return null;
  const useV11 = usesV11Metrics(period, measurementContractVersion);
  const periodForMetrics = useV11 ? projectPeriodForV11CoreMetrics(period) : period;
  const em = buildOptionalExecutiveMetrics(periodForMetrics, scenarios, propertyProfile);
  return {
    periodId: period.periodId,
    date: period.executionDate,
    propertyRealityCoverage: computePropertyRealityCoverage(periodForMetrics, propertyProfile),
    scenarioPresenceRate: em?.scenarioPresence?.rate ?? null,
    considerationRate: em?.considerationRate?.rate ?? null,
    providerCount: period.providers ? period.providers.length : period.providerCount || null,
    observationCount: periodForMetrics.observations ? periodForMetrics.observations.length : 0,
    measurementContractVersion: useV11
      ? MEASUREMENT_CONTRACT_V1_1
      : period?.measurementContractVersionActiveForCorrection ||
        period?.measurementContractVersion ||
        "ADP_MEASUREMENT_CONTRACT_V1",
    role:
      null, // filled by caller when current/prior known
  };
}

/**
 * Build Trends array from the canonical comparable-period set.
 */
export function buildTrendsFromCanonicalResolution({
  resolution,
  scenarios,
  propertyProfile,
  measurementContractVersion = null,
}) {
  const periods = resolution?.comparablePeriods || [];
  if (!periods.length || !propertyProfile) return undefined;

  return periods.map((p) => {
    const point = buildCertifiedTrendMetricPoint({
      period: p,
      scenarios,
      propertyProfile,
      measurementContractVersion,
    });
    if (!point) return null;
    let role = "historical";
    if (p.periodId === resolution.currentPeriodId) role = "current";
    else if (p.periodId === resolution.priorPeriodId) role = "prior_run";
    return { ...point, role };
  }).filter(Boolean);
}

/**
 * True when baked trends are stale relative to canonical comparable history.
 */
export function isStaleSinglePeriodTrends(bakedTrends, resolution) {
  const baked = Array.isArray(bakedTrends) ? bakedTrends : [];
  const expected = resolution?.periodCount || 0;
  if (expected >= 2 && baked.length < 2) return true;
  if (expected >= 2 && resolution.currentPeriodId) {
    const hasCurrent = baked.some((t) => t.periodId === resolution.currentPeriodId);
    if (!hasCurrent) return true;
  }
  if (expected > baked.length) return true;
  return false;
}
