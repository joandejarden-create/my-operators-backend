/**
 * Core Period-2 prior run longitudinal reconciliation.
 */

import { loadAllPeriods, loadPeriod } from "../data-model.js";
import { loadPropertyProfile } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { buildLongitudinalComparison } from "../metrics/longitudinal-comparability.js";
import { resolvePriorCertifiedCorePeriodId } from "../monitoring/core-monitoring-period-registry-v1.js";

export const CORE_PRIOR_RUN_LONGITUDINAL_RECONCILIATION =
  "CORE_PRIOR_RUN_LONGITUDINAL_RECONCILIATION";

export function reconcileCorePriorRun(propertyId, currentPeriodId) {
  const profile = loadPropertyProfile(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const allPeriods = loadAllPeriods(propertyId);
  const currentPeriod = allPeriods.find((p) => p.periodId === currentPeriodId) || loadPeriod(currentPeriodId);
  const expectedPriorId = resolvePriorCertifiedCorePeriodId(propertyId);

  const longitudinal = buildLongitudinalComparison(currentPeriod, allPeriods, scenarios, profile);
  const priorMatchesExpected =
    !expectedPriorId || longitudinal.priorComparablePeriodId === expectedPriorId;

  return {
    propertyId,
    currentPeriodId,
    expectedPriorPeriodId: expectedPriorId,
    priorComparablePeriodId: longitudinal.priorComparablePeriodId,
    priorMatchesExpected,
    currentVsPriorReady: longitudinal.currentVsPriorReady,
    deltas: longitudinal.deltas,
    periodsSkippedAsIncomparable: longitudinal.periodsSkippedAsIncomparable,
    pass: longitudinal.currentVsPriorReady && priorMatchesExpected,
  };
}

export function reconcileAllCorePriorRuns(periodIdsByProperty) {
  const rows = [];
  for (const [propertyId, periodId] of Object.entries(periodIdsByProperty || {})) {
    rows.push(reconcileCorePriorRun(propertyId, periodId));
  }
  return {
    gate: CORE_PRIOR_RUN_LONGITUDINAL_RECONCILIATION,
    pass: rows.every((r) => r.pass),
    properties: rows,
  };
}
