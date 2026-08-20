/**
 * Current vs Prior comparability — not customer trend charts.
 */

import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { buildExecutiveMetricsFoundation } from "./executive-metrics-foundation.js";
import { roundAdpPercent } from "../format-percent.js";
import { computeRealityGap } from "../intelligence/reality-gap.js";
import { enrichObservationsWithRank } from "./executive-metrics-foundation.js";
import {
  isCustomerTrendEligible,
  periodsAreContractCompatible,
} from "../period-eligibility-v1.js";
import { isTargetedMeasurementPeriod } from "../data-model.js";

function propertyRealityCoverageForPeriod(period, propertyProfile) {
  const observations = enrichObservationsWithRank(period.observations || [], propertyProfile);
  const rg = computeRealityGap(observations.filter((o) => o.parsed), propertyProfile);
  if (!rg.totalAttributes) return null;
  return roundAdpPercent((rg.recognizedCount / rg.totalAttributes) * 100);
}

export function scenarioUniverseFingerprint(scenarios) {
  return scenarios
    .map((s) => s.scenarioId)
    .sort()
    .join("|");
}

export function arePeriodsComparable(currentPeriod, priorPeriod, scenarios) {
  if (!currentPeriod || !priorPeriod) return { comparable: false, reason: "missing_period" };
  if (isTargetedMeasurementPeriod(currentPeriod) || isTargetedMeasurementPeriod(priorPeriod)) {
    return { comparable: false, reason: "targeted_period_excluded" };
  }
  // Official customer history: only customer-trend-eligible periods may compare.
  // If current is official, prior must also be official + contract-compatible.
  if (currentPeriod.officialPeriod === true || priorPeriod.officialPeriod === true) {
    const contract = periodsAreContractCompatible(currentPeriod, priorPeriod);
    if (!contract.comparable) {
      return { comparable: false, reason: contract.reason || "contract_incompatible" };
    }
  } else if (!isCustomerTrendEligible(currentPeriod) || !isCustomerTrendEligible(priorPeriod)) {
    // Pre-baseline vs pre-baseline remains allowed for internal research only when
    // neither side is official. Customer surfaces filter these out separately.
  }
  const fp = scenarioUniverseFingerprint(scenarios);
  const curSc = currentPeriod.scenarioCount ?? new Set((currentPeriod.observations || []).map((o) => o.scenarioId)).size;
  const priSc = priorPeriod.scenarioCount ?? new Set((priorPeriod.observations || []).map((o) => o.scenarioId)).size;
  if (curSc !== priSc) return { comparable: false, reason: "scenario_count_mismatch" };
  if ((currentPeriod.providerCount || 4) !== (priorPeriod.providerCount || 4)) {
    return { comparable: false, reason: "provider_scope_mismatch" };
  }
  if (!currentPeriod.observations?.length || !priorPeriod.observations?.length) {
    return { comparable: false, reason: "missing_observations" };
  }
  return { comparable: true, reason: null, fingerprint: fp };
}

export function selectPriorComparablePeriod(currentPeriod, allPeriods, scenarios) {
  const pool = (allPeriods || []).filter((p) => {
    if (isTargetedMeasurementPeriod(p)) return false;
    if (currentPeriod?.officialPeriod === true) return isCustomerTrendEligible(p);
    return true;
  });
  const sorted = [...pool].sort((a, b) =>
    String(a.executionDate || "").localeCompare(String(b.executionDate || ""))
  );
  const currentIdx = sorted.findIndex((p) => p.periodId === currentPeriod.periodId);
  const skipped = [];
  let prior = null;
  for (let i = (currentIdx >= 0 ? currentIdx : sorted.length) - 1; i >= 0; i--) {
    const candidate = sorted[i];
    if (candidate.periodId === currentPeriod.periodId) continue;
    const check = arePeriodsComparable(currentPeriod, candidate, scenarios);
    if (check.comparable) {
      prior = candidate;
      break;
    }
    skipped.push({ periodId: candidate.periodId, reason: check.reason });
  }
  return { priorPeriod: prior, periodsSkippedAsIncomparable: skipped };
}

function ppDelta(current, prior) {
  if (current == null || prior == null) return null;
  return Math.round((current - prior) * 10) / 10;
}

export function buildLongitudinalComparison(currentPeriod, allPeriods, scenarios, propertyProfile) {
  const { priorPeriod, periodsSkippedAsIncomparable } = selectPriorComparablePeriod(
    currentPeriod,
    allPeriods,
    scenarios
  );

  const realComparablePeriods = allPeriods.filter((p) =>
    arePeriodsComparable(currentPeriod, p, scenarios).comparable
  ).length;

  const currentFoundation = buildExecutiveMetricsFoundation(currentPeriod, scenarios, propertyProfile, {
    periodCount: allPeriods.length,
    enrichRank: true,
  });

  let priorFoundation = null;
  let deltas = null;
  if (priorPeriod) {
    priorFoundation = buildExecutiveMetricsFoundation(priorPeriod, scenarios, propertyProfile, {
      periodCount: allPeriods.length,
      enrichRank: true,
    });
    deltas = {
      aiConsiderationRate: ppDelta(
        currentFoundation.consideration.observationConsiderationRate,
        priorFoundation.consideration.observationConsiderationRate
      ),
      aiScenarioPresence: ppDelta(
        currentFoundation.consideration.scenarioConsiderationCoverage,
        priorFoundation.consideration.scenarioConsiderationCoverage
      ),
      propertyRealityCoverage: ppDelta(
        propertyRealityCoverageForPeriod(currentPeriod, propertyProfile),
        propertyRealityCoverageForPeriod(priorPeriod, propertyProfile)
      ),
      numberOneAppearanceRate:
        currentFoundation.position.rankEligibleObservations >= 5 &&
        priorFoundation.position.rankEligibleObservations >= 5
          ? ppDelta(currentFoundation.position.numberOneRate, priorFoundation.position.numberOneRate)
          : null,
    };

    const territoryDeltas = {};
    for (const row of currentFoundation.demandPositionMap.rows || []) {
      const priorRow = (priorFoundation.demandPositionMap.rows || []).find((r) => r.intent === row.intent);
      territoryDeltas[row.intent] = {
        observationConsiderationRate: ppDelta(
          row.observationConsiderationRate,
          priorRow?.observationConsiderationRate
        ),
        scenarioConsiderationCoverage: ppDelta(
          row.scenarioConsiderationCoverage,
          priorRow?.scenarioConsiderationCoverage
        ),
      };
    }
    deltas.byTerritory = territoryDeltas;
  }

  const realComparableCount = realComparablePeriods;
  return {
    totalPeriodFiles: allPeriods.length,
    realComparablePeriods: realComparableCount,
    currentPeriodId: currentPeriod.periodId,
    priorComparablePeriodId: priorPeriod?.periodId || null,
    periodsSkippedAsIncomparable,
    currentVsPriorReady: !!priorPeriod,
    trendResearchReady: realComparableCount >= 3,
    customerTrendReady: false,
    deltas,
    note: "Two comparable periods = current vs prior only. Not a trend.",
  };
}

export function formatPpDelta(delta) {
  if (delta == null || !Number.isFinite(delta)) return "—";
  return (delta > 0 ? "+" : "") + delta.toFixed(1) + " pp";
}
