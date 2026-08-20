/**
 * Optional additive executive metrics — backward-compatible with legacy ADP payloads.
 * Never required by UI; derived offline from stored observations only.
 */

import { computeConsiderationMetrics } from "./consideration-rate.js";
import { computePositionMetrics, MIN_RANK_SAMPLE } from "./position-metrics.js";
import { computeCompetitorPresentGaps } from "./competitor-present-gaps.js";
import { buildLongitudinalComparison } from "./longitudinal-comparability.js";
import { enrichObservationsWithRank } from "./executive-metrics-foundation.js";

export const OPTIONAL_EXECUTIVE_METRICS_VERSION = "adp_optional_executive_metrics_v2";

function isFiniteNumber(value) {
  return value != null && Number.isFinite(Number(value));
}

/**
 * Build optional executiveMetrics block. Returns null when nothing derivable.
 */
export function buildOptionalExecutiveMetrics(period, scenarios, propertyProfile, options = {}) {
  const observations = (period?.observations || []).filter((o) => o.parsed);
  if (!observations.length || !scenarios?.length || !propertyProfile) return null;

  const enriched = enrichObservationsWithRank(observations, propertyProfile);
  const consideration = computeConsiderationMetrics(enriched, scenarios, propertyProfile);
  const position = computePositionMetrics(enriched, scenarios, propertyProfile);
  const gaps = computeCompetitorPresentGaps(enriched, scenarios, propertyProfile);

  const result = { version: OPTIONAL_EXECUTIVE_METRICS_VERSION };

  if (isFiniteNumber(consideration.observationConsiderationRate)) {
    result.considerationRate = {
      rate: consideration.observationConsiderationRate,
      presentObservations: consideration.presentObservations,
      comparableObservations: consideration.comparableObservations,
    };
  }

  if (isFiniteNumber(consideration.scenarioConsiderationCoverage)) {
    result.scenarioPresence = {
      rate: consideration.scenarioConsiderationCoverage,
      capturedScenarios: consideration.capturedScenarios,
      eligibleScenarios: consideration.eligibleScenarios,
    };
  }

  if (
    position.rankEligibleObservations >= MIN_RANK_SAMPLE &&
    isFiniteNumber(position.numberOneRate) &&
    isFiniteNumber(position.top3Rate)
  ) {
    result.rankMetrics = {
      rankEligibleN: position.rankEligibleObservations,
      numberOneAppearanceRate: position.numberOneRate,
      topThreeAppearanceRate: position.top3Rate,
      numberOneCount: position.numberOneCount,
      topThreeCount: position.top3Count,
    };
  }

  if (gaps.competitorPresentScenarios > 0) {
    result.competitorPresentScenarios = {
      scenarioCount: gaps.competitorPresentScenarios,
      observationCount: gaps.competitorPresentObservations,
    };
  }

  const allPeriods = options.allPeriods;
  if (Array.isArray(allPeriods) && allPeriods.length >= 2) {
    try {
      const longitudinal = buildLongitudinalComparison(period, allPeriods, scenarios, propertyProfile);
      if (longitudinal.currentVsPriorReady && longitudinal.deltas) {
        const deltas = {};
        if (isFiniteNumber(longitudinal.deltas.aiConsiderationRate)) {
          deltas.considerationRate = longitudinal.deltas.aiConsiderationRate;
        }
        if (isFiniteNumber(longitudinal.deltas.aiScenarioPresence)) {
          deltas.scenarioPresence = longitudinal.deltas.aiScenarioPresence;
        }
        if (isFiniteNumber(longitudinal.deltas.numberOneAppearanceRate)) {
          deltas.numberOneAppearanceRate = longitudinal.deltas.numberOneAppearanceRate;
        }
        if (Object.keys(deltas).length) {
          result.currentVsPrior = {
            currentPeriodId: longitudinal.currentPeriodId,
            priorComparablePeriodId: longitudinal.priorComparablePeriodId,
            deltas,
          };
        }
      }
    } catch (_) {
      // Optional delta only — never fail payload build.
    }
  }

  const hasMetrics =
    result.considerationRate ||
    result.scenarioPresence ||
    result.rankMetrics ||
    result.competitorPresentScenarios;

  return hasMetrics ? result : null;
}

/**
 * Safely attach optional metrics without mutating legacy snapshot fields.
 */
export function attachOptionalExecutiveMetrics(payload, period, scenarios, propertyProfile, options = {}) {
  if (!payload || payload.executiveMetrics) return payload;
  try {
    const executiveMetrics = buildOptionalExecutiveMetrics(period, scenarios, propertyProfile, options);
    if (!executiveMetrics) return payload;
    return { ...payload, executiveMetrics };
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] attachOptionalExecutiveMetrics failed:", err.message);
    }
    return payload;
  }
}
