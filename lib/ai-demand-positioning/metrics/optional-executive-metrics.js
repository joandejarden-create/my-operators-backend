/**
 * Optional additive executive metrics — backward-compatible with legacy ADP payloads.
 * Never required by UI; derived offline from stored observations only.
 */

import { computeConsiderationMetrics } from "./consideration-rate.js";
import { computePositionMetrics, MIN_RANK_SAMPLE } from "./position-metrics.js";
import { computeCompetitorPresentGaps } from "./competitor-present-gaps.js";
import { enrichCompetitorPresentPayload } from "./competitor-present-scenario-copy-v1.js";
import { buildLongitudinalComparison } from "./longitudinal-comparability.js";
import { enrichObservationsWithRank } from "./executive-metrics-foundation.js";
import {
  applyUnpromptedStandardToUniverse,
  UNPROMPTED_METHODOLOGY_VERSION,
  ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
  HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID,
  CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
} from "../certification/adp-unprompted-independent-discovery-standard-v1.js";

export const OPTIONAL_EXECUTIVE_METRICS_VERSION = "adp_optional_executive_metrics_v2";
/** When unprompted filter applied: appearance KPIs use D1-eligible scenarios only. */
export const OPTIONAL_EXECUTIVE_METRICS_UNPROMPTED_VERSION =
  "adp_optional_executive_metrics_v2_unprompted_v1";

function isFiniteNumber(value) {
  return value != null && Number.isFinite(Number(value));
}

/**
 * Build optional executiveMetrics block. Returns null when nothing derivable.
 */
export function buildOptionalExecutiveMetrics(period, scenarios, propertyProfile, options = {}) {
  const observations = (period?.observations || []).filter((o) => o.parsed);
  if (!observations.length || !scenarios?.length || !propertyProfile) return null;

  let scenariosForAppearance = scenarios;
  let unpromptedMeta = null;
  if (options.unpromptedIndependentDiscovery === true) {
    unpromptedMeta = applyUnpromptedStandardToUniverse({
      propertyId: propertyProfile.propertyId || period?.propertyId,
      periodId: period?.periodId,
      scenarios,
      propertyProfile,
      promptMap: options.promptMap || null,
    });
    scenariosForAppearance = unpromptedMeta.eligibleScenarios;
    if (!scenariosForAppearance.length) {
      // Fail closed: do not fall back to contaminated universe for appearance KPIs.
      scenariosForAppearance = [];
    }
  }

  const enriched = enrichObservationsWithRank(observations, propertyProfile);
  const consideration = computeConsiderationMetrics(
    enriched,
    scenariosForAppearance,
    propertyProfile
  );
  const position = computePositionMetrics(enriched, scenariosForAppearance, propertyProfile);
  const gaps = computeCompetitorPresentGaps(enriched, scenariosForAppearance, propertyProfile);

  const result = {
    version: unpromptedMeta
      ? OPTIONAL_EXECUTIVE_METRICS_UNPROMPTED_VERSION
      : OPTIONAL_EXECUTIVE_METRICS_VERSION,
  };
  if (unpromptedMeta) {
    result.unpromptedIndependentDiscovery = {
      standard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
      methodologyVersion: UNPROMPTED_METHODOLOGY_VERSION,
      ...unpromptedMeta.methodologyStamp,
      coverage: unpromptedMeta.coverage,
      researchScenarioInventory: unpromptedMeta.counts.total,
      independentEligibleScenarios: unpromptedMeta.counts.eligible,
      excludedScenarios: unpromptedMeta.counts.excluded,
    };
  }

  if (isFiniteNumber(consideration.observationConsiderationRate)) {
    result.considerationRate = {
      rate: consideration.observationConsiderationRate,
      presentObservations: consideration.presentObservations,
      comparableObservations: consideration.comparableObservations,
      methodologyId: unpromptedMeta
        ? CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID
        : HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID,
      historicalMethodologyId: HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID,
      ...(unpromptedMeta
        ? { correctedMethodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID }
        : {}),
    };
  }

  if (isFiniteNumber(consideration.scenarioConsiderationCoverage)) {
    result.scenarioPresence = {
      rate: consideration.scenarioConsiderationCoverage,
      capturedScenarios: consideration.capturedScenarios,
      eligibleScenarios: consideration.eligibleScenarios,
      methodologyId: unpromptedMeta
        ? CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID
        : HISTORICAL_MENTION_CONSIDERATION_METHODOLOGY_ID,
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

  // Always emit competitorPresentScenarios when computable — including zero.
  // Omitting zero previously caused false POPULATED_NOT_CERTIFIED section gaps.
  if (
    gaps.competitorPresentScenarios != null &&
    Number.isFinite(Number(gaps.competitorPresentScenarios))
  ) {
    result.competitorPresentScenarios = enrichCompetitorPresentPayload({
      scenarioCount: gaps.competitorPresentScenarios,
      observationCount: gaps.competitorPresentObservations ?? 0,
      fullyAbsentScenarioCount: gaps.competitorPresentScenariosFullyAbsent ?? null,
      countingGrain: "RESPONSE_LEVEL_ABSENCE",
      fullyAbsentCountingGrain: "SCENARIO_LEVEL_ABSENCE",
      labelNote:
        "scenarioCount counts traveler needs with ≥1 provider answer omitting the subject while a competitor appears — not full scenario absence.",
    });
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
 * When executiveMetrics already exists, still patch missing currentVsPrior from
 * the canonical comparable-period lineage (Prior Run + Trends parity).
 */
export function attachOptionalExecutiveMetrics(payload, period, scenarios, propertyProfile, options = {}) {
  if (!payload) return payload;
  try {
    const existing = payload.executiveMetrics;
    if (!existing) {
      const executiveMetrics = buildOptionalExecutiveMetrics(period, scenarios, propertyProfile, options);
      if (!executiveMetrics) return payload;
      return { ...payload, executiveMetrics };
    }
    const built = buildOptionalExecutiveMetrics(period, scenarios, propertyProfile, options);
    const missingCompetitorPresent =
      existing.competitorPresentScenarios == null && built?.competitorPresentScenarios;
    if (missingCompetitorPresent && (!Array.isArray(options.allPeriods) || options.allPeriods.length < 2)) {
      return {
        ...payload,
        executiveMetrics: {
          ...existing,
          competitorPresentScenarios: built.competitorPresentScenarios,
        },
      };
    }
    if (existing.currentVsPrior?.priorComparablePeriodId && !missingCompetitorPresent) return payload;
    if (!Array.isArray(options.allPeriods) || options.allPeriods.length < 2) return payload;
    if (!built?.currentVsPrior && !missingCompetitorPresent) return payload;
    return {
      ...payload,
      executiveMetrics: {
        ...existing,
        ...(missingCompetitorPresent
          ? { competitorPresentScenarios: built.competitorPresentScenarios }
          : {}),
        ...(built?.currentVsPrior ? { currentVsPrior: built.currentVsPrior } : {}),
      },
    };
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] attachOptionalExecutiveMetrics failed:", err.message);
    }
    return payload;
  }
}
