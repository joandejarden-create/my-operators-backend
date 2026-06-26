/**
 * Evaluate CALA Radar Buildout status from live counts + strategy rules.
 */

import { COVERAGE_STATUS, getBuildStrategyDefinition } from "./country-build-strategies.js";
import {
  countCorridorsCovered,
  countDistinctPointTypes,
} from "./coverage-metrics.js";

/**
 * @param {object} input
 * @param {object} input.config — country config
 * @param {object} input.current — live counts summary
 * @param {object} [input.coverage]
 */
export function evaluateBuildStatus(input) {
  const config = input.config || {};
  const current = input.current || {};
  const coverage = input.coverage || {};
  const strategy = getBuildStrategyDefinition(config.buildStrategy);
  const criteria = strategy?.readinessCriteria || {};

  const total = current.totalRadarPoints || 0;
  const da = current.demandAnchors || 0;
  const ti = current.travelInfrastructure || 0;
  const corridors = countCorridorsCovered(current.submarkets);
  const markets = countCorridorsCovered(current.markets || current.submarkets);
  const pointTypes = countDistinctPointTypes([
    ...(current.demandAnchorPoints || []),
    ...(current.travelInfraPoints || []),
  ]);
  const sourcePct = coverage.sourceCoveragePct ?? current.sourceCoveragePct ?? 0;
  const coordPct = coverage.coordinateCoveragePct ?? current.coordinateCoveragePct ?? 0;

  if (total === 0 && !config.manualStatus) {
    return {
      buildStatus: COVERAGE_STATUS.NOT_STARTED,
      reason: "No radar points for country",
    };
  }

  if (total === 0 && config.manualStatus) {
    return { buildStatus: config.manualStatus, reason: "Manual config status" };
  }

  const seeded = criteria.seeded || { minTotalPoints: 10 };
  if (total < seeded.minTotalPoints) {
    return {
      buildStatus: COVERAGE_STATUS.PLANNED,
      reason: `Below seeded threshold (${total}/${seeded.minTotalPoints})`,
    };
  }

  const intel = criteria.intelligenceReady;
  if (
    intel &&
    total >= (intel.minTotalPoints || 9999) &&
    da >= (intel.minDemandAnchors || 0) &&
    pointTypes >= (intel.minPointTypes || 8) &&
    sourcePct >= 90 &&
    coordPct >= 95
  ) {
    const corridorOk =
      !intel.minCorridorsCovered || corridors >= intel.minCorridorsCovered;
    const marketOk = !intel.minMarketsCovered || markets >= intel.minMarketsCovered;
    if (corridorOk && marketOk) {
      return { buildStatus: COVERAGE_STATUS.INTELLIGENCE_READY, reason: "Intelligence thresholds met" };
    }
  }

  const deal = criteria.dealReady;
  if (
    deal &&
    total >= (deal.minTotalPoints || 55) &&
    da >= (deal.minDemandAnchors || 40) &&
    ti >= (deal.minTravelInfra || 10)
  ) {
    const corridorOk = !deal.minCorridorsCovered || corridors >= deal.minCorridorsCovered;
    const marketOk = !deal.minMarketsCovered || markets >= deal.minMarketsCovered;
    const submarketOk = !deal.requireSubmarketTagging || corridors >= 3;
    if (corridorOk && marketOk && submarketOk) {
      return { buildStatus: COVERAGE_STATUS.DEAL_READY, reason: "Deal-ready thresholds met" };
    }
  }

  const market = criteria.marketReady;
  if (
    market &&
    total >= (market.minTotalPoints || 40) &&
    da >= (market.minDemandAnchors || 30) &&
    ti >= (market.minTravelInfra || 8)
  ) {
    return { buildStatus: COVERAGE_STATUS.MARKET_READY, reason: "Market-ready thresholds met" };
  }

  if (total >= seeded.minTotalPoints) {
    return { buildStatus: COVERAGE_STATUS.SEEDED, reason: "Seeded threshold met" };
  }

  return { buildStatus: COVERAGE_STATUS.NEEDS_REVIEW, reason: "Could not classify — manual review" };
}

/**
 * @param {object} config
 * @param {object} evaluation
 */
export function recommendNextAction(config, evaluation) {
  const country = config.country || "Country";
  const status = evaluation.buildStatus;

  if (status === COVERAGE_STATUS.NOT_STARTED || status === COVERAGE_STATUS.PLANNED) {
    if (country === "Dominican Republic") {
      return "Research source-backed DR corridor anchors; run build:dominican-republic-fixtures then preview import.";
    }
    return `Create ${country} fixtures from country config submarkets; preview import before commit.`;
  }
  if (status === COVERAGE_STATUS.SEEDED) {
    return `Expand ${country} countrywide/corridor pass to reach Market Ready targets.`;
  }
  if (status === COVERAGE_STATUS.MARKET_READY) {
    return `Fill travel infrastructure gaps and complete submarket tagging for Deal Ready.`;
  }
  if (status === COVERAGE_STATUS.DEAL_READY) {
    return `Broaden point-type diversity and confidence mix for Intelligence Ready.`;
  }
  if (status === COVERAGE_STATUS.INTELLIGENCE_READY) {
    return `Maintain QA; ready for Market Demand bridge when approved.`;
  }
  return "Review build plan manually.";
}
