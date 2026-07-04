/**
 * Generate a single country build plan payload from config + live counts.
 */

import { resolveStrategyTargets } from "./country-build-strategies.js";
import { evaluateBuildStatus, recommendNextAction } from "./evaluate-build-status.js";
import { resolveActiveSubmarkets, resolveActiveMarketTargets } from "./country-configs.js";

/**
 * @param {string} country
 * @param {object} config
 * @param {object} live — from fetchLiveCountsForCountry or byCountry bucket
 */
export function buildCountryPlanPayload(country, config, live) {
  const activeTargets = resolveActiveMarketTargets(config) || config.targets;
  const enrichedConfig = {
    ...config,
    country,
    targets: activeTargets,
    submarkets: resolveActiveSubmarkets(config),
  };

  const summary = live.summary || live;
  const targets = resolveStrategyTargets(enrichedConfig);
  const current = {
    demandAnchors: summary.demandAnchors || 0,
    travelInfrastructure: summary.travelInfrastructure || 0,
    totalRadarPoints: summary.totalRadarPoints || 0,
  };
  const coverage = {
    sourceCoveragePct: summary.sourceCoveragePct || 0,
    coordinateCoveragePct: summary.coordinateCoveragePct || 0,
    dataConfidenceMix: summary.dataConfidenceMix || {},
  };

  const evaluation = evaluateBuildStatus({
    config: enrichedConfig,
    current: {
      ...current,
      submarkets: summary.submarkets,
      demandAnchorPoints: live.demandAnchors || summary.demandAnchorPoints || [],
      travelInfraPoints: live.travelInfrastructure || summary.travelInfraPoints || [],
    },
    coverage,
  });

  const buildStatus =
    config.manualStatus && current.totalRadarPoints === 0
      ? config.manualStatus
      : evaluation.buildStatus;

  return {
    country,
    region: config.region || "",
    buildStrategy: config.buildStrategy,
    priorityTier: config.priorityTier || "Future",
    buildStatus,
    targets,
    current,
    coverage,
    submarkets: enrichedConfig.submarkets || [],
    primaryHotelDemandProfile:
      config.primaryHotelDemandProfiles?.[0] || config.primaryHotelDemandProfile || "",
    recommendedBuildSequence: config.recommendedBuildSequence ?? null,
    nextBuildMarket: config.nextBuildMarket || "",
    buildApproachNotes: config.buildApproachNotes || "",
    firstPassTargetDescription: config.firstPassTargetDescription || "",
    lastBuildDate: new Date().toISOString().slice(0, 10),
    lastQaDate: "",
    nextRecommendedAction: recommendNextAction({ ...config, country }, { buildStatus }),
    notes: config.notes || "",
    evaluationReason: evaluation.reason,
  };
}
