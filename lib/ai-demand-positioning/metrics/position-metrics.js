/**
 * Rank-based appearance metrics — rank-eligible observations only.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "./grain-governance.js";
import {
  buildScenarioEligibilityMap,
  CONSIDERATION_ELIGIBLE_STATES,
  scenariosForEligibility,
} from "./scenario-eligibility.js";

export const MIN_RANK_SAMPLE = 5;

export function isRankEligibleObservation(obs) {
  return obs?.mentioned === true && obs?.rankEligible === true && obs?.position != null;
}

export function computePositionMetrics(observations, scenarios, profile) {
  const eligibility = buildScenarioEligibilityMap(scenarios, profile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));

  const comparable = filterComparableObservations(observations).filter((o) => eligibleIds.has(o.scenarioId));
  const present = comparable.filter((o) => o.mentioned);
  const rankEligible = comparable.filter((o) => isRankEligibleObservation(o));
  const withPosition = rankEligible.filter((o) => Number.isFinite(o.position));

  const top3 = withPosition.filter((o) => o.position <= 3);
  const numberOne = withPosition.filter((o) => o.position === 1);

  const top3Rate =
    rankEligible.length >= MIN_RANK_SAMPLE
      ? roundAdpPercent((top3.length / rankEligible.length) * 100)
      : null;
  const numberOneRate =
    rankEligible.length >= MIN_RANK_SAMPLE
      ? roundAdpPercent((numberOne.length / rankEligible.length) * 100)
      : null;

  const positions = withPosition.map((o) => o.position).sort((a, b) => a - b);
  let mean = null;
  let median = null;
  if (positions.length >= MIN_RANK_SAMPLE) {
    mean = Math.round((positions.reduce((a, b) => a + b, 0) / positions.length) * 10) / 10;
    const mid = Math.floor(positions.length / 2);
    median = positions.length % 2 ? positions[mid] : (positions[mid - 1] + positions[mid]) / 2;
  }

  const formatCounts = {};
  for (const obs of withPosition) {
    const fmt = obs.rankSource || "unknown";
    formatCounts[fmt] = (formatCounts[fmt] || 0) + 1;
  }

  return {
    propertyPresentObservations: present.length,
    positionDetected: withPosition.length,
    positionNull: present.length - withPosition.length,
    positionDetectionRate:
      present.length > 0 ? roundAdpPercent((withPosition.length / present.length) * 100) : null,
    rankEligibleObservations: rankEligible.length,
    top3Count: top3.length,
    numberOneCount: numberOne.length,
    top3Rate,
    numberOneRate,
    averagePosition: mean,
    medianPosition: median,
    positionDistribution: positions,
    positionFormatTypes: formatCounts,
    minSampleRequired: MIN_RANK_SAMPLE,
    productionReady:
      rankEligible.length >= MIN_RANK_SAMPLE ? "PARTIAL" : "NO",
    customerLabels: {
      top3: "#1 Appearance Rate uses rank-eligible denominator only",
      numberOne: "#1 Appearance Rate",
      average: "Average Detected Position",
    },
  };
}

export function auditPositionDetection(observations) {
  const present = filterComparableObservations(observations).filter((o) => o.mentioned);
  const detected = present.filter((o) => isRankEligibleObservation(o));
  return {
    totalPropertyPresentObservations: present.length,
    positionDetected: detected.length,
    positionNull: present.length - detected.length,
    positionDetectionRate:
      present.length > 0 ? roundAdpPercent((detected.length / present.length) * 100) : null,
  };
}
