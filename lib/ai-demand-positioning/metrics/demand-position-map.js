/**
 * Unified Demand Position Map — progressive column rendering.
 */

import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { computeConsiderationMetrics } from "./consideration-rate.js";
import { computePositionMetrics, MIN_RANK_SAMPLE } from "./position-metrics.js";
import { buildScenarioEligibilityMap, CONSIDERATION_ELIGIBLE_STATES, scenariosForEligibility } from "./scenario-eligibility.js";
import { filterComparableObservations } from "./grain-governance.js";

export function buildDemandPositionMap(observations, scenarios, profile, options = {}) {
  const consideration = computeConsiderationMetrics(observations, scenarios, profile);
  const position = computePositionMetrics(observations, scenarios, profile);
  const eligibility = buildScenarioEligibilityMap(scenarios, profile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));
  const comparable = filterComparableObservations(observations).filter((o) => eligibleIds.has(o.scenarioId));

  const intents = [...new Set(eligibleScenarios.map((s) => s.intent))];
  const rows = [];

  for (const intent of intents) {
    const intentScenarios = eligibleScenarios.filter((s) => s.intent === intent);
    const intentObs = comparable.filter((o) => intentScenarios.some((s) => s.scenarioId === o.scenarioId));
    const rankEligible = intentObs.filter((o) => o.rankEligible && o.position != null);
    const top3 = rankEligible.filter((o) => o.position <= 3);
    const numberOne = rankEligible.filter((o) => o.position === 1);
    const gaps = intentObs.filter((o) => !o.mentioned && (o.competitorsMentioned?.length || 0) > 0).length;

    const intentConsideration = consideration.byIntent[intent] || {};
    const row = {
      territory: territoryLabelForIntent(intent),
      intent,
      observationConsiderationRate: intentConsideration.observationConsiderationRate,
      scenarioConsiderationCoverage: intentConsideration.scenarioConsiderationCoverage,
      top3Rate:
        rankEligible.length >= MIN_RANK_SAMPLE
          ? Math.round((top3.length / rankEligible.length) * 1000) / 10
          : null,
      numberOneRate:
        rankEligible.length >= MIN_RANK_SAMPLE
          ? Math.round((numberOne.length / rankEligible.length) * 1000) / 10
          : null,
      competitorPresentGaps: gaps,
      opportunity: gaps >= 3 ? "HIGH" : gaps >= 1 ? "MEDIUM" : "LOW",
      chgVsPrior: null,
    };

    row.fieldsReady = [];
    row.fieldsWithheld = [];
    if (row.observationConsiderationRate != null) row.fieldsReady.push("observationConsiderationRate");
    else row.fieldsWithheld.push("observationConsiderationRate");
    if (row.scenarioConsiderationCoverage != null) row.fieldsReady.push("scenarioConsiderationCoverage");
    if (row.top3Rate != null) row.fieldsReady.push("top3Rate");
    else row.fieldsWithheld.push("top3Rate");
    if (row.numberOneRate != null) row.fieldsReady.push("numberOneRate");
    else row.fieldsWithheld.push("numberOneRate");
    row.fieldsReady.push("competitorPresentGaps");
    row.fieldsWithheld.push("chgVsPrior");

    rows.push(row);
  }

  rows.sort((a, b) => (b.competitorPresentGaps || 0) - (a.competitorPresentGaps || 0));

  return {
    territories: rows.length,
    customerVisibleRows: rows,
    fieldsReadyGlobal: [
      "observationConsiderationRate",
      "scenarioConsiderationCoverage",
      "competitorPresentGaps",
      ...(position.rankEligibleObservations >= MIN_RANK_SAMPLE ? ["top3Rate", "numberOneRate"] : []),
    ],
    fieldsWithheldGlobal: [
      "chgVsPrior",
      ...(position.rankEligibleObservations < MIN_RANK_SAMPLE ? ["top3Rate", "numberOneRate", "averagePosition"] : []),
      "aiConsiderationIndex",
    ],
    rows,
  };
}
