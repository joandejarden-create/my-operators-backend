/**
 * AI Consideration Rate + Demand Scenario Coverage.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations, METRIC_GRAINS } from "./grain-governance.js";
import {
  buildScenarioEligibilityMap,
  CONSIDERATION_ELIGIBLE_STATES,
  scenariosForEligibility,
} from "./scenario-eligibility.js";

export function computeConsiderationMetrics(observations, scenarios, profile) {
  const eligibility = buildScenarioEligibilityMap(scenarios, profile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleScenarioIds = new Set(eligibleScenarios.map((s) => s.scenarioId));

  const comparable = filterComparableObservations(observations).filter((o) =>
    eligibleScenarioIds.has(o.scenarioId)
  );

  const presentObs = comparable.filter((o) => o.mentioned);
  const observationConsiderationRate =
    comparable.length > 0 ? roundAdpPercent((presentObs.length / comparable.length) * 100) : null;

  const scenarioPresence = new Map();
  for (const obs of comparable) {
    if (!scenarioPresence.has(obs.scenarioId)) scenarioPresence.set(obs.scenarioId, false);
    if (obs.mentioned) scenarioPresence.set(obs.scenarioId, true);
  }

  let capturedScenarios = 0;
  for (const scenario of eligibleScenarios) {
    if (scenarioPresence.get(scenario.scenarioId)) capturedScenarios += 1;
  }

  const scenarioConsiderationCoverage =
    eligibleScenarios.length > 0
      ? roundAdpPercent((capturedScenarios / eligibleScenarios.length) * 100)
      : null;

  const byIntent = {};
  for (const scenario of eligibleScenarios) {
    const intent = scenario.intent;
    if (!byIntent[intent]) {
      byIntent[intent] = { totalScenarios: 0, capturedScenarios: 0, totalObservations: 0, presentObservations: 0 };
    }
    byIntent[intent].totalScenarios += 1;
    if (scenarioPresence.get(scenario.scenarioId)) byIntent[intent].capturedScenarios += 1;
  }
  for (const obs of comparable) {
    const scenario = eligibleScenarios.find((s) => s.scenarioId === obs.scenarioId);
    if (!scenario) continue;
    byIntent[scenario.intent].totalObservations += 1;
    if (obs.mentioned) byIntent[scenario.intent].presentObservations += 1;
  }
  for (const intent of Object.keys(byIntent)) {
    const row = byIntent[intent];
    row.observationConsiderationRate =
      row.totalObservations > 0
        ? roundAdpPercent((row.presentObservations / row.totalObservations) * 100)
        : null;
    row.scenarioConsiderationCoverage =
      row.totalScenarios > 0
        ? roundAdpPercent((row.capturedScenarios / row.totalScenarios) * 100)
        : null;
  }

  return {
    observationConsiderationRate,
    scenarioConsiderationCoverage,
    comparableObservations: comparable.length,
    presentObservations: presentObs.length,
    eligibleScenarios: eligibleScenarios.length,
    capturedScenarios,
    observationGrain: METRIC_GRAINS.OBSERVATION,
    scenarioGrain: METRIC_GRAINS.SCENARIO,
    byIntent,
    customerSafe: true,
    semanticNote:
      "Property mention in comparable monitored AI response treated as AI consideration presence; distinct from booking demand or verified recommendation.",
  };
}
