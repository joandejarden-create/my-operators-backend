/**
 * AI Demand Positioning — Demand Capture Index.
 * Headline KPI: what share of relevant demand scenarios does this property appear in?
 */

import { formatAdpPercent, roundAdpPercent } from "../format-percent.js";

export function computeDemandCaptureIndex(observations, scenarios) {
  const byIntent = {};
  const allIntents = [...new Set(scenarios.map((s) => s.intent))];
  for (const intent of allIntents) {
    byIntent[intent] = { total: 0, captured: 0, rate: 0 };
  }

  const scenarioResults = new Map();
  for (const obs of observations) {
    const key = obs.scenarioId;
    if (!scenarioResults.has(key)) scenarioResults.set(key, { mentioned: false, providers: 0, mentionedProviders: 0 });
    const entry = scenarioResults.get(key);
    entry.providers += 1;
    if (obs.mentioned) {
      entry.mentioned = true;
      entry.mentionedProviders += 1;
    }
  }

  let totalScenarios = 0;
  let capturedScenarios = 0;

  for (const scenario of scenarios) {
    const result = scenarioResults.get(scenario.scenarioId);
    const intent = scenario.intent;
    if (byIntent[intent]) byIntent[intent].total += 1;
    totalScenarios += 1;

    if (result?.mentioned) {
      capturedScenarios += 1;
      if (byIntent[intent]) byIntent[intent].captured += 1;
    }
  }

  for (const intent of allIntents) {
    const i = byIntent[intent];
    i.rate = i.total > 0 ? roundAdpPercent((i.captured / i.total) * 100) : 0;
  }

  const overallRate = totalScenarios > 0 ? roundAdpPercent((capturedScenarios / totalScenarios) * 100) : 0;

  return {
    overallRate,
    totalScenarios,
    capturedScenarios,
    missedScenarios: totalScenarios - capturedScenarios,
    byIntent,
    display: formatAdpPercent(overallRate),
  };
}
