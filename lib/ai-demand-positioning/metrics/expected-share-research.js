/**
 * Expected Consideration Share — RESEARCH ONLY (V1).
 * No customer AI Consideration Index until certified.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "./grain-governance.js";
import { buildScenarioEligibilityMap, CONSIDERATION_ELIGIBLE_STATES, scenariosForEligibility } from "./scenario-eligibility.js";
import { matchesDeclaredComp, canonicalizeCompetitorName } from "../intelligence/competitor-name-resolution.js";

const MIN_RECURRENCE = 2;

function subjectAppearances(observations, eligibleIds) {
  return filterComparableObservations(observations).filter(
    (o) => eligibleIds.has(o.scenarioId) && o.mentioned
  ).length;
}

function totalEligibleAppearances(observations, eligibleIds, universeNames) {
  let total = 0;
  for (const obs of filterComparableObservations(observations)) {
    if (!eligibleIds.has(obs.scenarioId)) continue;
    const seen = new Set();
    if (obs.mentioned) {
      seen.add("__subject__");
      total += 1;
    }
    for (const comp of obs.competitorsMentioned || []) {
      const key = (canonicalizeCompetitorName(comp) || comp).toLowerCase();
      if (universeNames.has(key) && !seen.has(key)) {
        seen.add(key);
        total += 1;
      }
    }
  }
  return total;
}

function buildGovernedUniverse(observations, propertyProfile) {
  const declared = (propertyProfile.declaredCompSet || []).map((d) => d.toLowerCase());
  const recurring = new Set();
  const counts = {};
  for (const obs of filterComparableObservations(observations)) {
    for (const comp of obs.competitorsMentioned || []) {
      const name = (canonicalizeCompetitorName(comp, { market: propertyProfile.market }) || comp).toLowerCase();
      counts[name] = (counts[name] || 0) + 1;
    }
  }
  for (const [name, count] of Object.entries(counts)) {
    if (count >= MIN_RECURRENCE) recurring.add(name);
  }
  for (const d of declared) recurring.add(d);
  return recurring;
}

export function computeExpectedShareResearch(observations, scenarios, propertyProfile) {
  const eligibility = buildScenarioEligibilityMap(scenarios, propertyProfile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));
  const subject = subjectAppearances(observations, eligibleIds);

  const modelAUniverse = buildGovernedUniverse(observations, propertyProfile);
  const modelAExpected = modelAUniverse.size > 0 ? roundAdpPercent(100 / modelAUniverse.size) : null;
  const modelATotal = totalEligibleAppearances(observations, eligibleIds, modelAUniverse);
  const modelAActual = modelATotal > 0 ? roundAdpPercent((subject / modelATotal) * 100) : null;
  const modelAIndex = modelAExpected > 0 && modelAActual != null
    ? Math.round((modelAActual / modelAExpected) * 100)
    : null;

  const byIntent = {};
  for (const scenario of eligibleScenarios) {
    const intent = scenario.intent;
    if (!byIntent[intent]) byIntent[intent] = { scenarios: [], ids: new Set() };
    byIntent[intent].scenarios.push(scenario);
    byIntent[intent].ids.add(scenario.scenarioId);
  }

  const modelB = {};
  for (const [intent, bucket] of Object.entries(byIntent)) {
    const intentObs = filterComparableObservations(observations).filter((o) => bucket.ids.has(o.scenarioId));
    const comps = new Set();
    for (const obs of intentObs) {
      for (const comp of obs.competitorsMentioned || []) {
        comps.add((canonicalizeCompetitorName(comp) || comp).toLowerCase());
      }
    }
    const expected = comps.size > 0 ? roundAdpPercent(100 / (comps.size + 1)) : null;
    const subj = intentObs.filter((o) => o.mentioned).length;
    const total = intentObs.reduce((s, o) => s + 1 + (o.competitorsMentioned?.length || 0), 0);
    const actual = total > 0 ? roundAdpPercent((subj / total) * 100) : null;
    modelB[intent] = { expected, actual, researchIndex: expected && actual ? Math.round((actual / expected) * 100) : null };
  }

  const modelCUniverse = modelAUniverse;
  const modelC = {
    universeSize: modelCUniverse.size,
    expectedShare: modelAExpected,
    actualShare: modelAActual,
    researchIndex: modelAIndex,
  };

  const modelD = {
    note: "Weighted eligibility deferred — requires governed attribute weights per territory",
    researchIndex: null,
  };

  const sensitivity = {
    modelAIndex,
    modelCRange: modelAIndex != null ? `±${Math.round(modelAUniverse.size * 2)} pts if universe +/-1 competitor` : "N/A",
    universeSize: modelAUniverse.size,
  };

  return {
    status: "RESEARCH_ONLY",
    customerAciBlocked: true,
    modelA_equalShareAmongGovernedEligible: { expectedShare: modelAExpected, actualShare: modelAActual, researchIndex: modelAIndex },
    modelB_scenarioSpecificEligible: modelB,
    modelC_declaredObservedHybrid: modelC,
    modelD_weightedEligibility: modelD,
    recommendedModel: "MODEL_C_HYBRID_WITH_RECURRENCE_FLOOR",
    sensitivity,
    researchFormula: "RESEARCH_ACI = (ACTUAL_CONSIDERATION_SHARE / EXPECTED_CONSIDERATION_SHARE) × 100",
    parityInterpretation: "PARTIAL — defensible only with certified governed universe and stable denominators",
    remainingCertificationGates: [
      "minimum CORE competitors >= 3",
      "minimum scenario coverage >= 80% eligible",
      "minimum provider coverage all 4",
      "stable governed universe (recurrence floor)",
      "no single-competitor denominator artifact",
      "longitudinal repeatability (2+ periods)",
      "property eligibility valid",
    ],
  };
}
