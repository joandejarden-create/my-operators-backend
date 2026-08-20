/**
 * Competitor-present gap rollups — scenario and observation grain.
 */

import { filterComparableObservations } from "./grain-governance.js";
import {
  buildScenarioEligibilityMap,
  CONSIDERATION_ELIGIBLE_STATES,
  scenariosForEligibility,
} from "./scenario-eligibility.js";
import { matchesDeclaredComp } from "../intelligence/competitor-name-resolution.js";

function hasGovernedCompetitor(competitors, propertyProfile) {
  const declared = propertyProfile?.declaredCompSet || [];
  for (const comp of competitors || []) {
    if (declared.some((d) => matchesDeclaredComp(comp, d))) return true;
  }
  return false;
}

function hasAnyCompetitor(competitors) {
  return Array.isArray(competitors) && competitors.length > 0;
}

export function computeCompetitorPresentGaps(observations, scenarios, propertyProfile) {
  const eligibility = buildScenarioEligibilityMap(scenarios, propertyProfile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));
  const comparable = filterComparableObservations(observations).filter((o) => eligibleIds.has(o.scenarioId));

  const governedScenarioIds = new Set();
  let observationGapsAnyCompetitor = 0;
  let observationGapsGoverned = 0;

  for (const obs of comparable) {
    if (obs.mentioned) continue;
    if (hasAnyCompetitor(obs.competitorsMentioned)) {
      observationGapsAnyCompetitor += 1;
    }
    if (!hasGovernedCompetitor(obs.competitorsMentioned, propertyProfile)) continue;
    observationGapsGoverned += 1;
    governedScenarioIds.add(obs.scenarioId);
  }

  return {
    competitorPresentScenarios: governedScenarioIds.size,
    competitorPresentObservations: observationGapsAnyCompetitor,
    competitorPresentObservationsGoverned: observationGapsGoverned,
    grain: {
      scenarios: "SCENARIO_GRAIN",
      observations: "OBSERVATION_GRAIN",
      observationNote:
        "Hero/detail observation count uses eligible comparable responses with any competitor mention; scenario rollup uses governed declared comparables.",
    },
  };
}
