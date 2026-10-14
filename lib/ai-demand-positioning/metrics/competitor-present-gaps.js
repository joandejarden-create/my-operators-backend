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
import { resolveCustomerFacingEntity } from "../customer/customer-entity-resolution-v1.js";

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

function hasCustomerFacingCompetitor(competitors, propertyProfile) {
  for (const comp of competitors || []) {
    const resolved = resolveCustomerFacingEntity(comp, propertyProfile);
    if (resolved.ok && !resolved.rejected) return true;
  }
  return false;
}

export function computeCompetitorPresentGaps(observations, scenarios, propertyProfile) {
  const eligibility = buildScenarioEligibilityMap(scenarios, propertyProfile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));
  const comparable = filterComparableObservations(observations).filter((o) => eligibleIds.has(o.scenarioId));

  const governedScenarioIds = new Set();
  let observationGapsAnyCompetitor = 0;
  let observationGapsGoverned = 0;
  const declaredCompSet = propertyProfile?.declaredCompSet || [];
  const useObservedUniverse = declaredCompSet.length === 0;

  for (const obs of comparable) {
    if (obs.mentioned) continue;
    if (hasAnyCompetitor(obs.competitorsMentioned)) {
      observationGapsAnyCompetitor += 1;
    }
    if (useObservedUniverse) {
      if (!hasCustomerFacingCompetitor(obs.competitorsMentioned, propertyProfile)) continue;
      observationGapsGoverned += 1;
      governedScenarioIds.add(obs.scenarioId);
      continue;
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
        "Scenario count is unique traveler questions with at least one answer where the subject is absent and a comparable hotel is named. A declared comp set is used when present. Hotels with no declared set use customer-facing competitors, the same universe as displacement. Denominator is eligible scenarios, not provider responses.",
    },
  };
}
