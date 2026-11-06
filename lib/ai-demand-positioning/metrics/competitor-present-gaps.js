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
import { subjectEntityIdsForProperty } from "../subject-presence/subject-entity-ids-v1.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";
import { canonicalizeForProperty } from "./adp-property-entity-registries.js";

function hasGovernedCompetitor(competitors, propertyProfile) {
  const declared = propertyProfile?.declaredCompSet || [];
  for (const comp of competitors || []) {
    if (declared.some((d) => matchesDeclaredComp(comp, d))) return true;
  }
  return false;
}

function nonSubjectCompetitors(competitors, propertyProfile) {
  const subjectIds = subjectEntityIdsForProperty(propertyProfile);
  const propertyId = propertyProfile?.propertyId;
  return (competitors || []).filter((comp) => {
    const id = propertyId ? canonicalizeForProperty(propertyId, comp) : null;
    if (id && subjectIds.has(id)) return false;
    return true;
  });
}

function hasAnyCompetitor(competitors, propertyProfile) {
  return nonSubjectCompetitors(competitors, propertyProfile).length > 0;
}

export function hasCustomerFacingCompetitor(competitors, propertyProfile) {
  for (const comp of nonSubjectCompetitors(competitors, propertyProfile)) {
    const resolved = resolveCustomerFacingEntity(comp, propertyProfile);
    if (resolved.ok && !resolved.rejected) return true;
  }
  return false;
}

export function observationSubjectPresent(obs, propertyProfile) {
  if (getGovernedSubjectMentioned(obs)) return true;
  if (obs?.mentioned) return true;
  const subjectIds = subjectEntityIdsForProperty(propertyProfile);
  const propertyId = propertyProfile?.propertyId;
  for (const comp of obs?.competitorsMentioned || []) {
    const id = propertyId ? canonicalizeForProperty(propertyId, comp) : null;
    if (id && subjectIds.has(id)) return true;
  }
  return false;
}

function observationSubjectAbsent(obs, propertyProfile) {
  return !observationSubjectPresent(obs, propertyProfile);
}

/** Observation-level customer-facing competitor present (non-subject). */
export function observationHasCustomerFacingCompetitor(obs, propertyProfile) {
  return hasCustomerFacingCompetitor(obs?.competitorsMentioned || [], propertyProfile);
}

export function computeCompetitorPresentGaps(observations, scenarios, propertyProfile) {
  const eligibility = buildScenarioEligibilityMap(scenarios, propertyProfile);
  const eligibleScenarios = scenariosForEligibility(scenarios, eligibility, CONSIDERATION_ELIGIBLE_STATES);
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));
  const comparable = filterComparableObservations(observations).filter((o) => eligibleIds.has(o.scenarioId));

  const governedScenarioIds = new Set();
  const fullyAbsentScenarioIds = new Set();
  let observationGapsAnyCompetitor = 0;
  let observationGapsGoverned = 0;
  const declaredCompSet = propertyProfile?.declaredCompSet || [];
  const useObservedUniverse = declaredCompSet.length === 0;

  // Scenario fully absent = no provider response mentions the subject
  const scenarioSubjectPresent = new Map();
  for (const obs of comparable) {
    const prev = scenarioSubjectPresent.get(obs.scenarioId) === true;
    const present = observationSubjectPresent(obs, propertyProfile);
    scenarioSubjectPresent.set(obs.scenarioId, prev || present);
  }

  for (const obs of comparable) {
    if (!observationSubjectAbsent(obs, propertyProfile)) continue;
    // Also skip when subject entity leaked into competitorsMentioned only
    const comps = nonSubjectCompetitors(obs.competitorsMentioned, propertyProfile);
    if (hasAnyCompetitor(obs.competitorsMentioned, propertyProfile)) {
      observationGapsAnyCompetitor += 1;
    }
    if (useObservedUniverse) {
      if (!hasCustomerFacingCompetitor(obs.competitorsMentioned, propertyProfile)) continue;
      observationGapsGoverned += 1;
      governedScenarioIds.add(obs.scenarioId);
      if (scenarioSubjectPresent.get(obs.scenarioId) !== true) {
        fullyAbsentScenarioIds.add(obs.scenarioId);
      }
      continue;
    }
    if (!hasGovernedCompetitor(comps, propertyProfile)) continue;
    observationGapsGoverned += 1;
    governedScenarioIds.add(obs.scenarioId);
    if (scenarioSubjectPresent.get(obs.scenarioId) !== true) {
      fullyAbsentScenarioIds.add(obs.scenarioId);
    }
  }

  const responseLevel = governedScenarioIds.size;
  const fullyAbsent = fullyAbsentScenarioIds.size;
  // Invariant: SCENARIO_LEVEL competitor-present ⊆ RESPONSE_LEVEL set
  if (fullyAbsent > responseLevel) {
    throw new Error(
      `competitor-present invariant violated: fullyAbsent(${fullyAbsent}) > responseLevel(${responseLevel})`
    );
  }

  return {
    competitorPresentScenarios: responseLevel,
    competitorPresentScenariosFullyAbsent: fullyAbsent,
    competitorPresentObservations: observationGapsAnyCompetitor,
    competitorPresentObservationsGoverned: observationGapsGoverned,
    grain: {
      scenarios: "SCENARIO_GRAIN",
      observations: "OBSERVATION_GRAIN",
      countingRule:
        "RESPONSE_LEVEL_ABSENCE: a scenario counts when at least one provider answer omits the subject while naming a comparable hotel — even if another provider mentioned the subject for the same scenario.",
      fullyAbsentRule:
        "SCENARIO_LEVEL_ABSENCE: subject omitted by every comparable provider response for that traveler question. This count cannot exceed scenario-absence.",
      observationNote:
        "competitorPresentScenarios uses RESPONSE_LEVEL_ABSENCE (may exceed full scenario-absence). competitorPresentScenariosFullyAbsent uses SCENARIO_LEVEL_ABSENCE. Subject entity aliases never count as competitors. Declared comp set when present; otherwise customer-facing competitors.",
      mayExceedSubjectFullyAbsent:
        "RESPONSE_LEVEL competitor-present may exceed total subject-fully-absent scenarios because some counted questions still have subject presence on another provider.",
    },
  };
}
