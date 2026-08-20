/**
 * ADP metric grain governance.
 * SCENARIO_GRAIN: property × scenario (may aggregate providers)
 * OBSERVATION_GRAIN: property × scenario × provider × period
 */

export const METRIC_GRAINS = Object.freeze({
  SCENARIO: "SCENARIO_GRAIN",
  OBSERVATION: "OBSERVATION_GRAIN",
});

export function isComparableObservation(obs) {
  if (!obs) return false;
  if (obs.dryRun) return false;
  if (obs.error) return false;
  if (!obs.parsed && obs.mentioned === undefined && !obs.rawResponse) return false;
  return true;
}

export function filterComparableObservations(observations) {
  return (observations || []).filter(isComparableObservation);
}

export function groupObservationsByScenario(observations) {
  const map = new Map();
  for (const obs of filterComparableObservations(observations)) {
    if (!map.has(obs.scenarioId)) map.set(obs.scenarioId, []);
    map.get(obs.scenarioId).push(obs);
  }
  return map;
}
