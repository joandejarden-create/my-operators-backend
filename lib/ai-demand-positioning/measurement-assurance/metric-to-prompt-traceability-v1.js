/**
 * METRIC_TO_PROMPT_TRACEABILITY — resolve core metrics → observations → prompts → responses.
 */

export const METRIC_TO_PROMPT_TRACEABILITY = "METRIC_TO_PROMPT_TRACEABILITY";

/**
 * @param {object} args
 * @param {string} args.metricName
 * @param {object[]} args.contributingObservations — already filtered to core-eligible set
 * @param {Map|object} args.scenarioById
 */
export function buildMetricToPromptTrace({ metricName, contributingObservations = [], scenarioById = {} }) {
  const getScenario = (id) =>
    scenarioById instanceof Map ? scenarioById.get(id) : scenarioById[id];

  const observationTraces = contributingObservations.map((obs) => {
    const sc = getScenario(obs.scenarioId);
    return {
      observationId: obs.observationId,
      scenarioId: obs.scenarioId,
      provider: obs.provider,
      periodId: obs.periodId,
      exactRenderedPrompt: obs.exactRenderedPrompt || sc?.query || null,
      promptHash: obs.promptHash || null,
      exactResponseRef: obs.rawResponse
        ? { inlineLength: String(obs.rawResponse).length, responseHash: obs.responseHash || null }
        : null,
      subjectPresent: obs.governedInterpretation?.subjectMentioned ?? obs.mentioned ?? null,
      rank: obs.rank ?? obs.position ?? null,
      measurementEligibility: obs.measurementEligibility,
      scenarioClass: obs.scenarioClass || null,
      replacesObservationId: obs.replacesObservationId || null,
    };
  });

  const scenarioSlots = [...new Set(observationTraces.map((t) => t.scenarioId))];

  return {
    gate: METRIC_TO_PROMPT_TRACEABILITY,
    metricName,
    scenarioSlotCount: scenarioSlots.length,
    observationCount: observationTraces.length,
    scenarioSlots,
    observationTraces,
    lineage:
      "Metric → scenario slot → observation IDs → exactRenderedPrompt → exact response → governed interpretation → contribution",
  };
}

export function buildRankingPromptTrace({ rankRows = [], observationsById = {} }) {
  return {
    gate: METRIC_TO_PROMPT_TRACEABILITY,
    kind: "competitive_ranking",
    rows: rankRows.map((row) => ({
      entityId: row.entityId || row.canonicalEntityId,
      rank: row.rank,
      presenceObservationIds: row.observationIds || row.presenceObservationIds || [],
      traces: (row.observationIds || row.presenceObservationIds || []).map((id) => {
        const obs = observationsById[id];
        return obs
          ? {
              observationId: id,
              scenarioId: obs.scenarioId,
              exactRenderedPrompt: obs.exactRenderedPrompt || null,
              promptHash: obs.promptHash || null,
            }
          : { observationId: id, missing: true };
      }),
    })),
  };
}

export function buildDisplacementPromptTrace({ displacementEvents = [] }) {
  return {
    gate: METRIC_TO_PROMPT_TRACEABILITY,
    kind: "displacement",
    eventCount: displacementEvents.length,
    events: displacementEvents.map((ev) => ({
      scenarioId: ev.scenarioId,
      observationId: ev.observationId,
      competitorEntityId: ev.competitorEntityId || ev.entityId,
      exactRenderedPrompt: ev.exactRenderedPrompt || null,
      promptHash: ev.promptHash || null,
    })),
  };
}
