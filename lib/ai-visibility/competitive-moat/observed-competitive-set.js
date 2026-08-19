/**
 * Observed Competitive Set — derived from validated Presence / gap evidence only.
 * No win/loss, no recommendation outcome.
 */

export const OBSERVED_COMPETITIVE_SET_VERSION = "observed_competitive_set_v1";
export const DERIVATION = "PRESENCE_AND_VALIDATED_GAP_EVIDENCE";
export const WIN_LOSS_DEPENDENCY = false;

export const SET_RELATIONSHIPS = Object.freeze([
  "DECLARED_ONLY",
  "OBSERVED_ONLY",
  "DECLARED_AND_OBSERVED",
  "EMERGING_OBSERVED",
]);

export const DEFAULT_CUSTOMER_COMPETITOR_LIMIT = 5;

/**
 * Build observed competitor record from presence/gap evidence.
 */
export function buildObservedCompetitorRecord(opts = {}) {
  return Object.freeze({
    entityId: opts.entityId || null,
    canonicalName: opts.canonicalName || null,
    entityType: opts.entityType || "BRAND",
    scenarioIds: [...(opts.scenarioIds || [])],
    intentIds: [...(opts.intentIds || [])],
    providers: [...(opts.providers || [])],
    geographies: [...(opts.geographies || [])],
    coOccurrenceCount: opts.coOccurrenceCount || 0,
    appearanceCount: opts.appearanceCount || 0,
    subjectMissingCompetitorPresentCount: opts.subjectMissingCompetitorPresentCount || 0,
    recurrenceState: opts.recurrenceState || "SINGLE_PERIOD",
    firstObservedDate: opts.firstObservedDate || null,
    latestObservedDate: opts.latestObservedDate || null,
    declaredCompetitor: opts.declaredCompetitor === true,
    emergingCandidate: opts.emergingCandidate === true,
    setRelationship: opts.setRelationship || "OBSERVED_ONLY",
    winCount: undefined,
    lossCount: undefined,
    recommendationFrequency: undefined,
    displacementFrequency: undefined,
  });
}

/**
 * Derive top observed competitors from peer presence matrix.
 * @param {{ subjectId: string, peerRows: Array<{ entityId, entityName, presenceRate, coOccurrence?, subjectMissing? }>, declaredIds?: string[] }}
 */
export function deriveObservedCompetitiveSet(opts = {}) {
  const subjectId = opts.subjectId;
  const declared = new Set(opts.declaredIds || []);
  const peers = (opts.peerRows || []).filter((p) => p.entityId !== subjectId);

  const observed = peers
    .map((p) => {
      const declaredCompetitor = declared.has(p.entityId);
      const appearanceCount = p.appearanceCount ?? (p.presenceRate > 0 ? 1 : 0);
      let setRelationship = "OBSERVED_ONLY";
      if (declaredCompetitor) setRelationship = "DECLARED_AND_OBSERVED";
      return buildObservedCompetitorRecord({
        entityId: p.entityId,
        canonicalName: p.entityName,
        entityType: opts.entityType || "BRAND",
        appearanceCount,
        coOccurrenceCount: p.coOccurrenceCount || 0,
        subjectMissingCompetitorPresentCount: p.subjectMissingCompetitorPresentCount || p.subjectMissing || 0,
        scenarioIds: p.scenarioIds || [],
        providers: p.providers || [],
        geographies: p.geographies || [],
        declaredCompetitor,
        setRelationship,
      });
    })
    .filter((o) => o.appearanceCount > 0 || o.subjectMissingCompetitorPresentCount > 0)
    .sort((a, b) => {
      const scoreA = a.subjectMissingCompetitorPresentCount * 2 + a.appearanceCount;
      const scoreB = b.subjectMissingCompetitorPresentCount * 2 + b.appearanceCount;
      return scoreB - scoreA;
    });

  return {
    version: OBSERVED_COMPETITIVE_SET_VERSION,
    derivation: DERIVATION,
    winLossDependency: WIN_LOSS_DEPENDENCY,
    subjectId,
    totalObserved: observed.length,
    topObserved: observed.slice(0, opts.limit ?? DEFAULT_CUSTOMER_COMPETITOR_LIMIT),
    allObservedInternal: observed,
  };
}

/**
 * Customer-safe competitor list — limited count, no raw matrix.
 */
export function toCustomerObservedCompetitors(observedSet, limit = DEFAULT_CUSTOMER_COMPETITOR_LIMIT) {
  return (observedSet.topObserved || observedSet.allObservedInternal || [])
    .slice(0, limit)
    .map((c) => ({
      entityId: c.entityId,
      canonicalName: c.canonicalName,
      appearanceCount: c.appearanceCount,
      subjectMissingCompetitorPresentCount: c.subjectMissingCompetitorPresentCount,
      declaredCompetitor: c.declaredCompetitor,
      emergingCandidate: c.emergingCandidate || false,
    }));
}
