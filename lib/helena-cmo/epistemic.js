/**
 * Epistemic classification for Helena CMO THINK mode.
 * FACT | OBSERVATION | HYPOTHESIS | RECOMMENDATION | FOUNDER_DECISION
 * DECISION retained as alias of FOUNDER_DECISION for Phase 3A compatibility.
 */
export const EPISTEMIC_TYPES = Object.freeze([
  'FACT',
  'OBSERVATION',
  'HYPOTHESIS',
  'RECOMMENDATION',
  'FOUNDER_DECISION',
  'DECISION',
]);

export const CONFIDENCE_LEVELS = Object.freeze(['HIGH', 'MEDIUM', 'LOW']);

export function classifyEpistemic(type) {
  const t = String(type || '').toUpperCase().trim();
  if (!EPISTEMIC_TYPES.includes(t)) {
    throw new Error(`Invalid epistemic type: ${type}`);
  }
  // Preserve DECISION for Phase 3A compatibility; FOUNDER_DECISION is the Operating Law v1 name.
  return t;
}

export function isFounderDecisionType(type) {
  const t = classifyEpistemic(type);
  return t === 'FOUNDER_DECISION' || t === 'DECISION';
}

export function classifyConfidence(level) {
  const c = String(level || '').toUpperCase().trim();
  if (!CONFIDENCE_LEVELS.includes(c)) {
    throw new Error(`Invalid confidence: ${level}`);
  }
  return c;
}

export function assertNotHypothesisAsFact(item) {
  if (item?.presentedAs === 'FACT' && item?.epistemic === 'HYPOTHESIS') {
    throw new Error('Helena must not represent a hypothesis as fact');
  }
  return true;
}

export function assertNotRecommendationAsFounderDecision(item) {
  const epi = classifyEpistemic(item?.epistemic || 'RECOMMENDATION');
  if (
    (item?.presentedAs === 'FOUNDER_DECISION' || item?.presentedAs === 'DECISION') &&
    epi === 'RECOMMENDATION'
  ) {
    throw new Error('Helena must not present a recommendation as a founder decision');
  }
  return true;
}
