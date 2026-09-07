export {
  EPISTEMIC_TYPES,
  CONFIDENCE_LEVELS,
  classifyEpistemic,
  classifyConfidence,
  assertNotHypothesisAsFact,
  assertNotRecommendationAsFounderDecision,
} from '../epistemic.js';

import {
  classifyEpistemic,
  classifyConfidence,
  assertNotHypothesisAsFact,
  assertNotRecommendationAsFounderDecision,
} from '../epistemic.js';

export function buildTaggedConclusion(input = {}) {
  const epistemic = classifyEpistemic(input.epistemic || 'HYPOTHESIS');
  const confidence = classifyConfidence(input.confidence || 'MEDIUM');
  const item = {
    statement: String(input.statement || ''),
    epistemic,
    confidence,
    authorityLevel: input.authorityLevel || 'ANALYTICAL_RECOMMENDATION',
    source: input.source || null,
    presentedAs: input.presentedAs || epistemic,
  };
  assertNotHypothesisAsFact(item);
  assertNotRecommendationAsFounderDecision(item);
  return item;
}
