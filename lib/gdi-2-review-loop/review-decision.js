import fs from 'node:fs';
import path from 'node:path';
import { REVIEWS_DIR } from './constants.js';
import { atomicWriteFile } from './immutability.js';

export const REVIEW_OUTCOMES = new Set(['PASS', 'CONDITIONAL_PASS', 'FAIL']);

/**
 * Structured ChatGPT/founder review decision stored OUTSIDE immutable run evidence.
 */
export function buildReviewDecision({
  runId,
  outcome,
  reviewer = 'ChatGPT',
  findings = [],
  correctiveActions = [],
  unlockNextMajorPhase = false,
  notes = '',
  reviewedAt = new Date().toISOString(),
} = {}) {
  if (!REVIEW_OUTCOMES.has(outcome)) {
    throw new Error(`Invalid review outcome: ${outcome}`);
  }
  if (outcome === 'CONDITIONAL_PASS' && unlockNextMajorPhase === true) {
    throw new Error('CONDITIONAL_PASS must not unlock the next major phase');
  }
  if (outcome === 'FAIL' && unlockNextMajorPhase === true) {
    throw new Error('FAIL must not unlock the next major phase');
  }
  return {
    contractId: 'GDI_2_REVIEW_DECISION_V1',
    runId,
    outcome,
    reviewer,
    reviewedAt,
    findings,
    correctiveActions,
    phaseGate: {
      unlockNextMajorPhase: Boolean(unlockNextMajorPhase),
      note:
        unlockNextMajorPhase === true
          ? 'Explicit unlock recorded; NEXT_CURSOR_PROMPT.md must still be APPROVED before execution'
          : 'Next major phase remains blocked',
    },
    notes,
  };
}

export function writeReviewDecision(decision, { fileName } = {}) {
  fs.mkdirSync(REVIEWS_DIR, { recursive: true });
  const name = fileName || `${decision.runId}-${decision.outcome.toLowerCase().replace(/_/g, '-')}.json`;
  const abs = path.join(REVIEWS_DIR, name);
  atomicWriteFile(abs, `${JSON.stringify(decision, null, 2)}\n`);
  return abs;
}

export function assertPhaseAdvancementAllowed({ reviewDecision, nextPromptStatus }) {
  const errors = [];
  if (!reviewDecision) {
    errors.push('missing review decision');
    return { ok: false, errors };
  }
  if (reviewDecision.outcome === 'CONDITIONAL_PASS' || reviewDecision.outcome === 'FAIL') {
    if (reviewDecision.phaseGate?.unlockNextMajorPhase) {
      errors.push(`${reviewDecision.outcome} cannot unlock next major phase`);
    } else {
      errors.push(`${reviewDecision.outcome} blocks next major phase advancement`);
    }
  }
  if (reviewDecision.outcome === 'PASS' && reviewDecision.phaseGate?.unlockNextMajorPhase !== true) {
    errors.push('PASS without explicit unlockNextMajorPhase still blocks advancement');
  }
  if (String(nextPromptStatus || '').toUpperCase() !== 'APPROVED') {
    errors.push('NEXT_CURSOR_PROMPT.md is not APPROVED');
  }
  return { ok: errors.length === 0, errors };
}
