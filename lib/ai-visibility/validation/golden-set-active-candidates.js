/**
 * Canonical active Golden Set review candidate resolver.
 * ALL queue / export / import / promote / progress paths must use this.
 * Superseded/null-subject records remain stored for audit only.
 */

export const SUPERSEDED_INVALID_SUBJECT = "SUPERSEDED_INVALID_SUBJECT";
export const CASE_TYPE_NO_ENTITY_DETECTED = "NO_ENTITY_DETECTED";

/**
 * FIELD_USED_FOR_ACTIVE_STATUS: candidate.reviewStatus
 * SUPERSEDED_STATUS_VALUE: SUPERSEDED_INVALID_SUBJECT
 * WHERE_STATUS_IS_STORED: fixtures/.../ai-intelligence-golden-set-v2-candidates.json → cases[].reviewStatus
 */

export function isSupersededCandidate(c) {
  if (!c) return true;
  if (c.notReviewable === true) return true;
  if (c.reviewStatus === SUPERSEDED_INVALID_SUBJECT) return true;
  return false;
}

export function hasSubjectEntity(c) {
  if (!c) return false;
  const name = String(c.candidateEntity || c.canonicalEntityName || "").trim();
  const id = String(c.canonicalEntityId || "").trim();
  const canonicalName = String(c.canonicalEntityName || c.candidateEntity || "").trim();
  return Boolean(name && id && canonicalName);
}

export function hasReviewableStoredResponse(c) {
  if (!c) return false;
  const text = String(c.rawResponseExcerpt || c.storedResponse || "").trim();
  return text.length >= 20;
}

/**
 * Active reviewable Golden Set candidate contract.
 * Ordinary cases must be RESPONSE × canonical entity.
 */
export function isActiveReviewCandidate(c) {
  if (!c) return false;
  if (isSupersededCandidate(c)) return false;
  if (c.caseType === CASE_TYPE_NO_ENTITY_DETECTED && c.explicitNoEntityCase === true) {
    // Separately governed no-entity test cases only
    return hasReviewableStoredResponse(c);
  }
  if (!hasSubjectEntity(c)) return false;
  if (!hasReviewableStoredResponse(c)) return false;
  return true;
}

/** @deprecated use isActiveReviewCandidate */
export function isReviewableCandidate(c) {
  return isActiveReviewCandidate(c);
}

/**
 * Single source of truth for human-review surfaces.
 * @param {{ cases?: object[] }|null} doc candidate document
 * @returns {object[]}
 */
export function getActiveGoldenSetReviewCandidates(doc) {
  return (doc?.cases || []).filter(isActiveReviewCandidate);
}

export function getSupersededGoldenSetCandidates(doc) {
  return (doc?.cases || []).filter(isSupersededCandidate);
}

export function getNullSubjectCandidates(doc, { includeSuperseded = true } = {}) {
  return (doc?.cases || []).filter((c) => {
    if (hasSubjectEntity(c)) return false;
    if (!includeSuperseded && isSupersededCandidate(c)) return false;
    return true;
  });
}

/**
 * Population summary — never conflate stored vs active queue.
 */
export function summarizeCandidatePopulation(doc) {
  const stored = doc?.cases || [];
  const active = getActiveGoldenSetReviewCandidates(doc);
  const superseded = getSupersededGoldenSetCandidates(doc);
  const nullTotal = getNullSubjectCandidates(doc, { includeSuperseded: true });
  const nullActive = getNullSubjectCandidates(doc, { includeSuperseded: false });
  return {
    storedCandidateCount: stored.length,
    activeReviewCandidateCount: active.length,
    supersededCandidateCount: superseded.length,
    nullSubjectTotal: nullTotal.length,
    nullSubjectActive: nullActive.length,
    FIELD_USED_FOR_ACTIVE_STATUS: "reviewStatus",
    SUPERSEDED_STATUS_VALUE: SUPERSEDED_INVALID_SUBJECT,
    WHERE_STATUS_IS_STORED:
      "fixtures/ai-visibility/ai-intelligence-golden-set-v2-candidates.json → cases[].reviewStatus",
    activeCaseIds: active.map((c) => c.caseId),
  };
}

export function assertCaseIsActiveForReview(candidate, caseId) {
  if (!candidate) {
    const err = new Error("CASE_NOT_FOUND");
    err.code = "CASE_NOT_FOUND";
    throw err;
  }
  if (isSupersededCandidate(candidate) || !isActiveReviewCandidate(candidate)) {
    const err = new Error(`CANDIDATE_NOT_ACTIVE:${caseId || candidate.caseId}`);
    err.code = "CANDIDATE_NOT_ACTIVE";
    err.caseId = caseId || candidate.caseId;
    err.reviewStatus = candidate.reviewStatus || null;
    throw err;
  }
  return true;
}
