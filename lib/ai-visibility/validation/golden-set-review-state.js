/**
 * Golden Set Review — current-state buckets for queue UI.
 * Completed/invalidated remain auditable; default work queue is ACTIVE only.
 * One primary current-state bucket per case (no double-count).
 */

import {
  REVIEW_STATUS,
  loadCandidateDocument,
  listAllReviewRecords,
  loadGoldenSetV2Document,
} from "./golden-set-human-review.js";
import {
  getActiveGoldenSetReviewCandidates,
  SUPERSEDED_INVALID_SUBJECT,
  summarizeCandidatePopulation,
} from "./golden-set-active-candidates.js";

export const REVIEW_STATE_BUCKET = Object.freeze({
  ACTIVE_REVIEW: "ACTIVE_REVIEW",
  COMPLETED: "COMPLETED",
  INVALIDATED: "INVALIDATED",
  SUPERSEDED: "SUPERSEDED",
});

/** Query param aliases → bucket */
export const REVIEW_STATE_QUERY = Object.freeze({
  active: REVIEW_STATE_BUCKET.ACTIVE_REVIEW,
  completed: REVIEW_STATE_BUCKET.COMPLETED,
  invalidated: REVIEW_STATE_BUCKET.INVALIDATED,
  superseded: REVIEW_STATE_BUCKET.SUPERSEDED,
  all: "ALL",
});

export const INVALIDATED_CANDIDATE_SUBJECT = "INVALIDATED_CANDIDATE_SUBJECT";

/**
 * Build set of candidate caseIds invalidated via Golden Set v2 amendment.
 * v2 caseIds are `v2_${sourceCaseId}`; map back to review candidate ids.
 */
export function loadInvalidatedCandidateIdSet(options = {}) {
  const v2 = options.v2Doc || loadGoldenSetV2Document();
  const ids = new Set();
  for (const c of v2?.cases || []) {
    if (
      c.groundTruthInvalidated === true ||
      c.excludeFromClassificationDenominator === true ||
      c.reviewStatus === INVALIDATED_CANDIDATE_SUBJECT
    ) {
      if (c.sourceCaseId) ids.add(c.sourceCaseId);
      const stripped = String(c.caseId || "").replace(/^v2_/, "");
      if (stripped) ids.add(stripped);
      if (c.caseId) ids.add(c.caseId);
    }
  }
  return ids;
}

/**
 * Canonical current-state bucket for a review case / row.
 * Precedence: SUPERSEDED → INVALIDATED → COMPLETED → ACTIVE_REVIEW
 */
export function getGoldenSetReviewState(row, options = {}) {
  const invalidatedIds = options.invalidatedIds || loadInvalidatedCandidateIdSet(options);
  const caseId = row?.caseId || row?.id || null;
  const reviewStatus = row?.reviewStatus || null;
  const candidateStatus = row?.candidateReviewStatus || row?.storedCandidateStatus || null;

  if (
    reviewStatus === REVIEW_STATUS.SUPERSEDED_INVALID_SUBJECT ||
    reviewStatus === SUPERSEDED_INVALID_SUBJECT ||
    candidateStatus === SUPERSEDED_INVALID_SUBJECT ||
    row?.superseded === true
  ) {
    return REVIEW_STATE_BUCKET.SUPERSEDED;
  }

  if (
    reviewStatus === INVALIDATED_CANDIDATE_SUBJECT ||
    row?.groundTruthInvalidated === true ||
    row?.excludeFromClassificationDenominator === true ||
    (caseId && invalidatedIds.has(caseId))
  ) {
    return REVIEW_STATE_BUCKET.INVALIDATED;
  }

  if (
    reviewStatus === REVIEW_STATUS.CONFIRMED ||
    reviewStatus === REVIEW_STATUS.CORRECTED
  ) {
    return REVIEW_STATE_BUCKET.COMPLETED;
  }

  // UNREVIEWED, DEFERRED, SECOND_REVIEW_REQUIRED, REOPENED, REVIEW_REQUIRED, missing
  return REVIEW_STATE_BUCKET.ACTIVE_REVIEW;
}

export function isActiveReviewState(bucket) {
  return bucket === REVIEW_STATE_BUCKET.ACTIVE_REVIEW;
}

/**
 * Resolve query `state` / legacy reviewStatus into a bucket filter.
 * Default: ACTIVE_REVIEW
 */
export function resolveReviewStateFilter(filters = {}) {
  if (filters.state) {
    const key = String(filters.state).toLowerCase();
    if (REVIEW_STATE_QUERY[key]) return REVIEW_STATE_QUERY[key];
    const upper = String(filters.state).toUpperCase();
    if (Object.values(REVIEW_STATE_BUCKET).includes(upper)) return upper;
    if (upper === "ALL") return "ALL";
  }
  // Legacy exact status filter still supported (does not change default)
  if (filters.reviewStatus) return null;
  return REVIEW_STATE_BUCKET.ACTIVE_REVIEW;
}

/**
 * Count cases by current-state bucket (one primary bucket each).
 */
export function summarizeReviewStateBuckets(options = {}) {
  const doc = options.doc || loadCandidateDocument();
  const population = summarizeCandidatePopulation(doc);
  const reviews = options.reviews || listAllReviewRecords(options);
  const byId = Object.fromEntries(reviews.map((r) => [r.caseId, r]));
  const invalidatedIds = options.invalidatedIds || loadInvalidatedCandidateIdSet(options);

  const counts = {
    ACTIVE_REVIEW: 0,
    COMPLETED: 0,
    INVALIDATED: 0,
    SUPERSEDED: population.supersededCandidateCount || 0,
    TOTAL_HISTORICAL: 0,
  };

  // Active review candidates (non-superseded subjects)
  for (const c of getActiveGoldenSetReviewCandidates(doc)) {
    const review = byId[c.caseId];
    const status =
      review?.reviewStatus ||
      (c.reviewStatus === "PENDING_HUMAN_REVIEW"
        ? REVIEW_STATUS.UNREVIEWED
        : c.reviewStatus || REVIEW_STATUS.UNREVIEWED);
    const bucket = getGoldenSetReviewState(
      { caseId: c.caseId, reviewStatus: status },
      { invalidatedIds }
    );
    counts[bucket] = (counts[bucket] || 0) + 1;
    counts.TOTAL_HISTORICAL += 1;
  }

  // Superseded stored for audit (not in active list)
  counts.TOTAL_HISTORICAL += counts.SUPERSEDED;

  return {
    ...counts,
    OUTSTANDING_REVIEW: counts.ACTIVE_REVIEW,
    COMPLETED_REVIEW: counts.COMPLETED,
    note:
      "Each case has one primary current-state bucket. Invalidated (post-amendment) is not counted as Completed.",
  };
}
