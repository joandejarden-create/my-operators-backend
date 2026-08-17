/**
 * AI Visibility access depth — deep / comparative / none.
 * Authorization layer only; does not affect monitoring execution.
 */

export const ACCESS_DEPTH = Object.freeze({
  DEEP: "deep",
  COMPARATIVE: "comparative",
  NONE: "none",
});

export const ACCESS_DEPTH_VERSION = "ai_visibility_access_depth_v1";

/** Fields allowed on comparative (benchmark-safe) entity rows. */
export const COMPARATIVE_SAFE_METRIC_FIELDS = Object.freeze([
  "entityId",
  "entityName",
  "aiPresenceRate",
  "aiPresenceChange",
  "competitivePosition",
  "recommendationShare",
  "recommendationRate",
  "top3RecommendationRate",
  "firstRecommendationRate",
  "isSubject",
  "provider",
  "geographyScope",
  "commercialRegion",
  "country",
  "metricVersion",
  "batchId",
  "batchDate",
]);

/** Fields blocked at comparative depth (diagnostic / private workflow). */
export const COMPARATIVE_BLOCKED_FIELDS = Object.freeze([
  "opportunityQueue",
  "questionsMissingDetail",
  "questionsWonDetail",
  "fullQuestionGapHistory",
  "privateWorkflow",
  "workspaceNotes",
  "clientActions",
  "diagnosticReason",
  "interpretationStatus",
]);

/**
 * @param {string} depth
 * @returns {boolean}
 */
export function isDeepAccess(depth) {
  return depth === ACCESS_DEPTH.DEEP;
}

/**
 * @param {string} depth
 * @returns {boolean}
 */
export function isComparativeAccess(depth) {
  return depth === ACCESS_DEPTH.COMPARATIVE;
}

/**
 * @param {string} depth
 * @returns {boolean}
 */
export function hasAnyAccess(depth) {
  return depth === ACCESS_DEPTH.DEEP || depth === ACCESS_DEPTH.COMPARATIVE;
}
