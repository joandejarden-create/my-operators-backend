/**
 * Targeted recall workstreams — independent of multiclass production classifier.
 * Do not restart broad 10-class hardening here.
 */

export const RECALL_WORKSTREAM_VERSION = "ai_intelligence_signal_recall_workstreams_v1";

export const WORKSTREAM_A_RECOMMENDED_FLAG_RECALL = Object.freeze({
  id: "A_RECOMMENDED_FLAG_RECALL",
  status: "CLOSED_RESEARCH_BLOCKED",
  goal: "Improve RECOMMENDED recall while maintaining precision >= 98%",
  current: Object.freeze({
    precision: 0.8182,
    recall: 0.6,
    f1: 0.6923,
    note: "Post binary remediation unambiguous DEV; semantic feasibility also below gate",
  }),
  focusMisses: Object.freeze([
    "first_recommendation",
    "ranked_recommendation",
    "explicit_recommendation",
  ]),
  doNotCareAbout:
    "associated vs discussed distinction except to prevent false positives",
  precisionFloor: 0.98,
  holdout: "DO_NOT_USE_YET",
  recurringResearchScheduled: false,
  reopen: "See REOPEN_RESEARCH_IF in recommended-research-closure.js",
});

export const WORKSTREAM_B_FIRST_RECOMMENDATION_FLAG_RECALL = Object.freeze({
  id: "B_FIRST_RECOMMENDATION_FLAG_RECALL",
  status: "PAUSED_UNTIL_RECOMMENDED_REOPEN",
  goal: "Improve FIRST_RECOMMENDATION recall while maintaining precision >= 98%",
  current: Object.freeze({
    precision: 1,
    recall: 0.43333333333333335,
    f1: 0.6046511627906976,
  }),
  focus: "strict lead / rank-1 evidence only",
  doNotInferFrom: Object.freeze([
    "consideration list numbering",
    "first mention",
    "document order",
  ]),
  precisionFloor: 0.98,
  holdout: "DO_NOT_USE_YET",
  FIRST_RECOMMENDATION_WORK: 0,
});

export const DEFERRED_SPARSE_SIGNALS = Object.freeze({
  NEGATIVE_OR_QUALIFIED: "Defer until sample volume justifies",
  COMPARATOR: "Defer until sample volume justifies",
});

export function listRecallWorkstreams() {
  return {
    version: RECALL_WORKSTREAM_VERSION,
    workstreams: [
      WORKSTREAM_A_RECOMMENDED_FLAG_RECALL,
      WORKSTREAM_B_FIRST_RECOMMENDATION_FLAG_RECALL,
    ],
    deferred: DEFERRED_SPARSE_SIGNALS,
    noBroadMulticlassRestart: true,
  };
}
