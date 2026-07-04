/**
 * Operator match score UI helpers — shared by server browser bundle and tests.
 */
import {
  OPERATOR_MATCH_SCORE_BANDS,
  OPERATOR_MATCH_AGGREGATION,
} from "./operator-alignment-scoring-weight-config.js";

const BREAKDOWN_CLASS_BY_UI = {
  "match-score-high": "high",
  "match-score-medium": "medium",
  "match-score-weak": "low",
  "match-score-poor": "poor",
};

/** @param {typeof OPERATOR_MATCH_SCORE_BANDS} bands */
export function sortScoreBandsDesc(bands) {
  return [...bands].sort((a, b) => b.min - a.min);
}

/**
 * @param {number|string|null|undefined} score
 * @param {typeof OPERATOR_MATCH_SCORE_BANDS} [bands]
 */
export function resolveOperatorMatchScoreBand(score, bands = OPERATOR_MATCH_SCORE_BANDS) {
  const n = Number(score);
  if (!Number.isFinite(n)) return null;
  const sorted = sortScoreBandsDesc(bands);
  for (const band of sorted) {
    if (n >= band.min) return band;
  }
  return sorted[sorted.length - 1] || null;
}

/** @param {number|string|null|undefined} score */
export function getOperatorMatchAlignmentScoreClass(score) {
  const band = resolveOperatorMatchScoreBand(score);
  return band ? band.uiClass : "";
}

/** @param {number|string|null|undefined} score */
export function getOperatorMatchBreakdownScoreClass(score) {
  const uiClass = getOperatorMatchAlignmentScoreClass(score);
  return BREAKDOWN_CLASS_BY_UI[uiClass] || "medium";
}

/**
 * Narrative tier for operator alignment copy (strong / moderate / weak / poor).
 * @param {number|string|null|undefined} score
 */
export function getOperatorMatchNarrativeTier(score) {
  const n = Number(score);
  if (!Number.isFinite(n)) return "poor";
  const sorted = sortScoreBandsDesc(OPERATOR_MATCH_SCORE_BANDS);
  for (let i = 0; i < sorted.length; i++) {
    if (n >= sorted[i].min) {
      if (i === 0) return "strong";
      if (i === 1) return "moderate";
      if (i === 2) return "weak";
      return "poor";
    }
  }
  return "poor";
}

/** Min factor score counted as "strong" in breakdown narratives. */
export function getOperatorMatchFactorStrongMin() {
  const sorted = sortScoreBandsDesc(OPERATOR_MATCH_SCORE_BANDS);
  return sorted[0] ? sorted[0].min : 80;
}

/** Factor scores below moderate band min are "weak" in breakdown narratives. */
export function getOperatorMatchFactorWeakBelow() {
  const sorted = sortScoreBandsDesc(OPERATOR_MATCH_SCORE_BANDS);
  return sorted[1] ? sorted[1].min : 50;
}

/** Payload embedded in /js/generated/operator-match-scoring-config.js */
export function getOperatorMatchScoreBrowserPayload() {
  return {
    bands: OPERATOR_MATCH_SCORE_BANDS,
    aggregation: OPERATOR_MATCH_AGGREGATION,
    factorStrongMin: getOperatorMatchFactorStrongMin(),
    factorWeakBelow: getOperatorMatchFactorWeakBelow(),
    sourceModule: "lib/operator-alignment-scoring-weight-config.js",
  };
}
