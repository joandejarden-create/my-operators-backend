/**
 * Portfolio-level question metrics — unique prompts, never brand×prompt sums.
 */

import { computeQuestionsWon, computeQuestionsMissing } from "./metrics.js";

export const PORTFOLIO_QUESTION_METRICS_VERSION = "ai_visibility_portfolio_question_metrics_v1";

/**
 * Unique-prompt portfolio Questions Won / Missing for one monitoring period.
 *
 * QUESTIONS_WON = unique prompts where ANY entitled brand is sole first recommendation
 * QUESTIONS_MISSING = unique prompts where NO entitled brand appears (present)
 *
 * @param {object[]} observations — success observations for one period/slot scope
 * @param {string[]} entitledBrandIds
 */
export function computePortfolioQuestionMetrics(observations, entitledBrandIds) {
  const ids = [...new Set((entitledBrandIds || []).filter(Boolean))];
  const relevant = (observations || []).filter((o) => o.success);
  const promptIds = [...new Set(relevant.map((o) => o.promptId).filter(Boolean))];

  if (!promptIds.length) {
    return {
      version: PORTFOLIO_QUESTION_METRICS_VERSION,
      eligiblePromptCount: 0,
      questionsWonCount: 0,
      questionsMissingCount: 0,
      questionsWonRate: null,
      questionsMissingRate: null,
      wonPromptIds: [],
      missingPromptIds: [],
    };
  }

  const wonPromptIds = [];
  const missingPromptIds = [];

  for (const promptId of promptIds) {
    const obs = relevant.filter((o) => o.promptId === promptId);
    const present = new Set();
    /** @type {Record<string, number>} */
    const firstCounts = {};
    for (const o of obs) {
      for (const id of o.presentEntityIds || []) {
        if (ids.includes(id)) present.add(id);
      }
      const first = (o.recommendedEntityIds || [])[0];
      if (first && ids.includes(first)) {
        firstCounts[first] = (firstCounts[first] || 0) + 1;
      }
    }
    const leaders = Object.entries(firstCounts)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .filter(([, c], _, arr) => c === arr[0][1])
      .map(([id]) => id);

    if (leaders.length === 1) {
      wonPromptIds.push(promptId);
    } else if (present.size === 0) {
      missingPromptIds.push(promptId);
    }
  }

  const eligible = promptIds.length;
  const won = wonPromptIds.length;
  const missing = missingPromptIds.length;

  return {
    version: PORTFOLIO_QUESTION_METRICS_VERSION,
    eligiblePromptCount: eligible,
    questionsWonCount: won,
    questionsMissingCount: missing,
    questionsWonRate: eligible > 0 ? won / eligible : null,
    questionsMissingRate: eligible > 0 ? missing / eligible : null,
    wonPromptIds,
    missingPromptIds,
    INVARIANT_WON_LE_ELIGIBLE: won <= eligible,
    INVARIANT_MISSING_LE_ELIGIBLE: missing <= eligible,
    INVARIANT_RATE_LE_1:
      (won <= eligible && missing <= eligible) &&
      (won / eligible <= 1 || eligible === 0) &&
      (missing / eligible <= 1 || eligible === 0),
  };
}

/**
 * Brand-level questions from observations (unique prompts).
 *
 * Presence-led Present / Missing:
 *   PRESENT_N = unique prompts where subject Presence = true
 *   MISSING_N = unique prompts where subject Presence = false
 *   PRESENT_N + MISSING_N = MONITORED_N (eligible unique prompts)
 *
 * questionsWon remains recommendation-led (sole first) and is not required
 * to sum with Missing to Monitored.
 */
export function computeBrandQuestionMetrics(observations, brandId) {
  const relevant = (observations || []).filter((o) => o.success);
  const promptIds = [...new Set(relevant.map((o) => o.promptId).filter(Boolean))];
  const won = computeQuestionsWon(relevant, brandId, promptIds);
  const missing = computeQuestionsMissing(relevant, brandId, promptIds);
  const eligible = promptIds.length;
  const wonCount = won.count ?? won.value ?? 0;
  const missingCount = missing.count ?? missing.value ?? 0;
  const presentCount = Math.max(0, eligible - missingCount);
  const presenceRate = eligible > 0 ? presentCount / eligible : null;
  return {
    eligiblePromptCount: eligible,
    questionsWonCount: wonCount,
    questionsPresentCount: presentCount,
    questionsMissingCount: missingCount,
    questionsWonRate: eligible > 0 ? wonCount / eligible : null,
    questionsMissingRate: eligible > 0 ? missingCount / eligible : null,
    presenceRate,
    INVARIANT_RATE_LE_1: wonCount <= eligible && missingCount <= eligible,
    INVARIANT_PRESENT_PLUS_MISSING_EQ_MONITORED: presentCount + missingCount === eligible,
    INVARIANT_PRESENCE_EQ_PRESENT_OVER_MONITORED:
      presenceRate == null ||
      (eligible > 0 && Math.abs(presenceRate - presentCount / eligible) < 1e-12),
  };
}
