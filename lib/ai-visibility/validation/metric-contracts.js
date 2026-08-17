/**
 * Governed contracts for client-visible AI Visibility metrics.
 * No composite confidence scores. Deterministic math only.
 */

export const METRIC_CONTRACT_REGISTRY_VERSION = "ai_intelligence_metric_contracts_v1";

/** @typedef {'rate'|'count'|'rank'} MetricUnit */

/**
 * @typedef {object} MetricContract
 * @property {string} METRIC_ID
 * @property {string} DISPLAY_NAME
 * @property {string} DEFINITION
 * @property {string} NUMERATOR
 * @property {string} DENOMINATOR
 * @property {string} SCOPE
 * @property {{ min: number|null, max: number|null }} VALID_RANGE
 * @property {string} ROUNDING_RULE
 * @property {string} CURRENT_PERIOD_RULE
 * @property {string} HISTORICAL_RULE
 * @property {string} PROVIDER_RULE
 * @property {string} GEOGRAPHY_RULE
 * @property {string} LANGUAGE_RULE
 * @property {string} ZERO_RULE
 * @property {string} NOT_MONITORED_RULE
 * @property {string} EVIDENCE_REQUIREMENT
 * @property {string} RECOMPUTATION_REQUIREMENT
 * @property {MetricUnit} unit
 * @property {string} [summaryField]
 */

/** @type {Record<string, MetricContract>} */
export const METRIC_CONTRACTS = Object.freeze({
  AI_PRESENCE: {
    METRIC_ID: "AI_PRESENCE",
    DISPLAY_NAME: "AI Presence",
    DEFINITION:
      "Share of successful eligible governed prompts in which the canonical entity appeared.",
    NUMERATOR: "Unique successful eligible prompts where canonical entity was observed.",
    DENOMINATOR: "Unique successful eligible prompts in the selected monitoring period.",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal percentage point; store as 0–1 ratio",
    CURRENT_PERIOD_RULE: "current monitoring period / batch only",
    HISTORICAL_RULE: "NO accumulation into current-period KPI",
    PROVIDER_RULE: "NO cross-provider aggregation",
    GEOGRAPHY_RULE: "NO cross-geography aggregation unless explicit portfolio contract",
    LANGUAGE_RULE: "NO cross-language aggregation",
    ZERO_RULE: "0 means monitored with zero appearances; not Not Monitored",
    NOT_MONITORED_RULE: "No eligible successful prompts in scope",
    EVIDENCE_REQUIREMENT: "Each hit observation resolves to stored evidenceId",
    RECOMPUTATION_REQUIREMENT: "Independent audit must match within 1e-9 relative tolerance",
    unit: "rate",
    summaryField: "presence",
  },
  RECOMMENDATION_SHARE: {
    METRIC_ID: "RECOMMENDATION_SHARE",
    DISPLAY_NAME: "Recommendation Share",
    DEFINITION:
      "Share of positive recommendation slots across successful prompts attributed to the entity.",
    NUMERATOR: "Count of positive recommendation mentions for the entity.",
    DENOMINATOR: "Count of all positive recommendation mentions in the cohort.",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp; store 0–1",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = monitored, zero positive recs",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "Recommendation roles from evidence mentions",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9",
    unit: "rate",
    summaryField: "recommendationShare",
  },
  RECOMMENDATION_RATE: {
    METRIC_ID: "RECOMMENDATION_RATE",
    DISPLAY_NAME: "Recommendation Rate",
    DEFINITION:
      "Share of successful prompts where the entity received ≥1 positive recommendation role.",
    NUMERATOR: "Successful prompts with ≥1 positive recommendation for entity.",
    DENOMINATOR: "Successful eligible prompts.",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = present or absent but never positively recommended",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "Role classification on mentions",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9",
    unit: "rate",
    summaryField: "recommendationRate",
  },
  TOP3_RECOMMENDATION_RATE: {
    METRIC_ID: "TOP3_RECOMMENDATION_RATE",
    DISPLAY_NAME: "Top-3 Recommendation Rate",
    DEFINITION:
      "Share of successful answers where the entity was recommended in positions 1–3 when rank is known.",
    NUMERATOR: "Successful prompts with entity in top-3 recommendation positions.",
    DENOMINATOR: "Successful eligible prompts.",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = never in top 3",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "Ranked recommendation positions from evidence",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9",
    unit: "rate",
    summaryField: "top3RecommendationRate",
  },
  FIRST_RECOMMENDATION_RATE: {
    METRIC_ID: "FIRST_RECOMMENDATION_RATE",
    DISPLAY_NAME: "First Recommendation Rate",
    DEFINITION:
      "Share of successful prompts where the entity was the first recommendation.",
    NUMERATOR: "Successful prompts with first_recommendation role for entity.",
    DENOMINATOR: "Successful eligible prompts.",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = never first",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "first_recommendation role",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9",
    unit: "rate",
    summaryField: "firstRecommendationRate",
  },
  QUESTIONS_WON_COUNT: {
    METRIC_ID: "QUESTIONS_WON_COUNT",
    DISPLAY_NAME: "Questions Won",
    DEFINITION: "Count of unique successful prompts where the entity was first recommendation.",
    NUMERATOR: "Unique promptIds won",
    DENOMINATOR: "N/A (count)",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: null },
    ROUNDING_RULE: "integer",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = monitored, zero first recommendations",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "first recommendation evidence",
    RECOMPUTATION_REQUIREMENT: "Exact integer match",
    unit: "count",
    summaryField: "questionsWon",
  },
  QUESTIONS_MISSING_COUNT: {
    METRIC_ID: "QUESTIONS_MISSING_COUNT",
    DISPLAY_NAME: "Questions Missing",
    DEFINITION:
      "Count of unique successful prompts where the entity did not appear.",
    NUMERATOR: "Unique promptIds with no entity presence",
    DENOMINATOR: "N/A (count)",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: null },
    ROUNDING_RULE: "integer",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = present on all eligible prompts",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "Successful observations without entity",
    RECOMPUTATION_REQUIREMENT: "Exact integer match",
    unit: "count",
    summaryField: "questionsMissing",
  },
  COMPETITIVE_POSITION: {
    METRIC_ID: "COMPETITIVE_POSITION",
    DISPLAY_NAME: "Competitive Position",
    DEFINITION: "Rank by AI Presence among the peer set (1 = highest presence).",
    NUMERATOR: "Rank ordinal",
    DENOMINATOR: "Peer set size",
    SCOPE: "peer set × provider × geography × language × period",
    VALID_RANGE: { min: 1, max: null },
    ROUNDING_RULE: "integer rank",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "Comparable only under same peer-set version",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "Rank 0 is invalid",
    NOT_MONITORED_RULE: "No peer presence rates",
    EVIDENCE_REQUIREMENT: "Presence rates for all peers",
    RECOMPUTATION_REQUIREMENT: "Exact rank match",
    unit: "rank",
    summaryField: "competitivePosition",
  },
  QUESTIONS_WON_RATE: {
    METRIC_ID: "QUESTIONS_WON_RATE",
    DISPLAY_NAME: "Questions Won Rate",
    DEFINITION: "Share of eligible prompts where the entity is the sole first-recommendation leader.",
    NUMERATOR: "Questions Won Count",
    DENOMINATOR: "Unique successful eligible prompts",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp; store 0–1",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = monitored, never sole winner",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "First-recommendation leaders from evidence",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9",
    unit: "rate",
    summaryField: "questionsWonRate",
  },
  QUESTIONS_MISSING_RATE: {
    METRIC_ID: "QUESTIONS_MISSING_RATE",
    DISPLAY_NAME: "Questions Missing Rate",
    DEFINITION: "Share of eligible prompts with no entity presence.",
    NUMERATOR: "Questions Missing Count",
    DENOMINATOR: "Unique successful eligible prompts",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp; store 0–1",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = present on all eligible prompts",
    NOT_MONITORED_RULE: "No successful prompts",
    EVIDENCE_REQUIREMENT: "Presence checks across prompts",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9",
    unit: "rate",
    summaryField: "questionsMissingRate",
  },
  DECISION_VISIBILITY_COVERAGE: {
    METRIC_ID: "DECISION_VISIBILITY_COVERAGE",
    DISPLAY_NAME: "Decision Visibility Coverage",
    DEFINITION: "Share of decision territories with at least one monitored successful observation for the subject.",
    NUMERATOR: "Territories with presence or recommendation evidence",
    DENOMINATOR: "Governed decision territories in scope",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: 0, max: 1 },
    ROUNDING_RULE: "display to 1 decimal pp; store 0–1",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "NO",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "0 = monitored, no territory coverage",
    NOT_MONITORED_RULE: "Feature not monitored / no eligible prompts",
    EVIDENCE_REQUIREMENT: "Intent-tagged observations",
    RECOMPUTATION_REQUIREMENT: "Exact match within 1e-9 when feature implemented",
    unit: "rate",
    summaryField: "decisionVisibilityCoverage",
  },
  TOP_DECISION_TERRITORY: {
    METRIC_ID: "TOP_DECISION_TERRITORY",
    DISPLAY_NAME: "Top Decision Territory",
    DEFINITION: "Intent territory with the strongest subject visibility in the monitored period (when implemented).",
    NUMERATOR: "Territory label / id",
    DENOMINATOR: "N/A",
    SCOPE: "entity × provider × geography × language × period",
    VALID_RANGE: { min: null, max: null },
    ROUNDING_RULE: "categorical label",
    CURRENT_PERIOD_RULE: "current period only",
    HISTORICAL_RULE: "Comparable under same prompt-set version",
    PROVIDER_RULE: "NO cross-provider",
    GEOGRAPHY_RULE: "NO cross-geography",
    LANGUAGE_RULE: "NO cross-language",
    ZERO_RULE: "N/A",
    NOT_MONITORED_RULE: "Not available yet / not implemented",
    EVIDENCE_REQUIREMENT: "Intent-tagged observations",
    RECOMPUTATION_REQUIREMENT: "Exact categorical match when implemented",
    unit: "count",
    summaryField: "topDecisionTerritory",
  },
});

export function listMetricContracts() {
  return Object.values(METRIC_CONTRACTS);
}

export function getMetricContract(metricId) {
  return METRIC_CONTRACTS[metricId] || null;
}
