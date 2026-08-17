/**
 * Operational metric contracts for Validation Scorecard Monitoring Coverage/Ops.
 * UI must not invent calculations — use these definitions.
 */

export const OPS_METRIC_CONTRACT_VERSION = "ai_intelligence_ops_metric_contracts_v1";

/** @type {Record<string, object>} */
export const OPS_METRIC_CONTRACTS = Object.freeze({
  PROMPTS_ATTEMPTED: {
    METRIC_ID: "PROMPTS_ATTEMPTED",
    DEFINITION: "Count of monitoring runs with status attempted (completed, failed, or partial).",
    SOURCE_FIELDS: ["runs[].status"],
    SCOPE: "batch | provider | language | geography",
    DENOMINATOR: "N/A",
    NULL_RULE: "Missing runs → 0 for empty batch; Not Available if batch inventory missing",
    ESTIMATION_RULE: "Exact count from stored runs",
    AGGREGATION_RULE: "Sum across scoped runs",
  },
  PROMPTS_SUCCESSFUL: {
    METRIC_ID: "PROMPTS_SUCCESSFUL",
    DEFINITION: "Runs with status=completed and usable response/evidence.",
    SOURCE_FIELDS: ["runs[].status"],
    SCOPE: "batch | provider | language | geography",
    DENOMINATOR: "N/A",
    NULL_RULE: "0 when none",
    ESTIMATION_RULE: "Exact",
    AGGREGATION_RULE: "Sum",
  },
  RUN_COUNT: {
    METRIC_ID: "RUN_COUNT",
    DEFINITION: "Total stored run records in scope.",
    SOURCE_FIELDS: ["runs"],
    SCOPE: "batch | provider | language | geography",
    DENOMINATOR: "N/A",
    NULL_RULE: "0",
    ESTIMATION_RULE: "Exact",
    AGGREGATION_RULE: "Sum",
  },
  BATCH_COUNT: {
    METRIC_ID: "BATCH_COUNT",
    DEFINITION: "Distinct batchId values in scope.",
    SOURCE_FIELDS: ["summaries[].batchId"],
    SCOPE: "provider | language | geography | global",
    DENOMINATOR: "N/A",
    NULL_RULE: "0",
    ESTIMATION_RULE: "Exact",
    AGGREGATION_RULE: "Count distinct",
  },
  TOKENS: {
    METRIC_ID: "TOKENS",
    DEFINITION: "Sum of usage.inputTokens / outputTokens / totalTokens when present.",
    SOURCE_FIELDS: ["runs[].usage"],
    SCOPE: "batch | provider | language | geography",
    DENOMINATOR: "N/A",
    NULL_RULE: "Not Available when no runs expose usage — never render as 0",
    ESTIMATION_RULE: "Exact sum of available usage fields only",
    AGGREGATION_RULE: "Sum available; flag incomplete",
  },
  ESTIMATED_COST: {
    METRIC_ID: "ESTIMATED_COST",
    DEFINITION: "Sum of runs[].estimatedCost when present (USD estimate).",
    SOURCE_FIELDS: ["runs[].estimatedCost"],
    SCOPE: "batch | provider | language | geography",
    DENOMINATOR: "N/A",
    NULL_RULE: "Not Available when no cost fields — never invent; never display missing as 0",
    ESTIMATION_RULE: "Label Estimated unless reconciled to billing",
    AGGREGATION_RULE: "Sum available estimatedCost only",
  },
  CITATION_YIELD: {
    METRIC_ID: "CITATION_YIELD",
    DEFINITION: "Successful responses with ≥1 citation / successful responses.",
    SOURCE_FIELDS: ["evidence.payload.citations", "runs.status"],
    SCOPE: "provider | language | geography | global",
    DENOMINATOR: "successful responses with evidence join",
    NULL_RULE: "Not Available if evidence join incomplete",
    ESTIMATION_RULE: "Exact ratio",
    AGGREGATION_RULE: "Rate",
  },
  RECOMMENDATION_RESPONSE_RATE: {
    METRIC_ID: "RECOMMENDATION_BEARING_RESPONSE_RATE",
    DEFINITION:
      "Successful responses with ≥1 positive recommendation role / successful responses.",
    SOURCE_FIELDS: ["evidence.payload.mentions[].role"],
    SCOPE: "provider | language | geography | global",
    DENOMINATOR: "successful responses",
    NULL_RULE: "Not Available if evidence join incomplete",
    ESTIMATION_RULE: "Exact",
    AGGREGATION_RULE: "Rate",
  },
});

export function listOpsMetricContracts() {
  return Object.values(OPS_METRIC_CONTRACTS);
}
