/**
 * Second Normal Operator Monitoring Wave — Readiness Plan V1
 * Purpose: longitudinal repeatability, gap recurrence, Arbor evidence, future index research.
 * Does NOT execute provider calls. Returns plan + CLI commands for founder approval.
 */

import { costOperatorFoundationWave } from "./cost-model.js";
import { OPERATOR_AI_UNIVERSE, PRIMARY_OPERATOR_COUNT } from "./universe.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { ARBOR_LODGING_ID } from "./comparability.js";

export const SECOND_WAVE_READINESS_VERSION = "operator_second_wave_readiness_v1";

export const PRIOR_PERIOD = Object.freeze({
  id: "aiv_operator_presence_validation_20260818_1342_20ee11",
  date: "2026-08-18",
  responses: 83,
  totalCalls: 84,
  cost: 23.13,
  status: "PRODUCTION_VALIDATED",
});

export function buildSecondWaveReadinessPlan() {
  const cost = costOperatorFoundationWave();

  return {
    version: SECOND_WAVE_READINESS_VERSION,
    priorPeriod: PRIOR_PERIOD,
    newPeriodReady: true,
    scenarios: OPERATOR_DECISION_SCENARIOS.length,
    primaryOperators: PRIMARY_OPERATOR_COUNT,
    executionGrain: "PROMPT_X_PROVIDER",
    plannedCalls: cost.totalCalls,
    providerBreakdown: {
      openai: cost.openaiCalls,
      gemini: cost.geminiCalls,
      perplexity: cost.perplexityCalls,
      claude: cost.claudeCalls,
    },
    estimatedCost: `$${cost.projectedHistoricCost.toFixed(2)}`,
    costCap: `$${cost.hardCapUsd}`,
    costGate: cost.costGate,

    longitudinalOutputs: {
      presenceRepeatability: true,
      competitiveGapRecurrence: true,
      providerRecurrence: true,
      arborPositiveEvidenceCheck: true,
      remingtonEvidenceCheck: true,
    },

    repeatabilityContract: {
      grainDimensions: ["operator", "scenario", "provider"],
      comparisonLabels: [
        "PRESENT_BOTH_PERIODS",
        "PRESENT_CURRENT_ONLY",
        "PRESENT_PRIOR_ONLY",
        "ABSENT_BOTH",
        "PROVIDER_UNAVAILABLE",
      ],
      note: "Two periods = current vs prior. Not a trend.",
    },

    gapRecurrenceContract: {
      currentPromotedGaps: 8,
      evaluationLabels: [
        "RECURRENT",
        "NOT_RECURRENT_YET",
        "RESOLVED_CURRENT_PERIOD",
        "NOT_COMPARABLE",
        "PROVIDER_UNAVAILABLE",
      ],
      autoRevoke: false,
      note: "Do not automatically revoke prior certified gap from a single non-recurrence.",
    },

    arborPlan: {
      operatorId: ARBOR_LODGING_ID,
      currentPositiveMentions: 0,
      currentPositiveGoldCases: 0,
      minimumForCompetitiveInterpretation: 3,
      biasPromptsTowardArbor: false,
      note: "Track naturally. Do not force mentions. Extract + validate identity if observed.",
    },

    remingtonPlan: {
      trackAliases: true,
      bareRemingtonSafety: true,
      evidenceGoals: ["CALA_scope_strength", "full_service_relevance"],
      truthUpgradeFromAiOutput: false,
      note: "AI observations may indicate need for separate truth validation, not directly upgrade truth.",
    },

    policyReviewAfterWave: {
      luxuryPolicyReviewReady: "POSSIBLE",
      resortPolicyReviewReady: "POSSIBLE",
      note: "After combined truth + recurring Presence evidence, audit whether policy upgrade is justified. Not automatic.",
    },

    operatorIndexReadiness: {
      currentStatus: "BLOCKED",
      postSecondWaveResearchReadiness: "PARTIAL",
      minimumPeriodsForIndex: 3,
      requirements: [
        "≥3 measurement periods",
        "scenario comparability across periods",
        "CORE peer density ≥ 2 per scenario",
        "provider coverage stable",
        "Arbor excluded until positive evidence",
        "scope integrity validated",
      ],
    },

    executionCommand: "npm run operator-presence-validation:execute",
    planCommand: "npm run operator-presence-validation:plan",

    readyToExecute: true,
    providerCallsExecuted: 0,
    spend: "$0",
  };
}
