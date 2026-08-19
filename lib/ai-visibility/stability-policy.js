/**
 * Repeated-testing sampling policy, validation cohort, and cost model.
 * Transparent rules. No opaque numeric priority score. Scheduler remains off.
 */

import { MONITORING_COST_RATES_USD } from "./prompt-provenance.js";
import { WAVE1_COST_EVIDENCE } from "./wave1-showcase-plan.js";

export const STABILITY_POLICY_VERSION = "ai_visibility_stability_policy_v1";

export const SAMPLING_PRIORITIES = Object.freeze([
  "CRITICAL",
  "HIGH",
  "STANDARD",
  "EXPLORATORY",
]);

export const VALIDATION_COST_CAPS = Object.freeze({
  TARGET_HISTORIC_USD: 50,
  /** Stage B founder-approved execution cap. Supersedes the prior $75 design cap. */
  HARD_CAP_USD: 30,
  DATAFORSEO_CALLS: 0,
  FULL_133_PROMPT_RUN: 0,
  SCHEDULER_ENABLE: 0,
  DEPLOY: 0,
});

/** Founder-approved Stage B wave. Sole source for recurrence/stability grains. */
export const STAGE_B_AUTHORITATIVE_WAVE_ID = "aiv_stability_stage_b_20260817_f0a829";

/** Stored for audit only. Do not aggregate or infer recurrence from this wave. */
export const STAGE_B_NON_AUTHORITATIVE_WAVE_IDS = Object.freeze([
  "aiv_stability_stage_b_20260817_22c195",
]);

/** Repo-relative path for Narrative & Source Intelligence / stability downstream reads. */
export const STAGE_B_AUTHORITATIVE_REPORT_REL_PATH =
  "reports/ai-visibility/repeated-testing-stage-b-report-final-wave.json";

/**
 * Per-provider historic cost from four-provider baseline actuals (84 calls each).
 * OpenAI also has Wave-1 expected/high calibration.
 */
export const HISTORIC_PROVIDER_COST = Object.freeze({
  openai: {
    historicUsdPerCall: Number((38.41 / 84).toFixed(6)),
    sampleSize: 84,
    source: "wave1_showcase_actual_usd_38.41_over_84",
    conservativeUsdPerCall: WAVE1_COST_EVIDENCE.HIGH_PER_CALL,
    conservativeSource: "WAVE1_COST_EVIDENCE.HIGH_PER_CALL",
    expectedUsdPerCall: WAVE1_COST_EVIDENCE.EXPECTED_PER_CALL,
  },
  gemini: {
    historicUsdPerCall: Number((6.56 / 84).toFixed(6)),
    sampleSize: 84,
    source: "provider_baseline_gemini_actual_usd_6.56_over_84",
    conservativeUsdPerCall: Number(((6.56 / 84) * 1.35 * 1.25).toFixed(6)),
    conservativeSource: "baseline_actual × 1.35 × 1.25 operational buffer",
  },
  perplexity: {
    historicUsdPerCall: Number((0.47 / 84).toFixed(6)),
    sampleSize: 84,
    source: "provider_baseline_perplexity_actual_usd_0.47_over_84",
    conservativeUsdPerCall: Number(Math.max((0.47 / 84) * 1.35 * 1.25, 0.02).toFixed(6)),
    conservativeSource: "baseline_actual × buffer, floor $0.02",
  },
  claude: {
    historicUsdPerCall: Number((58.19 / 84).toFixed(6)),
    sampleSize: 84,
    source: "provider_baseline_claude_actual_usd_58.19_over_84",
    conservativeUsdPerCall: Number(((58.19 / 84) * 1.15).toFixed(6)),
    conservativeSource: "baseline_actual × 1.15",
  },
  blendedOpenAiPlanning: {
    historicUsdPerCall: MONITORING_COST_RATES_USD.HISTORIC_EFFECTIVE,
    conservativeUsdPerCall: MONITORING_COST_RATES_USD.CONSERVATIVE,
    note: "Do not use the blended OpenAI $0.677 as the only per-provider rate.",
  },
});

export const SAMPLING_RULES = Object.freeze({
  CRITICAL: [
    "scenario commercialPriority = CRITICAL",
    "executive finding-driving prompt",
    "prior MIXED, CHANGING, or HIGH_VARIABILITY on this grain",
    "client-selected CRITICAL",
    "observed-demand HIGH tier AND a core owner-decision scenario",
  ],
  HIGH: [
    "scenario commercialPriority = HIGH",
    "important observed demand with material commercial relevance",
    "major portfolio competitor question (Autograph, Design Hotels, Westin context)",
    "Spanish observed prompt in Mexico when Brand AI includes Mexico/CALA Spanish",
  ],
  STANDARD: [
    "mapped scenario with consistent history",
    "PAA-only observed theme with lower commercial importance",
  ],
  EXPLORATORY: [
    "DERIVED prompts unless they solve a clear test-quality problem vs the observed parent",
    "investigation variants",
  ],
  OBSERVED_DEMAND_NOT_AUTOMATIC_CRITICAL:
    "Observed demand is one input. Expert-commercial importance is another. Volume-only weighting is forbidden.",
});

export const TIERED_REPETITION_POLICY = Object.freeze({
  SAME_CYCLE: {
    CRITICAL: { exactRepeats: 2, controlledVariant: "optional_later", providers: "all_four" },
    HIGH: { exactRepeats: 2, controlledVariant: 0, providers: "two_to_four" },
    STANDARD: { exactRepeats: 1, controlledVariant: 0, providers: "selected_monitored" },
    EXPLORATORY: { exactRepeats: 1, controlledVariant: 0, providers: "openai_or_sampled" },
  },
  LONGITUDINAL: {
    CRITICAL: "biweekly or monthly when scheduler is later approved",
    HIGH: "monthly",
    STANDARD: "period cadence only",
    EXPLORATORY: "periodic sample",
  },
  NOTE: "Same-cycle variability and longitudinal change are separate.",
});

export const PROVIDER_SAMPLING_POLICY = Object.freeze({
  CRITICAL: ["openai", "gemini", "perplexity", "claude"],
  HIGH: ["openai", "perplexity"],
  STANDARD: ["openai"],
  EXPLORATORY: ["openai"],
  comparability:
    "Do not mix provider sets inside one grain. Cross-provider alignment requires at least two providers with their own series.",
});

/**
 * Controlled V1 validation cohort (16 prompts). Not the 133-prompt universe.
 */
export const VALIDATION_COHORT = Object.freeze([
  {
    promptId: "p_cala_independent_affiliation_v1",
    origin: "SCENARIO",
    samplingPriority: "CRITICAL",
    why: "AC independent upper-upscale conversion — expert CORE, Marriott/CALA demo",
    providers: ["openai", "gemini", "perplexity", "claude"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_collection_affiliation_v1",
    origin: "SCENARIO",
    samplingPriority: "CRITICAL",
    why: "Autograph / collection affiliation — portfolio competitor question",
    providers: ["openai", "gemini", "perplexity", "claude"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_soft_brand_shortlist_v1",
    origin: "SCENARIO",
    samplingPriority: "CRITICAL",
    why: "Soft-brand shortlist — likely provider-disagreement example",
    providers: ["openai", "gemini", "perplexity", "claude"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_design_local_character_v1",
    origin: "SCENARIO",
    samplingPriority: "HIGH",
    why: "Design Hotels / local character",
    providers: ["openai", "perplexity"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_uu_owner_shortlist_v1",
    origin: "SCENARIO",
    samplingPriority: "HIGH",
    why: "Upper-upscale owner shortlist — Westin representation context when prompt-linked",
    providers: ["openai", "perplexity"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_residences_capability_v1",
    origin: "SCENARIO",
    samplingPriority: "HIGH",
    why: "Branded residences capability — scenario-only expert layer",
    providers: ["openai", "perplexity"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_affiliation_flexibility_v1",
    origin: "SCENARIO",
    samplingPriority: "CRITICAL",
    why: "Owner flexibility/control — search-demand absence does not reduce expert importance",
    providers: ["openai", "perplexity"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_lifestyle_strategy_v1",
    origin: "SCENARIO",
    samplingPriority: "HIGH",
    why: "Lifestyle/individuality — scenario-only complementary layer",
    providers: ["openai", "perplexity"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_cala_existing_asset_reposition_v1",
    origin: "SCENARIO",
    samplingPriority: "HIGH",
    why: "Conversion reposition family — keep separate from independent-affiliation exact-repeat denominator",
    providers: ["openai", "perplexity"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "CALA",
  },
  {
    promptId: "p_global_owner_economics_brand_v1",
    origin: "SCENARIO",
    samplingPriority: "HIGH",
    why: "Owner economics / fees — scenario counterpart to observed franchise fees",
    providers: ["openai"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "Global",
  },
  {
    promptId: "p_obs_hotel_franchise_fees_en_v1",
    origin: "OBSERVED",
    samplingPriority: "HIGH",
    why: "Licensed HIGH demand + fees/economics. First observation only",
    providers: ["openai"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "United States",
    monitoringEligible: false,
  },
  {
    promptId: "p_obs_soft_brand_hotel_en_v1",
    origin: "OBSERVED",
    samplingPriority: "HIGH",
    why: "Observed soft-brand theme; commercial importance with collection scenario",
    providers: ["openai"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "United States",
    monitoringEligible: false,
  },
  {
    promptId: "p_obs_franquicia_hotelera_es_v1",
    origin: "OBSERVED",
    samplingPriority: "HIGH",
    why: "Spanish observed prompt — Mexico/es. Do not translate.",
    providers: ["openai"],
    exactRepeats: 1,
    language: "es",
    geographyKey: "Mexico",
    monitoringEligible: false,
  },
  {
    promptId: "p_obs_hotel_branded_residences_en_v1",
    origin: "OBSERVED",
    samplingPriority: "HIGH",
    why: "Branded residences observed theme — HIGH for residence-capable portfolios",
    providers: ["openai"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "United States",
    monitoringEligible: false,
  },
  {
    promptId: "p_obs_hotel_franchise_vs_management_agreement_en_v1",
    origin: "OBSERVED",
    samplingPriority: "STANDARD",
    why: "PAA-supported franchise vs HMA. STANDARD unless later instability appears",
    providers: ["openai"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "United States",
    monitoringEligible: false,
  },
  {
    promptId: "p_obs_hotel_franchise_fees_derived_en_v1",
    origin: "DERIVED",
    samplingPriority: "EXPLORATORY",
    why: "Compare later to literal observed parent. Not bias correction.",
    providers: ["openai"],
    exactRepeats: 1,
    language: "en",
    geographyKey: "United States",
    monitoringEligible: false,
  },
]);

export function resolveSamplingPriority(input = {}) {
  const client = String(input.clientPriority || "").toUpperCase();
  if (SAMPLING_PRIORITIES.includes(client)) return client;
  if (input.priorInstability === true) return "CRITICAL";
  if (input.executiveFindingDriving === true) return "CRITICAL";
  if (String(input.commercialPriority || "").toUpperCase() === "CRITICAL") return "CRITICAL";
  if (input.promptOrigin === "DERIVED" && input.derivedSolvesTestQuality !== true) {
    return "EXPLORATORY";
  }
  if (input.demandTier === "HIGH" && input.coreOwnerDecision === true) return "HIGH";
  if (String(input.commercialPriority || "").toUpperCase() === "HIGH") return "HIGH";
  if (input.demandTier === "LOW") return "EXPLORATORY";
  return input.defaultPriority || "STANDARD";
}

export function expandValidationCalls(cohort = VALIDATION_COHORT) {
  const calls = [];
  for (const row of cohort) {
    const reps = Number(row.exactRepeats) || 1;
    for (const provider of row.providers || []) {
      for (let i = 0; i < reps; i += 1) {
        calls.push({
          promptId: row.promptId,
          provider,
          language: row.language,
          geographyKey: row.geographyKey,
          origin: row.origin,
          samplingPriority: row.samplingPriority,
          repeatType: "EXACT_REPEAT",
          repeatIndex: i + 1,
        });
      }
    }
  }
  return calls;
}

export function estimateValidationCost(cohort = VALIDATION_COHORT) {
  const calls = expandValidationCalls(cohort);
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  for (const c of calls) {
    if (byProvider[c.provider] != null) byProvider[c.provider] += 1;
  }
  let historic = 0;
  let conservative = 0;
  for (const [provider, n] of Object.entries(byProvider)) {
    const rates = HISTORIC_PROVIDER_COST[provider];
    historic += n * rates.historicUsdPerCall;
    conservative += n * rates.conservativeUsdPerCall;
  }
  const projected = {
    openai: Number((byProvider.openai * HISTORIC_PROVIDER_COST.openai.historicUsdPerCall).toFixed(6)),
    gemini: Number((byProvider.gemini * HISTORIC_PROVIDER_COST.gemini.historicUsdPerCall).toFixed(6)),
    perplexity: Number(
      (byProvider.perplexity * HISTORIC_PROVIDER_COST.perplexity.historicUsdPerCall).toFixed(6)
    ),
    claude: Number((byProvider.claude * HISTORIC_PROVIDER_COST.claude.historicUsdPerCall).toFixed(6)),
  };
  projected.total = Number(
    (projected.openai + projected.gemini + projected.perplexity + projected.claude).toFixed(6)
  );
  const expected = Number(historic.toFixed(4));
  const conservativeTotal = Number(conservative.toFixed(4));
  const stop =
    projected.total > VALIDATION_COST_CAPS.HARD_CAP_USD ||
    conservativeTotal > VALIDATION_COST_CAPS.HARD_CAP_USD;
  return {
    promptCount: cohort.length,
    observed: cohort.filter((r) => r.origin === "OBSERVED").length,
    scenario: cohort.filter((r) => r.origin === "SCENARIO").length,
    derived: cohort.filter((r) => r.origin === "DERIVED").length,
    exactRepetitions: calls.length,
    variantRepetitions: 0,
    totalValidationCalls: calls.length,
    callsByProvider: byProvider,
    PROJECTED_OPENAI_COST: projected.openai,
    PROJECTED_GEMINI_COST: projected.gemini,
    PROJECTED_PERPLEXITY_COST: projected.perplexity,
    PROJECTED_CLAUDE_COST: projected.claude,
    PROJECTED_TOTAL_COST: projected.total,
    expectedHistoricCost: expected,
    conservativeCost: conservativeTotal,
    targetCapUsd: VALIDATION_COST_CAPS.TARGET_HISTORIC_USD,
    hardCapUsd: VALIDATION_COST_CAPS.HARD_CAP_USD,
    withinTarget: expected <= VALIDATION_COST_CAPS.TARGET_HISTORIC_USD,
    STOP: stop,
    STAGE_B: stop ? "REPEATED_TESTING_BUDGET_BLOCKED" : "READY_UNDER_HARD_CAP",
    FULL_133_PROMPT_RUN: 0,
  };
}
