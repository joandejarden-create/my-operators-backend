/**
 * Controlled multi-provider validation plan (Phase 3B.1).
 * Design only — no live execution.
 */

export const VALIDATION_PROMPTS_PER_PROVIDER = 12;
export const VALIDATION_TOTAL_PROVIDER_CALLS = 36; // 12 × 3 providers

/**
 * Deterministic validation prompt selection — adapter/evidence integrity, not statistical coverage.
 * Selected from showcase_prompts_v1 seed (no new prompt texts).
 */
export const CONTROLLED_VALIDATION_PROMPT_IDS = Object.freeze([
  // Conversion — Global EN + Mexico ES (framing A)
  "p_global_existing_asset_reposition_v1",
  "p_mx_existing_asset_reposition_es_v1",
  // Collection / Soft Brand — CALA EN + CALA ES
  "p_cala_collection_affiliation_v1",
  "p_cala_collection_affiliation_es_v1",
  // Lifestyle Positioning — Global EN + Mexico EN
  "p_global_lifestyle_strategy_v1",
  "p_mx_lifestyle_strategy_v1",
  // Branded Residences — CALA EN + Mexico ES
  "p_cala_residences_capability_v1",
  "p_mx_residences_capability_es_v1",
  // Upper-Upscale — Europe EN
  "p_europe_uu_positioning_strategy_v1",
  // Soft-Brand Flexibility — CALA ES
  "p_cala_affiliation_flexibility_es_v1",
  // Conversion framing B — Mexico EN
  "p_mx_independent_affiliation_v1",
  // Collection framing B — Global EN
  "p_global_soft_brand_shortlist_v1",
]);

export const VALIDATION_GEO_LANGUAGE_COVERAGE = Object.freeze([
  "Global EN",
  "CALA EN",
  "CALA ES",
  "Europe EN",
  "Mexico EN",
  "Mexico ES",
]);

export const VALIDATION_INTENTS_COVERED = Object.freeze([
  "Conversion",
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Branded Residences",
  "Upper-Upscale Positioning",
  "Soft-Brand Affiliation Flexibility",
]);

/**
 * Cost estimates for controlled validation — marked unknown where not calibrated.
 */
export const VALIDATION_COST_ESTIMATE = Object.freeze({
  GEMINI: {
    LOW: null,
    EXPECTED: null,
    HIGH: null,
    STATUS: "NEEDS_LIVE_USAGE_CALIBRATION",
    note: "Estimate after Phase 3B.2 live calibration with gemini-2.5-flash",
  },
  PERPLEXITY: {
    LOW: null,
    EXPECTED: null,
    HIGH: null,
    STATUS: "NEEDS_LIVE_USAGE_CALIBRATION",
    note: "Sonar may return provider cost metadata; calibrate in Phase 3B.2",
  },
  CLAUDE: {
    LOW: null,
    EXPECTED: null,
    HIGH: null,
    STATUS: "NEEDS_LIVE_USAGE_CALIBRATION",
    note: "Estimate after Phase 3B.2 with claude-sonnet-4-6 + web_search",
  },
  TOTAL: {
    STATUS: "NEEDS_LIVE_USAGE_CALIBRATION",
  },
});

export const FULL_MULTIPROVIDER_BASELINE_PLAN = Object.freeze({
  OPENAI_EXISTING: 84,
  GEMINI_FUTURE: 84,
  PERPLEXITY_FUTURE: 84,
  CLAUDE_FUTURE: 84,
  INCREMENTAL_NEW_PROVIDER_CALLS: 252,
  TOTAL_FINAL_OBSERVATIONS: 336,
  note: "Do not rerun OpenAI unless methodology/model intentionally changes",
});

export const BASELINE_TIMING_RULE = Object.freeze({
  recommendedMaxWindowDays: 14,
  storeFields: ["startedAt", "completedAt", "waveId", "providerWaveId"],
  rule: "Cross-provider data collected as close together as operationally reasonable; do not claim same-time equivalence across different collection dates",
});

export function buildControlledValidationPlan() {
  return {
    CALLS_PER_PROVIDER: VALIDATION_PROMPTS_PER_PROVIDER,
    TOTAL_CALLS: VALIDATION_TOTAL_PROVIDER_CALLS,
    PROMPT_IDS: [...CONTROLLED_VALIDATION_PROMPT_IDS],
    LANGUAGE_GEO_COVERAGE: [...VALIDATION_GEO_LANGUAGE_COVERAGE],
    INTENTS: [...VALIDATION_INTENTS_COVERED],
    PROVIDERS: ["gemini", "perplexity", "claude"],
    LIVE_CALLS: 0,
    purpose: "adapter/evidence integrity before 252-call expansion",
  };
}
