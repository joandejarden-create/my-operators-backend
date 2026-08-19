/**
 * Customer-safe info icon copy contracts for benchmark / competitive moat UI.
 */

export const INFO_CONTRACT_VERSION = "competitive_moat_info_contracts_v1";

export const OWNER_INTENT_COPY = Object.freeze({
  title: "Owner Intent",
  body:
    "Owner Intent represents the hotel owner or developer decision Dealality is testing, such as brand " +
    "affiliation, conversion, flexibility or market entry. Dealality may use multiple governed question " +
    "formulations to measure the same decision.",
});

export const DECISION_CONTEXT_COPY = Object.freeze({
  title: "Decision Context",
  body:
    "Decision Context describes the business situation behind the measurement. It helps explain what the AI " +
    "was being asked to evaluate without exposing Dealality's exact production prompt.",
});

export const HOW_DEALALITY_MEASURES_AI_COPY = Object.freeze({
  title: "How Dealality Measures AI",
  body:
    "Dealality tests representative hotel owner and developer decision scenarios across monitored AI " +
    "providers, markets and languages. Results are measured using governed scenarios and repeat observations. " +
    "Exact production prompts and testing sequences are proprietary.",
});

export const QUESTIONS_MISSING_COPY = Object.freeze({
  title: "Questions Missing",
  body:
    "A question is considered missing when your brand is absent across every comparable monitored provider " +
    "for that owner-decision observation. Missing does not mean zero demand or that another brand 'won.'",
});

export const BENCHMARK_STILL_DEVELOPING_COPY = Object.freeze({
  title: "Benchmark still developing",
  body:
    "Dealality only shows a numeric benchmark after the brand and Owner Intent pass its measurement-quality " +
    "requirements. Until then, the underlying Presence observation may still be shown without an uncertified score.",
});

export const OBSERVED_COMPETITIVE_SET_COPY = Object.freeze({
  title: "Observed Competitive Set",
  body:
    "Your declared competitors are the organizations you identify as relevant. " +
    "Observed competitors are the brands or operators that repeatedly appear as " +
    "alternatives or peers across comparable Dealality owner-decision scenarios. " +
    "This can reveal competitors that differ from a traditional comp set.",
});

export const EMERGING_COMPETITOR_COPY = Object.freeze({
  title: "Emerging Competitor",
  body:
    "A competitor whose relevant presence is increasing across comparable Dealality " +
    "measurement periods.",
  showOnlyWhenLongitudinalGateSatisfied: true,
});

export const HISTORICAL_INTELLIGENCE_COPY = Object.freeze({
  title: "Historical Intelligence",
  body:
    "Dealality preserves repeated AI observations over real measurement periods so " +
    "changes in visibility, competitive context, representation and cited sources can " +
    "be evaluated over time.",
  noInfluenceClaim: true,
});

export const AI_PRESENCE_INDEX_COPY = Object.freeze({
  title: "AI Presence Index",
  body:
    "Measures how often your brand appears in a specific owner-decision context " +
    "relative to directly comparable brands measured across the same AI observations. " +
    "100 represents competitive parity. An index of 125 means your brand's Presence " +
    "is 25% above the relevant benchmark.",
});

export const COMPETITIVE_BENCHMARK_COPY = Object.freeze({
  title: "Competitive Benchmark",
  body:
    "The relevant peer-group performance level used to calculate your index. " +
    "Dealality selects a comparable benchmark using governed characteristics, " +
    "decision context and measurement comparability. The complete benchmark dataset " +
    "and cohort methodology are proprietary.",
});

export const GAP_TO_LEADER_COPY = Object.freeze({
  title: "Gap to Leader",
  body:
    "The difference between your current index and the strongest comparable observed " +
    "performer within the relevant benchmark cohort.",
  suppressLeaderWhenPolicyBlocks: true,
});

export const CORE_PEERS_COPY = Object.freeze({
  title: "Core Peers",
  body:
    "Brands considered direct commercial alternatives for this specific owner " +
    "decision. Dealality uses governed commercial characteristics to determine " +
    "relevant comparison groups.",
});

export const ALL_INFO_CONTRACTS = Object.freeze({
  OWNER_INTENT: OWNER_INTENT_COPY,
  DECISION_CONTEXT: DECISION_CONTEXT_COPY,
  HOW_DEALALITY_MEASURES_AI: HOW_DEALALITY_MEASURES_AI_COPY,
  QUESTIONS_MISSING: QUESTIONS_MISSING_COPY,
  BENCHMARK_STILL_DEVELOPING: BENCHMARK_STILL_DEVELOPING_COPY,
  OBSERVED_COMPETITIVE_SET: OBSERVED_COMPETITIVE_SET_COPY,
  EMERGING_COMPETITOR: EMERGING_COMPETITOR_COPY,
  HISTORICAL_INTELLIGENCE: HISTORICAL_INTELLIGENCE_COPY,
  AI_PRESENCE_INDEX: AI_PRESENCE_INDEX_COPY,
  COMPETITIVE_BENCHMARK: COMPETITIVE_BENCHMARK_COPY,
  GAP_TO_LEADER: GAP_TO_LEADER_COPY,
  CORE_PEERS: CORE_PEERS_COPY,
});

/**
 * Validate info contract completeness.
 */
export function validateInfoContracts() {
  const required = Object.keys(ALL_INFO_CONTRACTS);
  const missing = required.filter((k) => !ALL_INFO_CONTRACTS[k]?.title || !ALL_INFO_CONTRACTS[k]?.body);
  return { ok: missing.length === 0, required, missing };
}
