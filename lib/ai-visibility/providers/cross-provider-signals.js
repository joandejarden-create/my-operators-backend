/**
 * Cross-provider signal foundation (Phase 3B.1).
 * Architecture only — do NOT calculate from dry-run.
 */

export const CROSS_PROVIDER_SIGNAL_FOUNDATION_READY = true;

export const FUTURE_CROSS_PROVIDER_SIGNALS = Object.freeze([
  "Provider Visibility Gap",
  "Cross-Provider Recommendation Persistence",
  "Rank Divergence",
  "Consensus Top-3",
  "Source Divergence",
]);

/**
 * Comparability key for future cross-provider analysis.
 */
export const CROSS_PROVIDER_COMPARABILITY_KEY = Object.freeze({
  fields: [
    "promptFamily",
    "promptVersion",
    "geography",
    "language",
    "intent",
    "peerSetId",
    "peerSetVersion",
    "metricVersion",
    "compatibleEntityUniverse",
    "modelIdentityVersionPolicy",
    "collectionPeriod",
  ],
  modelIdentityVersionPolicy:
    "Exact providerModel required; model generation changes create distinct execution identity",
  collectionPeriodPolicy:
    "Expose startedAt/completedAt/waveId/providerWaveId; do not claim same-time equivalence across dates",
});

/**
 * Per-provider metric compatibility — provider-pure metrics only.
 */
export const PROVIDER_METRIC_COMPATIBILITY = Object.freeze({
  openai: {
    aiPresence: true,
    recommendationRate: true,
    recommendationShare: true,
    top3: true,
    firstRecommendation: true,
    competitivePosition: true,
    questionsWon: true,
    questionsMissing: true,
    ownerIntentCoverage: true,
    decisionVisibilityCoverage: true,
    citationRate: "PARTIAL",
  },
  gemini: {
    aiPresence: true,
    recommendationRate: true,
    recommendationShare: true,
    top3: true,
    firstRecommendation: true,
    competitivePosition: true,
    questionsWon: true,
    questionsMissing: true,
    ownerIntentCoverage: true,
    decisionVisibilityCoverage: true,
    citationRate: "PARTIAL_PENDING_VALIDATION",
  },
  perplexity: {
    aiPresence: true,
    recommendationRate: true,
    recommendationShare: true,
    top3: true,
    firstRecommendation: true,
    competitivePosition: true,
    questionsWon: true,
    questionsMissing: true,
    ownerIntentCoverage: true,
    decisionVisibilityCoverage: true,
    citationRate: "PARTIAL_PENDING_VALIDATION",
  },
  claude: {
    aiPresence: true,
    recommendationRate: true,
    recommendationShare: true,
    top3: true,
    firstRecommendation: true,
    competitivePosition: true,
    questionsWon: true,
    questionsMissing: true,
    ownerIntentCoverage: true,
    decisionVisibilityCoverage: true,
    citationRate: "PARTIAL_PENDING_VALIDATION",
  },
});

/**
 * Citation rate policy per provider — not semantically equivalent without governed methodology.
 */
export const CITATION_RATE_COMPATIBILITY = Object.freeze({
  openai: {
    CITATION_RATE_SUPPORTED: true,
    SEMANTICALLY_COMPARABLE_TO_OPENAI: true,
    LIMITATION: "PARTIAL — inline url_citation may not cover all search sources",
    METRIC_STATUS: "PARTIAL",
  },
  gemini: {
    CITATION_RATE_SUPPORTED: true,
    SEMANTICALLY_COMPARABLE_TO_OPENAI: false,
    LIMITATION: "Grounding chunk/support structure differs; no OpenAI-equivalent inline url_citation",
    METRIC_STATUS: "PROVIDER_SPECIFIC_PENDING_VALIDATION",
  },
  perplexity: {
    CITATION_RATE_SUPPORTED: true,
    SEMANTICALLY_COMPARABLE_TO_OPENAI: false,
    LIMITATION: "Top-level URL list only — no inline spans; search_results metadata separate",
    METRIC_STATUS: "PROVIDER_SPECIFIC_PENDING_VALIDATION",
  },
  claude: {
    CITATION_RATE_SUPPORTED: true,
    SEMANTICALLY_COMPARABLE_TO_OPENAI: false,
    LIMITATION: "Web search tool citations vary by block type; pause/continuation may affect completeness",
    METRIC_STATUS: "PROVIDER_SPECIFIC_PENDING_VALIDATION",
  },
});

export const SOURCE_ANALYSIS_FOUNDATION = Object.freeze({
  READY: true,
  capabilities: [
    "unique domains (provider-pure)",
    "recurring domains (provider-pure)",
    "owned vs third-party classification (provider-pure)",
    "source frequency by intent/language/geography (provider-pure)",
  ],
  LIMITATIONS: [
    "No causal claims — sources are not ranking factors or influencers",
    "Cross-provider source divergence requires real observations first",
    "Normalized citations preserve NULL for unavailable fields",
  ],
});

export const EARLY_DISCOVERABILITY_PHASE_RETAINED = true;

export const DISCOVERABILITY_ROADMAP_HANDOFF = Object.freeze({
  EARLY_DISCOVERABILITY_PHASE_RETAINED: true,
  trigger: "After multi-provider data collection begins",
  futureFoundation: [
    "OAI-SearchBot / AI crawler readiness",
    "crawlable development pages",
    "sitemap/indexability/canonical checks",
    "server/CDN bot crawl activity",
    "ChatGPT referral sessions",
    "future identifiable AI-provider referrals",
    "development-page visits",
    "engaged visits",
    "qualified development actions",
  ],
  note: "Do not defer indefinitely after multi-provider monitoring begins",
});
