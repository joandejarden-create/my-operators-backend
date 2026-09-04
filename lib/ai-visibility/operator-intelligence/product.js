/**
 * Operator AI Intelligence product shell — separate from Brand AI.
 */

export const OPERATOR_AI_PRODUCT = Object.freeze({
  title: "Operator AI Intelligence",
  subtitle:
    "Understand how AI represents hotel management companies when owners evaluate who should operate a hotel.",
  primaryDecisionQuestion: "Who should operate the hotel?",
  route: "/operator/ai-intelligence",
  apiNamespace: "/api/ai-visibility/operator",
  userType: "operator",
  marketIntelligenceNav: "Operator AI Intelligence",
  ownerVisibleByDefault: false,
  brandVisible: false,
  schedulerEnable: 0,
  ownerAiBuild: 0,
  brandUiWork: 0,
  recommendationMetrics: 0,
  censusReads: 0,
  dataforseoCalls: 0,
  storageNamespace: "data/ai-visibility/runtime/operator/",
  longitudinalReady: "PARTIAL",
});

export const OPERATOR_REUSE_INVENTORY = Object.freeze({
  REUSE_AS_IS: [
    "provider adapters (ChatGPT/Gemini/Perplexity/Claude)",
    "response storage abstraction + gitignored runtime root",
    "citation extraction (provider-supplied only)",
    "source domain parse / owned vs external mix contract",
    "prompt provenance fields (origin SCENARIO)",
    "stability N1/N2/N>=3 language (no numeric confidence)",
    "measurement-period / idempotency / cost ledger patterns",
    "current-vs-prior + common-cohort comparison design",
    "All Providers derived aggregation contract",
    "HISTORIC_PROVIDER_COST rate card",
    "Memberstack + workspace gates",
    "gitleaks CI secret scan",
    "normalizeMatchKey (operator span finder is operator-specific; Brand findEntitySpans is not used)",
  ],
  REUSE_WITH_ADAPTER: [
    "Presence classifier → OPERATOR_SIGNAL_PRESENCE (operator-only index + operating-context)",
    "Questions Missing denominator (operator absent across comparable providers)",
    "competitive gap → owner-decision gap + operator eligibility",
    "entity resolver aliases (separate overlay; do not mutate Brand overlay)",
    "longitudinal grain later as OPERATOR_LONGITUDINAL_COHORT_V1",
    "authorization already has OPERATOR subject type",
  ],
  OPERATOR_SPECIFIC: [
    "9-operator universe + CANONICAL + MONITORED_SCOPE",
    "operator decision scenario registry",
    "operator prompt library",
    "operator commercial eligibility",
    "operator truth dimensions",
    "operator association research taxonomy",
    "operator page / API namespace / entitlements",
  ],
  DO_NOT_REUSE: [
    "Brand recommendation-signal metrics (blocked for Operator V1)",
    "Brand peer set v2/v3",
    "Brand Executive finding selection (frozen except genuine defects)",
    "Brand UI / Executive cards",
    "Hotel Property Census",
    "DataForSEO observed demand",
    "Owner / Hotel & Market AI",
    "per-operator × provider execution matrix",
  ],
});
