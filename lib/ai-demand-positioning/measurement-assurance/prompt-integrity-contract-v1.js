/**
 * ADP_PROMPT_INTEGRITY_CONTRACT_V1
 *
 * Exact rendered prompt for measurement = scenario.query today
 * (multi-provider-runner: callProvider(provider, scenario.query)).
 * Observations currently store scenarioId only — forensic prompt must be
 * reconstructed from scenario registry OR (going forward) stored verbatim.
 */

export const ADP_PROMPT_INTEGRITY_CONTRACT_VERSION = "ADP_PROMPT_INTEGRITY_CONTRACT_V1";

export const SCENARIO_CLASSES = Object.freeze({
  NEUTRAL_DEMAND: "NEUTRAL_DEMAND",
  PROPERTY_SPECIFIC: "PROPERTY_SPECIFIC",
  BRAND_SPECIFIC: "BRAND_SPECIFIC",
  /** Product analytical class for Brand & Portfolio Position (not Core). */
  BRAND_PORTFOLIO_DEMAND: "BRAND_PORTFOLIO_DEMAND",
  COMPETITOR_SPECIFIC: "COMPETITOR_SPECIFIC",
  OTHER_GOVERNED_SPECIAL: "OTHER_GOVERNED_SPECIAL",
});

/**
 * Which scenario classes may enter core neutral-demand KPIs.
 * FOUNDER LOCKED 2026-08-21: NEUTRAL_DEMAND only.
 * Activation of live metric filters requires ADP_MEASUREMENT_CONTRACT_V1_1 publish path.
 */
export const CORE_KPI_ELIGIBLE_CLASSES = Object.freeze([SCENARIO_CLASSES.NEUTRAL_DEMAND]);
export const FOUNDER_CORE_ELIGIBILITY_LOCK = Object.freeze({
  lockedAt: "2026-08-21",
  rule: "Only NEUTRAL_DEMAND may contribute to core Existing Hotel ADP measurement KPIs.",
  excluded: [
    SCENARIO_CLASSES.PROPERTY_SPECIFIC,
    SCENARIO_CLASSES.BRAND_SPECIFIC,
    SCENARIO_CLASSES.COMPETITOR_SPECIFIC,
    SCENARIO_CLASSES.OTHER_GOVERNED_SPECIAL,
  ],
  specialIntelligenceRetention: true,
});

export const BIAS_CLASSIFICATIONS = Object.freeze({
  PASS_NEUTRAL: "PASS_NEUTRAL",
  GOVERNED_LEGITIMATE_BRAND_SPECIFIC: "GOVERNED_LEGITIMATE_BRAND_SPECIFIC",
  GOVERNED_LEGITIMATE_PROPERTY_SPECIFIC: "GOVERNED_LEGITIMATE_PROPERTY_SPECIFIC",
  UNINTENDED_BRAND_BIAS: "UNINTENDED_BRAND_BIAS",
  SUBJECT_NAME_LEAKAGE: "SUBJECT_NAME_LEAKAGE",
  COMPETITOR_PROMPT_LEAKAGE: "COMPETITOR_PROMPT_LEAKAGE",
  CROSS_PROPERTY_CONTAMINATION: "CROSS_PROPERTY_CONTAMINATION",
  GEOGRAPHY_MISMATCH: "GEOGRAPHY_MISMATCH",
  SEMANTIC_SCENARIO_DRIFT: "PROMPT_SCENARIO_SEMANTIC_DRIFT",
  UNRESOLVED_VARIABLE: "UNRESOLVED_VARIABLE",
  PROFILE_AFFILIATION_CONTAMINATION: "PROFILE_AFFILIATION_CONTAMINATION",
  AMBIGUOUS_REVIEW_REQUIRED: "AMBIGUOUS_REVIEW_REQUIRED",
});

export const MATERIALITY = Object.freeze({
  NON_MATERIAL: "NON_MATERIAL",
  MATERIAL_BUT_RECOVERABLE: "MATERIAL_BUT_RECOVERABLE",
  MATERIAL_REQUIRES_PERIOD_REPROCESS: "MATERIAL_REQUIRES_PERIOD_REPROCESS",
  METHODOLOGY_REVIEW_REQUIRED: "METHODOLOGY_REVIEW_REQUIRED",
});

export const CASE_TYPES = Object.freeze({
  A_GOVERNED_LEGITIMATE_BRAND_SPECIFIC: "A_GOVERNED_LEGITIMATE_BRAND_SPECIFIC",
  B_UNINTENDED_BRAND_BIAS: "B_UNINTENDED_BRAND_BIAS",
  C_CROSS_PROPERTY_TEMPLATE_CONTAMINATION: "C_CROSS_PROPERTY_TEMPLATE_CONTAMINATION",
  D_AMBIGUOUS_REVIEW_REQUIRED: "D_AMBIGUOUS_REVIEW_REQUIRED",
});

export const GATES = Object.freeze({
  PROMPT_NEUTRALITY_INTEGRITY: "PROMPT_NEUTRALITY_INTEGRITY",
  PROMPT_SCENARIO_SEMANTIC_INTEGRITY: "PROMPT_SCENARIO_SEMANTIC_INTEGRITY",
  PROMPT_PROPERTY_ISOLATION_INTEGRITY: "PROMPT_PROPERTY_ISOLATION_INTEGRITY",
  PROMPT_MARKET_GEOGRAPHY_INTEGRITY: "PROMPT_MARKET_GEOGRAPHY_INTEGRITY",
  MARKET_TO_PROMPT_PROVENANCE: "MARKET_TO_PROMPT_PROVENANCE",
  NEUTRAL_REPLACEMENT_SEMANTIC_EQUIVALENCE: "NEUTRAL_REPLACEMENT_SEMANTIC_EQUIVALENCE",
  NEUTRAL_RESPONSE_REUSE_EXACT_PROMPT_IDENTITY: "NEUTRAL_RESPONSE_REUSE_EXACT_PROMPT_IDENTITY",
  PROMPT_MANIFEST_CERTIFIED_BEFORE_EXECUTION: "PROMPT_MANIFEST_CERTIFIED_BEFORE_EXECUTION",
  EXECUTED_PROMPT_MATCHES_CERTIFIED_MANIFEST: "EXECUTED_PROMPT_MATCHES_CERTIFIED_MANIFEST",
  PROMPT_LEDGER_SNAPSHOT_RECONCILIATION: "PROMPT_LEDGER_SNAPSHOT_RECONCILIATION",
  METRIC_TO_PROMPT_TRACEABILITY: "METRIC_TO_PROMPT_TRACEABILITY",
  PROMPT_PROVENANCE_COMPLETENESS: "PROMPT_PROVENANCE_COMPLETENESS",
  CORRECTED_SCENARIO_SINGLE_MEASUREMENT_SLOT: "CORRECTED_SCENARIO_SINGLE_MEASUREMENT_SLOT",
  NEUTRAL_DEMAND_DENOMINATOR_RECONCILIATION: "NEUTRAL_DEMAND_DENOMINATOR_RECONCILIATION",
  PROMPT_INTEGRITY_READY_FOR_NEXT_PERIOD: "PROMPT_INTEGRITY_READY_FOR_NEXT_PERIOD",
});

export const DEFECT_CLASSES = Object.freeze({
  UNINTENDED_BRAND_BIASED_PROMPT: "UNINTENDED_BRAND_BIASED_PROMPT",
  SUBJECT_NAME_PROMPT_LEAKAGE: "SUBJECT_NAME_PROMPT_LEAKAGE",
  COMPETITOR_PROMPT_LEAKAGE: "COMPETITOR_PROMPT_LEAKAGE",
  CROSS_PROPERTY_PROMPT_CONTAMINATION: "CROSS_PROPERTY_PROMPT_CONTAMINATION",
  PROMPT_SCENARIO_SEMANTIC_DRIFT: "PROMPT_SCENARIO_SEMANTIC_DRIFT",
  UNTRACEABLE_EXECUTED_PROMPT: "UNTRACEABLE_EXECUTED_PROMPT",
});

/** Major chain / loyalty tokens for NEUTRAL_DEMAND preflight (token-boundary). */
export const MAJOR_BRAND_TOKENS = Object.freeze([
  "hyatt",
  "world of hyatt",
  "marriott",
  "bonvoy",
  "hilton",
  "hilton honors",
  "honors points",
  "ihg",
  "one rewards",
  "accor",
  "all - inclusive by accor",
  "wyndham",
  "choice hotels",
  "four seasons",
  "rosewood",
  "radisson",
  "best western",
  "intercontinental",
  "kimpton",
  "curio collection",
  "autograph collection",
  "tribute portfolio",
  "luxury collection",
  "st. regis",
  "w hotel",
  "westin",
  "sheraton",
  "renaissance hotels",
  "thompson hotel",
  "andaz",
  "park hyatt",
  "grand hyatt",
  "hyatt centric",
  "hyatt place",
  "hyatt house",
]);

export const PROMPT_NEUTRALITY_RULE = Object.freeze({
  summary:
    "Standard Existing Hotel traveler-demand prompts must not favor or require a particular hotel brand, subject property, competitor, or loyalty program unless the scenario is explicitly classified as BRAND_SPECIFIC / PROPERTY_SPECIFIC / COMPETITOR_SPECIFIC.",
  acceptableExample:
    "What are good hotels in downtown Manhattan for a couple looking for a stylish weekend stay?",
  biasedExample: "What Hyatt hotels in downtown Manhattan are good for a stylish weekend stay?",
  exactPromptAuthority:
    "scenario.query is the exact user message sent to providers (no additional template layer today).",
  storageGap:
    "Observations historically store scenarioId only; exact prompt must be snapshotted going forward.",
});

/**
 * Target eligibility matrix (does not auto-change live formulas).
 */
export const ELIGIBILITY_MATRIX = Object.freeze({
  [SCENARIO_CLASSES.NEUTRAL_DEMAND]: {
    consideration: true,
    scenarioPresence: true,
    demandCapture: true,
    providerPresence: true,
    competitiveRanking: true,
    displacement: true,
    territoryMetrics: true,
  },
  [SCENARIO_CLASSES.PROPERTY_SPECIFIC]: {
    consideration: false,
    scenarioPresence: false,
    demandCapture: false,
    providerPresence: false,
    competitiveRanking: false,
    displacement: false,
    territoryMetrics: false,
    note: "May be retained for intelligence; segregate from core neutral KPIs unless measurement contract explicitly includes.",
  },
  [SCENARIO_CLASSES.BRAND_SPECIFIC]: {
    consideration: false,
    scenarioPresence: false,
    demandCapture: false,
    providerPresence: false,
    competitiveRanking: false,
    displacement: false,
    territoryMetrics: false,
    note: "Loyalty/brand-restricted questions — segregate.",
  },
  [SCENARIO_CLASSES.COMPETITOR_SPECIFIC]: {
    consideration: false,
    scenarioPresence: false,
    demandCapture: false,
    providerPresence: false,
    competitiveRanking: false,
    displacement: false,
    territoryMetrics: false,
  },
  [SCENARIO_CLASSES.OTHER_GOVERNED_SPECIAL]: {
    consideration: false,
    scenarioPresence: false,
    demandCapture: false,
    providerPresence: false,
    competitiveRanking: false,
    displacement: false,
    territoryMetrics: false,
  },
});
