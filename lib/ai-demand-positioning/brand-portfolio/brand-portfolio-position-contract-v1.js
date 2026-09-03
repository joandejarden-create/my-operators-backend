/**
 * ADP Brand & Portfolio Position — product + measurement contract (DESIGN).
 * Separate analytical system from Core NEUTRAL_DEMAND.
 * Not customer-published; does not alter Core ADP metrics.
 */

export const BRAND_PORTFOLIO_SECTION_ID = "brand-portfolio-position";
export const BRAND_PORTFOLIO_CONTRACT_VERSION = "ADP_BRAND_PORTFOLIO_POSITION_CONTRACT_V1";

export const SCENARIO_CLASS_BRAND_PORTFOLIO_DEMAND = "BRAND_PORTFOLIO_DEMAND";

/** Core vs portfolio separation — hard rule. */
export const MEASUREMENT_SYSTEMS = Object.freeze({
  CORE_ADP: Object.freeze({
    systemId: "CORE_NEUTRAL_DEMAND",
    eligibleScenarioClass: "NEUTRAL_DEMAND",
    question:
      "Does this hotel earn AI consideration from an unconstrained traveler need?",
  }),
  BRAND_PORTFOLIO: Object.freeze({
    systemId: "BRAND_PORTFOLIO_DEMAND",
    eligibleScenarioClass: SCENARIO_CLASS_BRAND_PORTFOLIO_DEMAND,
    question:
      "Once the traveler has constrained the choice to this hotel’s actual affiliation ecosystem, how well does this property compete within that universe?",
  }),
});

export const SECTION_COPY = Object.freeze({
  title: "Brand & Portfolio Position",
  subtitle:
    "How this hotel performs when AI considers its brand, collection, portfolio, or loyalty ecosystem.",
  placementAfter: "provider-presence",
  placementBefore: "ai-competitive-set",
  recommendedSequence: [
    "executive-read",
    "property-snapshot / core AI consideration",
    "ai-presence-by-demand-territory",
    "provider-presence",
    "brand-portfolio-position",
    "ai-competitive-set (Competitive Overview)",
    "competitive-context-priority-actions",
    "trends / reality / sources as governed",
  ],
});

export const PORTFOLIO_TYPES = Object.freeze({
  HARD_BRAND_PORTFOLIO: "HARD_BRAND_PORTFOLIO",
  COLLECTION_PORTFOLIO: "COLLECTION_PORTFOLIO",
  LOYALTY_ECOSYSTEM: "LOYALTY_ECOSYSTEM",
  INDEPENDENT_POSITIONING: "INDEPENDENT_POSITIONING",
});

export const PORTFOLIO_LENS_STATUS = Object.freeze({
  DEFAULT: "DEFAULT",
  OPTIONAL_FUTURE: "OPTIONAL_FUTURE",
  SUPPRESSED: "SUPPRESSED",
  METHODOLOGY_PENDING: "METHODOLOGY_PENDING",
});

/**
 * Familiar KPI cards — same metric families as Core, different eligible universe.
 * Presence Index formula must be separately versioned (do not silently reuse Core Index).
 */
export const PORTFOLIO_KPI_CARDS = Object.freeze([
  {
    kpiId: "brand_portfolio_ai_presence",
    label: "Brand / Portfolio AI Presence",
    mirrors: "AI Presence / Consideration (observation or scenario grain — governed)",
    showNumeratorDenominator: true,
  },
  {
    kpiId: "portfolio_rank",
    label: "Portfolio Rank",
    mirrors: "Competitive Overview subject rank",
    displayExample: "#3 of 12",
  },
  {
    kpiId: "portfolio_benchmark",
    label: "Portfolio Benchmark",
    mirrors: "CORE Benchmark AI Presence",
    status: "FORMULA_CANDIDATE",
  },
  {
    kpiId: "portfolio_presence_index",
    label: "Portfolio Presence Index",
    mirrors: "AI Presence Index",
    formulaCandidate:
      "subject_portfolio_presence_rate / portfolio_benchmark_presence_rate × 100",
    versionLabel: "PORTFOLIO_PRESENCE_INDEX_V1_CANDIDATE",
    doNotSilentlyReuseCoreFormula: true,
  },
  {
    kpiId: "portfolio_number_one_appearance",
    label: "#1 Appearance",
    mirrors: "Core #1 Appearance",
    placement: "KPI_CARD",
  },
  {
    kpiId: "portfolio_top3_appearance",
    label: "Top-3 Appearance",
    mirrors: "Core Top-3 Appearance",
    placement: "KPI_CARD",
  },
]);

/** Competitive Overview column grammar — reuse exactly. */
export const PORTFOLIO_RANKING_TABLE_COLUMNS = Object.freeze([
  { id: "rank", label: "Rank", required: true },
  { id: "hotel", label: "Hotel", required: true },
  { id: "aiPresence", label: "AI Presence", required: true },
  { id: "deltaVsPrior", label: "Δ vs Prior Run", required: true },
  { id: "displacementVsYou", label: "Displacement vs You", required: true },
  { id: "scenariosShared", label: "Scenarios Shared", required: true },
]);

export const PORTFOLIO_TABLE_OMIT_FROM_PRIMARY = Object.freeze([
  "topDemandTerritory", // keep via territory filter, not extra column — reduce width vs Core Comp Overview
]);

export const TERRITORY_FILTER_REUSE = Object.freeze({
  source: "Existing Competitive Overview / Demand Territory taxonomy",
  values: [
    "Overall",
    "Business",
    "Leisure",
    "Couples",
    "Family",
    "Group / Meetings",
    "Wellness",
    "Adventure",
    "Celebrations",
  ],
  rule: "Do not invent a second territory taxonomy.",
});

export const TERRITORY_SUMMARY_TABLE_RECOMMENDATION = Object.freeze({
  recommendation: "DEFER_COMPACT_TERRITORY_SUMMARY",
  rationale:
    "A second territory table duplicates Demand Territories + the filtered ranking table. Prefer one territory dropdown on the portfolio ranking table (same Comp Overview pattern). Revisit only if owners need multi-territory scan without filter clicks.",
});

export const SAME_METRIC_DIFFERENT_UNIVERSE = Object.freeze({
  principle: "Preserve metric definition; change eligible scenario universe and peer universe only.",
  examples: {
    aiPresence: "share of eligible scenarios/observations where hotel appears",
    rank: "rank within selected governed competitor universe",
    displacement: "unique eligible scenarios where subject absent and peer present",
    scenariosShared: "unique eligible scenarios where subject and peer both appear",
  },
});

export const ASSURANCE_GATES = Object.freeze({
  BRAND_PORTFOLIO_PROMPT_ELIGIBILITY_INTEGRITY: "BRAND_PORTFOLIO_PROMPT_ELIGIBILITY_INTEGRITY",
  PORTFOLIO_AFFILIATION_INTEGRITY: "PORTFOLIO_AFFILIATION_INTEGRITY",
  PORTFOLIO_PEER_SET_INTEGRITY: "PORTFOLIO_PEER_SET_INTEGRITY",
  PORTFOLIO_METRIC_INTEGRITY: "PORTFOLIO_METRIC_INTEGRITY",
  PORTFOLIO_RANKING_INTEGRITY: "PORTFOLIO_RANKING_INTEGRITY",
  PORTFOLIO_DISPLACEMENT_INTEGRITY: "PORTFOLIO_DISPLACEMENT_INTEGRITY",
  PORTFOLIO_EVIDENCE_INTEGRITY: "PORTFOLIO_EVIDENCE_INTEGRITY",
  PORTFOLIO_AFFILIATION_SNAPSHOT_INTEGRITY: "PORTFOLIO_AFFILIATION_SNAPSHOT_INTEGRITY",
  BRAND_PORTFOLIO_CROSS_SURFACE_COHERENCE: "BRAND_PORTFOLIO_CROSS_SURFACE_COHERENCE",
  PROPERTY_PROFILE_AFFILIATION_INTEGRITY: "PROPERTY_PROFILE_AFFILIATION_INTEGRITY",
});

export const HISTORICAL_SNAPSHOT_REQUIREMENTS = Object.freeze([
  "portfolioType",
  "affiliationSnapshot",
  "portfolioLensId",
  "peerSetId",
  "peerSetVersion",
  "promptUniverse / promptManifestHash",
  "kpiValues",
  "portfolioBenchmark",
  "portfolioPresenceIndex",
  "fullRankingUniverse",
  "territoryRankings",
  "displacement",
  "sharedScenarios",
  "evidenceRefs",
  "versionMetadata",
]);

export const AIRTABLE_HISTORY_EXTENSION = Object.freeze({
  approach: "EXTEND_COMPETITIVE_RANKINGS_WITH_rankingUniverseType",
  rankingUniverseTypes: ["MARKET_CORE", "BRAND_PORTFOLIO"],
  additionalIndexedFields: [
    "portfolioLens",
    "portfolioType",
    "peerSetVersion",
    "scenarioClass",
    "portfolioBenchmark",
    "portfolioIndex",
  ],
  writesEnabled: false,
});

export const PROMPT_LINEAGE = Object.freeze({
  chain:
    "PROPERTY AFFILIATION → PORTFOLIO LENS → SCENARIO → EXACT PROMPT → RESPONSE → PORTFOLIO METRIC",
  scenarioClass: SCENARIO_CLASS_BRAND_PORTFOLIO_DEMAND,
  defaultDualEligibilityWithCore: false,
});

export const ACTIONS_POLICY = Object.freeze({
  allowedVerbs: ["Review", "Investigate", "Compare", "Validate", "Strengthen Representation"],
  prohibited: "Unsupported causal uplift claims (Fix X → presence +Y%)",
});

export const VISUAL_REUSE = Object.freeze({
  reuse: [
    "aiv-theme-group / aiv-theme-title / aiv-theme-help",
    "aiv-kpi-row KPI cards",
    "deals-table aiv-portfolio-table adp-comp-table-fixed",
    "sort indicators, info tooltips, rank grammar",
    "territory filter dropdown pattern from Competitive Overview",
    "evidence drawer / Missing + Positive Evidence patterns",
    "PEER_TEXT_VISUAL_BALANCE and related stress gates",
  ],
  doNotIntroduce: "New decorative design system or mini-product chrome",
});
