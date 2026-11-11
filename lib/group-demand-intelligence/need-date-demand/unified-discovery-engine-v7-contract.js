/**
 * GDI Unified Discovery Engine V7 — contract.
 *
 * Unifies Apify Discovery V1 + Exa + V6 adaptive waves into PowerShell #15.
 * Not a new demand segment or parallel opportunity database.
 */

export const UNIFIED_DISCOVERY_ENGINE_V7 = "gdi-unified-discovery-engine-v7";

/** Research execution strategies (not demand segments). */
export const V7_PROVIDER_STRATEGY = Object.freeze({
  EXA_ONLY: "EXA_ONLY",
  APIFY_ONLY_DISCOVERY: "APIFY_ONLY_DISCOVERY",
  APIFY_PLUS_EXA: "APIFY_PLUS_EXA",
  ADAPTIVE_MULTI_PROVIDER: "ADAPTIVE_MULTI_PROVIDER",
});

export const V7_PROVIDER_STRATEGY_META = Object.freeze({
  [V7_PROVIDER_STRATEGY.EXA_ONLY]: {
    usesApify: false,
    usesExa: true,
    note: "Track A control — Exa discovery + Deep Research / ORS only",
  },
  [V7_PROVIDER_STRATEGY.APIFY_ONLY_DISCOVERY]: {
    usesApify: true,
    usesExa: false,
    note: "Apify structured discovery only; candidates still require ORS/Exa later for qualification (handoff marked pending)",
  },
  [V7_PROVIDER_STRATEGY.APIFY_PLUS_EXA]: {
    usesApify: true,
    usesExa: true,
    note: "Track B — Apify differentiated discovery + Exa/ORS qualify at same depth as control",
  },
  [V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER]: {
    usesApify: true,
    usesExa: true,
    adaptive: true,
    note: "Per-objective provider selection via adaptive ledger; avoid duplicate searches",
  },
});

export const V7_PIPELINE = Object.freeze([
  "HOTEL_COMMERCIAL_PRIORITIES",
  "NEED_PERIODS",
  "EXISTING_GDI_INVENTORY",
  "ALL_14_STRATEGY_EVALUATION",
  "SOURCE_PROVIDER_SELECTION",
  "APIFY_PLUS_EXA_DISCOVERY",
  "SHARED_CANDIDATE_POOL",
  "CROSS_PROVIDER_DEDUP",
  "COMMERCIAL_PRE_SCREEN",
  "ADAPTIVE_EXA_ORS_DEEP_RESEARCH",
  "BUYER_TIMING_HOTEL_FIT",
  "COMMERCIAL_ACCESSIBILITY",
  "CANONICAL_CIB",
  "GOLD_DRAWER_V2",
  "LOCAL_STAGING",
  "FOUNDER_PUBLICATION_CHECKPOINT",
]);

export const WATERSTONE_V7_HOTEL = Object.freeze({
  name: "Waterstone Resort & Marina Boca Raton",
  hotelId: "recgMYovrrZDJMqzX",
  airtableBaseId: "appa2cE7FTRmIbB32",
  airtableBaseName: "AI Demand Positioning",
  needMonths: ["2027-08", "2027-09", "2027-10"],
  needPeriods: [
    { id: "2027-08", startDate: "2027-08-01", endDate: "2027-08-31" },
    { id: "2027-09", startDate: "2027-09-01", endDate: "2027-09-30" },
    { id: "2027-10", startDate: "2027-10-01", endDate: "2027-10-31" },
  ],
  needNote: "Founder-defined TEST_SCENARIO Hotel Commercial Priorities — not hotel-confirmed occupancy gaps",
  priorCampaign: {
    existingOpportunities: 45,
    exaSpendUsd: 6.9,
    competitorAssociatedStaged: 3,
    independentlyVerifiedDirectAddressable: 0,
    weddingResearch: "NOT_EXECUTED",
    watchContinuation: ["AES", "WIO", "URA"],
  },
});

export const V7_CERT_GATE = Object.freeze({
  POWER_SHELL_15_APIFY_INTEGRATION: "POWER_SHELL_15_APIFY_INTEGRATION",
  ALL_14_PROVIDER_ROUTING: "ALL_14_PROVIDER_ROUTING",
  APIFY_ACTOR_CONTRACT: "APIFY_ACTOR_CONTRACT",
  EXA_PROVIDER_NON_REGRESSION: "EXA_PROVIDER_NON_REGRESSION",
  CROSS_PROVIDER_IDENTITY: "CROSS_PROVIDER_IDENTITY",
  CROSS_PROVIDER_DEDUP: "CROSS_PROVIDER_DEDUP",
  NEED_DATE_EVIDENCE: "NEED_DATE_EVIDENCE",
  COMMERCIAL_ACCESSIBILITY: "COMMERCIAL_ACCESSIBILITY",
  BUYER_RELEVANCE: "BUYER_RELEVANCE",
  RESEARCH_DEPTH_PARITY: "RESEARCH_DEPTH_PARITY",
  V6_ADAPTIVE_WAVES: "V6_ADAPTIVE_WAVES",
  PROVIDER_BUDGET_ISOLATION: "PROVIDER_BUDGET_ISOLATION",
  NO_LIVE_CALLS_WITHOUT_AUTHORIZATION: "NO_LIVE_CALLS_WITHOUT_AUTHORIZATION",
  CIB_COMPLETENESS: "CIB_COMPLETENESS",
  GOLD_DRAWER_COMPATIBILITY: "GOLD_DRAWER_COMPATIBILITY",
  PUBLICATION_BLOCK: "PUBLICATION_BLOCK",
  ACTUAL_EXECUTION_READINESS: "ACTUAL_EXECUTION_READINESS",
});

/** Budget reconciliation constants (planning — not live quotes). */
export const V7_BUDGET_RECONCILIATION = Object.freeze({
  historicalV4SpentUsd: 6.9,
  historicalV4LeftoverShapedUsd: 11.1,
  note:
    "Historical $6.90 and leftover-shaped $11.10 are NOT reusable authorization. New campaigns require new founder GO.",
  apifyV1: {
    leanCombinedUsd: 18.98,
    fullTrackBCombinedUsd: 33.78,
    exaOnlyControlUsd: 28,
    apifyPortionFullUsd: 1.78,
    exaPortionFullUsd: 32,
  },
  v6: {
    focusedUsd: 11.1,
    commercialDepthUsd: 36,
    comprehensiveUsd: 54,
  },
  reconciliation: {
    apifyFull33_overlapsV6CommercialDepth36:
      "APIFY_PILOT_FULL (~$34) ≈ V6 COMMERCIAL_DEPTH ($36) for Track B depth — overlapping planning envelopes, not additive.",
    exaOnly28_plus_trackB34_is_comparative:
      "OPTION C Full Comparative = Track A (~$28 Exa) + Track B (~$34 Apify+Exa) ≈ $62 aggregate if both run live with comparable depth.",
    doNotDoubleCount:
      "Do not add $34 + $36. Do not treat $6.90 prior spend or $11.10 leftover as prepaid credit.",
  },
});
