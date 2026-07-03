/**
 * Operator match scoring weights — single source of truth for scoreOperatorMatchForDeal.
 * Consumed by api/my-deals.js and admin scoring-weight runbook (task 2.02).
 *
 * Do not duplicate these values in UI code or other modules.
 */

/** @readonly Pilot operator-alignment factor weights (positive factors + penalty). */
export const OPERATOR_MATCH_WEIGHTS = {
  geographyMarkets: 18,
  chainScale: 8,
  assetProjectStageFit: 14,
  dealStructureAssignment: 12,
  feeCommercial: 10,
  serviceOfferings: 8,
  systemsReporting: 6,
  ownerRelations: 6,
  brandPortfolioRelevance: 6,
  negativeFitPenalty: 2,
};

/**
 * Human-readable factor definitions aligned with docs/operator-alignment-field-matrix.md.
 * @type {ReadonlyArray<{
 *   key: keyof typeof OPERATOR_MATCH_WEIGHTS,
 *   label: string,
 *   engineRef: string,
 *   dealFields: string[],
 *   operatorFields: string[],
 *   mvpQuality: 'strong' | 'partial' | 'weak',
 *   notes: string,
 * }>}
 */
export const OPERATOR_MATCH_FACTOR_DEFINITIONS = [
  {
    key: "geographyMarkets",
    label: "Geography & Markets",
    engineRef: "scoreGeographyFactor",
    dealFields: ["Country", "City", "Market Presence Requirement", "Primary Market Region"],
    operatorFields: ["activeCountries", "activeMarkets", "specificMarkets", "regionsSupported"],
    mvpQuality: "partial",
    notes: "Country/city/market tier match via scoreGeographyFactor. Missing deal country → 60 if operator markets exist, else excluded. Missing operator geography → excluded (null), not a default low score.",
  },
  {
    key: "chainScale",
    label: "Chain Scale",
    engineRef: "inline (scoreOperatorMatchForDeal)",
    dealFields: ["Hotel Chain Scale"],
    operatorFields: ["chainScalesSupported", "chainScale", "chainScalesYouSupport"],
    mvpQuality: "strong",
    notes: "Exact match → 100; partial substring → 65; else 25. Empty operator scale → factor excluded (null).",
  },
  {
    key: "assetProjectStageFit",
    label: "Asset / Project / Stage Fit",
    engineRef: "scoreAssetStageFactor",
    dealFields: ["Project Type", "Building Type", "Stage of Development"],
    operatorFields: ["bestFitAssetTypes", "propertyTypes", "operatingSituations", "bf_selected_situation_types"],
    mvpQuality: "partial",
    notes: "Blends project/building overlap (~70%) with stage/situation overlap (~30%).",
  },
  {
    key: "dealStructureAssignment",
    label: "Deal Structure / Assignment",
    engineRef: "scoreDealStructureFactor",
    dealFields: [
      "Preferred Management Structure",
      "Brand Agreement Structure",
      "Operating Model",
      "Operator Scope",
      "Preferred Deal Structure (legacy MP)",
    ],
    operatorFields: ["managementStructuresSupported", "bestFitDealStructures", "serviceModels"],
    mvpQuality: "partial",
    notes: "Structured SI path: management structure + operating model vs operator structures (exact → 100, partial → 72). Legacy MP-only fallback when structured fields absent.",
  },
  {
    key: "feeCommercial",
    label: "Fee / Commercial",
    engineRef: "inline (scoreOperatorMatchForDeal)",
    dealFields: [
      "Royalty Fee Expectations",
      "Marketing Fee Expectations",
      "Loyalty Fee Expectations",
    ],
    operatorFields: ["feeStructureSummary", "ov_card_commercial", "bf_signal_capital"],
    mvpQuality: "weak",
    notes: "Placeholder 75 when both deal fee expectations and operator commercial text exist — treat as data gap in owner copy.",
  },
  {
    key: "serviceOfferings",
    label: "Service Offerings",
    engineRef: "scoreServiceOfferingsFactor",
    dealFields: ["Services Required From Operator", "Must-Haves From Brand/Operator", "Operator Capability Priorities"],
    operatorFields: ["primaryServices", "additionalServices", "revenueManagementServices", "salesMarketingSupport"],
    mvpQuality: "partial",
    notes: "Token overlap between deal must-haves/services and operator capability arrays.",
  },
  {
    key: "systemsReporting",
    label: "Systems & Reporting",
    engineRef: "scoreSystemsReportingFactor",
    dealFields: ["Owner Reporting Frequency", "Owner Reporting Package", "Preferred Reporting Frequency"],
    operatorFields: ["technologySystems", "ownerReportingCadence", "reportTypesProvided", "infra_kpi_reporting"],
    mvpQuality: "partial",
    notes: "Scores operator systems/reporting presence (40–90); cadence match is not fully symmetric today.",
  },
  {
    key: "ownerRelations",
    label: "Owner Relations",
    engineRef: "inline (scoreOperatorMatchForDeal)",
    dealFields: ["Level of Involvement in Day-to-Day Ops", "Owner Control Priorities"],
    operatorFields: ["ownerEngagementNarrative", "operatingCollaborationMode", "ownerCommunicationStyle"],
    mvpQuality: "weak",
    notes: "Keyword match on operator owner-relations narrative; deal side uses control preference when present.",
  },
  {
    key: "brandPortfolioRelevance",
    label: "Brand / Portfolio Relevance",
    engineRef: "overlapScore (preferred brands vs operator brands)",
    dealFields: ["Preferred Brands"],
    operatorFields: ["brands", "brand_narrative_compliance"],
    mvpQuality: "partial",
    notes: "Overlap between owner preferred brands and operator managed brand portfolio.",
  },
  {
    key: "negativeFitPenalty",
    label: "Negative-Fit Penalty",
    engineRef: "inline (scoreOperatorMatchForDeal)",
    dealFields: ["Top 3 Deal Breakers"],
    operatorFields: ["lessIdealSituations", "less_proven_areas"],
    mvpQuality: "partial",
    notes:
      "Weighted factor (weight 2), not a flat subtraction. Score 20 when a deal breaker substring-matches operator less-ideal situations; else 100. Included in weighted average when deal breakers and operator less-ideal text exist.",
  },
];

/**
 * Operator match UI bands — used in My Deals / Operator Strategy (`match-score-*` classes).
 * Thresholds: public/js/operator-strategy-my-deals.js, public/my-deals.html.
 */
export const OPERATOR_MATCH_SCORE_BANDS = [
  { min: 80, label: "Strong alignment signals", uiClass: "match-score-high" },
  { min: 50, label: "Moderate alignment — review gaps", uiClass: "match-score-medium" },
  { min: 25, label: "Weak alignment — significant gaps", uiClass: "match-score-weak" },
  { min: 0, label: "Very limited alignment", uiClass: "match-score-poor" },
];

/** How operator total score is computed in scoreOperatorMatchForDeal. */
export const OPERATOR_MATCH_AGGREGATION = {
  method: "weighted_average",
  nullFactorHandling: "exclude_from_denominator",
  description:
    "Sum(score × weight) / sum(weight) for factors with non-null scores only. negativeFitPenalty participates like other factors when scored.",
};

/** @returns {{ scoredFactorSum: number, penaltyWeight: number, documentedTotal: number, factorCount: number }} */
export function getOperatorMatchWeightSummary() {
  const entries = Object.entries(OPERATOR_MATCH_WEIGHTS);
  const scored = entries.filter(([key]) => key !== "negativeFitPenalty");
  const scoredFactorSum = scored.reduce((sum, [, weight]) => sum + weight, 0);
  return {
    scoredFactorSum,
    penaltyWeight: OPERATOR_MATCH_WEIGHTS.negativeFitPenalty,
    documentedTotal: scoredFactorSum + OPERATOR_MATCH_WEIGHTS.negativeFitPenalty,
    factorCount: scored.length,
  };
}
