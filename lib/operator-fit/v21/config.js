/**
 * Operator Fit v2.1 — centralized differentiation config.
 * Top-level factor weights and 70/15/15 are intentionally identical to v2.
 */

import { OPERATOR_PROJECT_FACTORS, PRIMARY_LAYER_WEIGHTS } from "../config.js";
import { MARKET_PRESENCE_TYPE } from "../../operator-intelligence/market-presence.js";

export const V21_TOP_LEVEL_FACTORS = OPERATOR_PROJECT_FACTORS;
export const V21_PRIMARY_LAYER_WEIGHTS = PRIMARY_LAYER_WEIGHTS;

/** Owner-facing tie materiality (Displayed Alignment points). */
export const V21_TIE_MATERIALITY_POINTS = 1.0;

/** Max verified comparables contributing numerically to CRI. */
export const V21_CRI_MAX_COMPARABLES = 3;

/** Diminishing weights for top-1..3 comparables. */
export const V21_CRI_DIMINISHING = Object.freeze([1.0, 0.6, 0.35]);

/**
 * CRI formulations (audit-selected default = moderate).
 * Blend: assetScore = (1-criWeight)*developmentModeScore + criWeight*criScore100
 */
export const V21_CRI_FORMULATIONS = Object.freeze({
  conservative: {
    id: "conservative",
    criWeightInsideAsset: 0.3,
    label: "Conservative",
  },
  moderate: {
    id: "moderate",
    criWeightInsideAsset: 0.55,
    label: "Moderate",
  },
  strong: {
    id: "strong",
    criWeightInsideAsset: 0.8,
    label: "Strong",
  },
});

export const V21_DEFAULT_CRI_FORMULATION = "moderate";

/**
 * Geography relevance subformula (selected: Balanced Depth).
 * presenceStrength 0–100 + depthBonus − currentnessPenalty, clamped.
 * Does not change Geography top-level weight (22).
 */
export const V21_GEOGRAPHY_FORMULA_ID = "balanced_depth_v1";

export const V21_PRESENCE_STRENGTH_SCORES = Object.freeze({
  [MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY]: 92,
  [MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO]: 88,
  [MARKET_PRESENCE_TYPE.REGIONAL_OFFICE_OR_TEAM]: 80,
  [MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT]: 52,
  [MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE]: 32,
  [MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST]: 16,
  [MARKET_PRESENCE_TYPE.CLAIMED_CAPABILITY]: 12,
  [MARKET_PRESENCE_TYPE.UNKNOWN]: 0,
});

/** Depth bonuses (additive, capped). */
export const V21_GEO_DEPTH = Object.freeze({
  cityMatch: 12,
  multipleCurrentProperties: 8,
  singleCurrentProperty: 4,
  regionalOfficeWithCountryOps: 3,
  countryListFallbackOnly: 0,
});

export const V21_OWNER_TIERS = Object.freeze({
  LEADING: "Leading Candidates",
  POTENTIAL: "Potential Fits — Validation Needed",
  ADDITIONAL: "Additional Candidates Requiring Validation",
  UNDER_EVALUATION: "Under Evaluation",
});
