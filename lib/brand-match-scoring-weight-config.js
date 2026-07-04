/**
 * Brand match scoring weights — single source of truth for Match Score New (api/match-score-server.js).
 * Legacy 19-factor WEIGHTS (Brand Development Dashboard) remain in match-score-server until migrated.
 */

/** Match Score New factor weights (%). Sum = 100. */
export const BRAND_MATCH_NEW_WEIGHTS = {
  chainScaleProximity: 10,
  serviceModelAlignment: 5,
  preferredBrand: 8,
  projectTypeCompatibility: 10,
  buildingTypeCompatibility: 5,
  projectStageCompatibility: 5,
  brandStandardsCompatibility: 10,
  agreementsTypeCompatibility: 10,
  roomRangeFitCompatibility: 10,
  keyMoneyWillingnessCompatibility: 12,
  incentivesMatchCompatibility: 5,
  feesToleranceCompatibility: 10,
};

/** @type {ReadonlyArray<{ key: keyof typeof BRAND_MATCH_NEW_WEIGHTS, label: string, dealSignals: string[], brandSignals: string[], notes: string }>} */
export const BRAND_MATCH_NEW_FACTOR_DEFINITIONS = [
  {
    key: "chainScaleProximity",
    label: "Chain scale proximity",
    dealSignals: ["Hotel Chain Scale"],
    brandSignals: ["Chain Scale", "Brand Basics chain scale"],
    notes: "Compares deal chain scale band to brand target scale.",
  },
  {
    key: "serviceModelAlignment",
    label: "Service model alignment",
    dealSignals: ["Hotel Service Model", "Preferred Future Operating Model"],
    brandSignals: ["Service model", "Operating model fit"],
    notes: "Token alignment between deal service model and brand positioning.",
  },
  {
    key: "preferredBrand",
    label: "Preferred brand",
    dealSignals: ["Preferred Brands"],
    brandSignals: ["Brand name match"],
    notes: "Boost when deal explicitly lists this brand as preferred.",
  },
  {
    key: "projectTypeCompatibility",
    label: "Project type compatibility",
    dealSignals: ["Project Type"],
    brandSignals: ["Project Fit — project types accepted"],
    notes: "Uses Project Fit linked records and deal project type.",
  },
  {
    key: "buildingTypeCompatibility",
    label: "Building type compatibility",
    dealSignals: ["Building Type"],
    brandSignals: ["Project Fit — building types"],
    notes: "Overlap between deal building type and brand ideal types.",
  },
  {
    key: "projectStageCompatibility",
    label: "Project stage compatibility",
    dealSignals: ["Stage of Development"],
    brandSignals: ["Project Fit — stages"],
    notes: "Stage-of-development fit against brand project-fit criteria.",
  },
  {
    key: "brandStandardsCompatibility",
    label: "Brand standards compatibility",
    dealSignals: ["Must-Haves From Brand/Operator", "Hotel Service Model"],
    brandSignals: ["Brand standards", "Amenity requirements"],
    notes: "Qualitative standards overlap (amenities, positioning).",
  },
  {
    key: "agreementsTypeCompatibility",
    label: "Agreements type compatibility",
    dealSignals: ["Preferred Deal Structure"],
    brandSignals: ["Agreement types offered"],
    notes: "Franchise vs management vs soft-brand structure fit.",
  },
  {
    key: "roomRangeFitCompatibility",
    label: "Room range fit",
    dealSignals: ["Total Number of Rooms", "Room keys"],
    brandSignals: ["Project Fit min/max room count"],
    notes: "Deal room count within brand ideal project size band.",
  },
  {
    key: "keyMoneyWillingnessCompatibility",
    label: "Key money willingness",
    dealSignals: ["Key money expectations", "Filter out brands without key money"],
    brandSignals: ["Key money willingness", "Contact uploads filters"],
    notes: "Highest single factor weight (12%) — key money alignment.",
  },
  {
    key: "incentivesMatchCompatibility",
    label: "Incentives match",
    dealSignals: ["Incentive priorities"],
    brandSignals: ["Incentive programs"],
    notes: "Overlap on incentive types (INC1 legacy factor lineage).",
  },
  {
    key: "feesToleranceCompatibility",
    label: "Fees tolerance",
    dealSignals: ["Royalty / marketing / loyalty fee expectations"],
    brandSignals: ["Typical fee structure"],
    notes: "Deal fee tolerance vs brand fee norms.",
  },
];

/** @returns {{ total: number, factorCount: number }} */
export function getBrandMatchNewWeightSummary() {
  const values = Object.values(BRAND_MATCH_NEW_WEIGHTS);
  return {
    total: values.reduce((sum, weight) => sum + weight, 0),
    factorCount: values.length,
  };
}

/** How Match Score New total is computed in computeMatchScoreNew. */
export const BRAND_MATCH_NEW_AGGREGATION = {
  method: "weighted_average",
  nullFactorHandling: "weight_stays_in_denominator",
  description:
    "All 12 factor weights always count in the denominator. Factors with null scores contribute 0 to the numerator but still reduce the total — differs from operator match scoring.",
};
