/**
 * Brand Match Score v2 — single source of truth for weights, gates, and aggregation.
 * Engine: api/match-score-server.js (computeMatchScoreForDealBrand / computeMatchScoreNew).
 *
 * Product meaning: how well this project/opportunity matches brand characteristics and
 * owner priorities (one number). Soft factors mix exact-match and graduated scores (fees, brand standards,
 * room-range fit, service model adjacency, chain scale proximity). Missing data is excluded from the
 * denominator. Hard gates force overall score to 0.
 * Preferred brand is a +bonus after the base average (not a penalty for non-preferred).
 *
 * Commercial soft factors (Fit-led one score): graduated geography priority + same-brand
 * Open density in deal Market/Submarket (Hotel Census). Density is soft only — not a hard gate.
 */

/** Soft-factor weights (%). Sum = 100. Project stage remains breakdown-only. */
export const BRAND_MATCH_NEW_WEIGHTS = {
  geographyPriority: 14,
  sameBrandMarketDensity: 10,
  chainScaleProximity: 10,
  brandStandardsCompatibility: 10,
  feesToleranceCompatibility: 10,
  roomRangeFitCompatibility: 7,
  serviceModelAlignment: 8,
  keyMoneyWillingnessCompatibility: 9,
  softHardPreference: 6,
  incentivesMatchCompatibility: 6,
  agreementsTypeCompatibility: 5,
  buildingTypeCompatibility: 5,
};

/**
 * Geography priority — graduated growth intent (still one soft factor, not a second pillar).
 * Global/country hit = 100; region-only hit = 75; priorities exist but miss = 0.
 */
export const BRAND_MATCH_GEOGRAPHY_PRIORITY_SCORES = {
  globalOrCountryHit: 100,
  regionHit: 75,
  miss: 0,
};

/**
 * Same-brand Open density bands (Hotel Census Affiliation + confident Market/Submarket).
 * Soft watchout only — never a hard gate. Unconfident geography → factor excluded.
 */
export const BRAND_MATCH_SAME_BRAND_DENSITY = {
  mode: "peer_count_bands",
  bands: [
    { maxPeers: 0, score: 100 },
    { maxPeers: 1, score: 55 },
    { maxPeers: 2, score: 30 },
    { maxPeers: Infinity, score: 10 },
  ],
  fallbackScore: 10,
  requireConfidentGeography: true,
};

/** Preferred-list bonus applied after base soft total (cap 100). Not a soft-factor weight. */
export const BRAND_MATCH_PREFERRED_BONUS = 4;

/** Chain scale soft scores (graduated; not a hard gate). */
export const BRAND_MATCH_CHAIN_SCALE_SCORES = {
  sameTier: 100,
  oneTierApart: 50,
  twoTiersApart: 25,
  threeOrMoreTiersApart: 0,
};

/**
 * Hotel Service Model alignment — graduated adjacency (not exact-match-only).
 * Canonical options match Deal Setup / Brand Setup select lists (Airtable field
 * "Hotel Service Model"; owner-facing label: Service / Operating Model).
 * Unlisted pairs score distantMismatchScore (0). Unknown/blank → factor excluded.
 *
 * Taxonomy (see docs/ai-build-system/BUILD_DECISIONS.md): this factor is ops /
 * commercial style, not Affiliation Model (Soft/Hard/Collection).
 */
export const BRAND_MATCH_SERVICE_MODEL_GRADUATION = {
  mode: "adjacency",
  exactMatchScore: 100,
  distantMismatchScore: 0,
  canonicalOptions: [
    "Full-Service",
    "Select-Service",
    "Extended Stay",
    "Lifestyle / Boutique",
    "All-Inclusive",
  ],
  /** Lowercase normalized alias → canonical option. */
  aliases: {
    "full service": "Full-Service",
    "full-service": "Full-Service",
    "select service": "Select-Service",
    "select-service": "Select-Service",
    "focused service": "Select-Service",
    "focused-service": "Select-Service",
    "extended stay": "Extended Stay",
    "extended-stay": "Extended Stay",
    "lifestyle / boutique": "Lifestyle / Boutique",
    "lifestyle/boutique": "Lifestyle / Boutique",
    lifestyle: "Lifestyle / Boutique",
    boutique: "Lifestyle / Boutique",
    "all-inclusive": "All-Inclusive",
    "all inclusive": "All-Inclusive",
  },
  /**
   * Adjacent pair scores (order-independent).
   * Keys are `canonicalA|canonicalB` sorted localeCompare so lookup is stable.
   */
  adjacentPairScores: {
    "Full-Service|Lifestyle / Boutique": 55,
    "Full-Service|Select-Service": 40,
    "Lifestyle / Boutique|Select-Service": 50,
    "All-Inclusive|Full-Service": 25,
    "Extended Stay|Select-Service": 25,
  },
};

/**
 * Brand Standards Compatibility — graduated (not binary).
 * Amenity score = match rate of brand-required amenities present on the deal.
 * F&B / parking are soft penalties after the amenity match rate.
 */
export const BRAND_MATCH_STANDARDS_GRADUATION = {
  mode: "match_rate_then_penalties",
  /** Amenities ignored as brand "requirements" for scoring. */
  ignoreAmenityValues: ["Not Applicable / None", "Other Amenities"],
  fbRequiredMissPenalty: 20,
  fbCountShortfallPenalty: 12,
  parkingRequiredMissPenalty: 10,
};

/**
 * Fees Tolerance — graduated shortfall bands vs brand min (not binary).
 * Deal within/above brand range = 100; below brand min uses shortfall % of brand min.
 */
export const BRAND_MATCH_FEES_GRADUATION = {
  mode: "shortfall_bands",
  bands: [
    { maxShortfallPct: 0, score: 100 },
    { maxShortfallPct: 10, score: 75 },
    { maxShortfallPct: 25, score: 50 },
    { maxShortfallPct: 50, score: 25 },
    { maxShortfallPct: Infinity, score: 0 },
  ],
  /** Deal midpoint above brand max still counts as full fit (owner willing to pay more). */
  aboveBrandMaxScore: 100,
  /**
   * Owner fee answers that mean "not sure yet" — never scored as a shortfall miss.
   * Those fee types are excluded from the fees factor (factor shows — / not in average).
   */
  unknownExpectationTokens: [
    "undetermined",
    "not yet determined",
    "not specified",
    "undeterimed",
    "tbd",
    "unknown",
    "n/a",
    "na",
  ],
};

/**
 * Room range fit — graduated soft factor (not a hard gate).
 * Within brand min–max = 100; outside uses distance from nearest bound as % of that bound.
 */
export const BRAND_MATCH_ROOM_RANGE_GRADUATION = {
  mode: "distance_bands",
  bands: [
    { maxDistancePct: 0, score: 100 },
    { maxDistancePct: 10, score: 75 },
    { maxDistancePct: 25, score: 50 },
    { maxDistancePct: 50, score: 25 },
    { maxDistancePct: Infinity, score: 0 },
  ],
};

/**
 * Hard gates — any fail forces overall score to 0 (with reason).
 * Evaluated only when both sides have enough data to decide; insufficient data → gate does not fire.
 */
export const BRAND_MATCH_V2_GATES = [
  {
    key: "keyMoney",
    label: "Key money",
    failWhen: "Owner requires key money and brand does not offer Key Money / Upfront Incentive.",
  },
  {
    key: "agreementType",
    label: "Agreement type",
    failWhen: "Deal structure not in brand acceptable agreements (unless brand Flexible/Open).",
  },
  {
    key: "projectType",
    label: "Project type",
    failWhen: "Deal project type not in brand acceptable project types.",
  },
  {
    key: "geographyAvoid",
    label: "Geography (markets to avoid)",
    failWhen: "Deal market/country matches brand Markets to Avoid.",
  },
];

/** Shown in breakdown only — not in weighted soft total. */
export const BRAND_MATCH_BREAKDOWN_ONLY_FACTORS = [
  {
    key: "projectStageCompatibility",
    label: "Project stage compatibility",
    notes: "Demoted from weighted total in v2 — diligence signal only.",
  },
  {
    key: "preferredBrand",
    label: "Preferred brand (bonus)",
    notes: "Not a soft weight. +4 to base when brand is on owner Preferred list (cap 100).",
  },
];

/** @type {ReadonlyArray<{ key: keyof typeof BRAND_MATCH_NEW_WEIGHTS, label: string, dealSignals: string[], brandSignals: string[], notes: string }>} */
export const BRAND_MATCH_NEW_FACTOR_DEFINITIONS = [
  {
    key: "geographyPriority",
    label: "Geography priority",
    dealSignals: ["Country", "Primary Market Region"],
    brandSignals: ["Priority Markets", "Global / regional priority market flags"],
    notes:
      "Graduated: global/country priority hit = 100; region-only hit = 75; priorities set but miss = 0. Markets to Avoid is a hard gate.",
  },
  {
    key: "sameBrandMarketDensity",
    label: "Same-brand market density",
    dealSignals: ["Country", "Hotel Submarket & Location", "Primary Market Region"],
    brandSignals: ["Hotel Census Affiliation (Open)"],
    notes:
      "Soft only: 0 Open peers in confident Market/Submarket = 100; 1 = 55; 2 = 30; 3+ = 10. Country-only or unpopulated census geography excludes the factor.",
  },
  {
    key: "chainScaleProximity",
    label: "Chain scale proximity",
    dealSignals: ["Hotel Chain Scale"],
    brandSignals: ["Chain Scale", "Brand Basics chain scale"],
    notes: "Same tier = 100; 1 tier apart = 50; 2 tiers = 25; 3+ tiers = 0 (soft factor only — not a hard gate).",
  },
  {
    key: "brandStandardsCompatibility",
    label: "Brand standards compatibility",
    dealSignals: ["Additional Amenities", "F&B Outlets?", "Number of Parking Spaces"],
    brandSignals: ["Additional Amenities", "F&B / parking requirements"],
    notes:
      "Graduated (not binary): amenity score = % of brand-required amenities the deal has; then −20 F&B required miss, −12 F&B count shortfall, −10 parking required miss; floor 0.",
  },
  {
    key: "feesToleranceCompatibility",
    label: "Fees tolerance",
    dealSignals: ["Royalty / marketing / loyalty fee expectations"],
    brandSignals: ["Typical fee structure ranges"],
    notes:
      "Graduated per comparable fee when owner has a numeric range: within/above brand range = 100; shortfall bands 75/50/25/0. Undetermined / unknown owner fees are excluded (not scored 0).",
  },
  {
    key: "roomRangeFitCompatibility",
    label: "Room range fit",
    dealSignals: ["Total Number of Rooms/Keys"],
    brandSignals: ["Min - Room Count", "Max - Room Count"],
    notes:
      "Graduated (not a hard gate): within brand min–max = 100; outside uses distance from nearest bound (≤10% → 75; ≤25% → 50; ≤50% → 25; else 0).",
  },
  {
    key: "serviceModelAlignment",
    label: "Service / operating model alignment",
    dealSignals: ["Hotel Service Model"],
    brandSignals: ["Hotel Service Model"],
    notes:
      "Graduated adjacency (not exact-only): exact = 100; Full-Service↔Lifestyle/Boutique = 55; Full-Service↔Select-Service = 40; Select-Service↔Lifestyle/Boutique = 50; Full-Service↔All-Inclusive = 25; Select-Service↔Extended Stay = 25; other pairs = 0. Unknown excluded.",
  },
  {
    key: "keyMoneyWillingnessCompatibility",
    label: "Key money willingness",
    dealSignals: ["Key money filter", "Must-Haves", "Top 3 Deal Breakers"],
    brandSignals: ["Incentive Types — Key Money / Upfront Incentive"],
    notes: "Also a hard gate when owner requires KM and brand does not offer it.",
  },
  {
    key: "softHardPreference",
    label: "Soft vs hard brand preference",
    dealSignals: ["Soft vs Hard Brand Preference"],
    brandSignals: ["Soft/Collection Brand"],
    notes: "Owner soft/hard preference matches brand soft/collection positioning = 100; conflict = 0.",
  },
  {
    key: "incentivesMatchCompatibility",
    label: "Incentives match",
    dealSignals: ["Incentive Types Interested In"],
    brandSignals: ["Incentive Types", "Willing to Negotiate Incentives"],
    notes: "Non–key-money incentive overlap / willingness.",
  },
  {
    key: "agreementsTypeCompatibility",
    label: "Agreements type compatibility",
    dealSignals: ["Preferred Deal Structure"],
    brandSignals: ["Acceptable Agreements Type"],
    notes: "Also a hard gate on mismatch. Soft factor is 100 when gate passes.",
  },
  {
    key: "buildingTypeCompatibility",
    label: "Building type compatibility",
    dealSignals: ["Building Type"],
    brandSignals: ["Acceptable Building Types"],
    notes: "Deal building type in brand Acceptable Building Types = 100; else 0. Missing either side excluded.",
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

/** How Match Score v2 base total is computed (before preferred bonus and gates). */
export const BRAND_MATCH_NEW_AGGREGATION = {
  method: "weighted_average_then_preferred_bonus_then_gates",
  nullFactorHandling: "exclude_from_denominator",
  /** Soft-factor weight that must be evaluable (non-null) before the numeric score is shown as reliable. */
  minScoredWeightPct: 40,
  preferredBonus: BRAND_MATCH_PREFERRED_BONUS,
  description:
    "Base = weighted average of soft factors with data on both sides (null / missing factors excluded from the denominator). " +
    "True mismatches score 0 and stay in the average. " +
    "Then add preferred-brand bonus (+4 if on Preferred list, cap 100). " +
    "Then apply hard gates (any fail → overall 0). " +
    "If scored soft weight is below minScoredWeightPct, UI treats the result as insufficient brand/deal data (not a low fit score). " +
    "Project stage remains breakdown-only. Commercial soft weight ≈ geography + same-brand density (~24%).",
};

/** @see BRAND_MATCH_NEW_AGGREGATION.minScoredWeightPct */
export const BRAND_MATCH_MIN_SCORED_WEIGHT_PCT = BRAND_MATCH_NEW_AGGREGATION.minScoredWeightPct;

/**
 * Shared UI / snapshot score bands (same thresholds as operator alignment colors).
 * Calibration target: solid complete matches ~70–80; 80+ exceptional.
 */
export const BRAND_MATCH_SCORE_BANDS = [
  { min: 80, label: "Strong alignment signals", uiClass: "match-score-high", tierLabel: "Higher Alignment Signal" },
  { min: 50, label: "Moderate alignment — review gaps", uiClass: "match-score-medium", tierLabel: "Moderate Alignment Signal" },
  { min: 25, label: "Weak alignment — significant gaps", uiClass: "match-score-weak", tierLabel: "Conditional Review Signal" },
  { min: 0, label: "Very limited alignment", uiClass: "match-score-poor", tierLabel: "Lower Alignment Signal" },
];

/** Territory radius defaults (km) — for v2.1+ census proximity; not applied in v2 engine yet. */
export const BRAND_MATCH_TERRITORY_RADIUS_KM = {
  urban: 10,
  resort: 40,
  otherOrUnknown: 25,
};

/** @param {unknown} score @returns {string} */
export function brandMatchTierFromScore(score) {
  const n = Number(score);
  if (!Number.isFinite(n)) return "Conditional Review Signal";
  for (const band of BRAND_MATCH_SCORE_BANDS) {
    if (n >= band.min) return band.tierLabel;
  }
  return "Lower Alignment Signal";
}
