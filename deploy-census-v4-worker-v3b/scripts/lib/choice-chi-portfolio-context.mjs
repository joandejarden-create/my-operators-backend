/**
 * Choice Hotels International — Overview Portfolio Context (ladder tier + copy).
 * Drives overview.portfolio_ladder_tier (0–3) and portfolio-focused overview.relative_positioning.
 *
 * Ladder labels in UI (brand-explorer-atelier-from-api.js) must stay aligned:
 *   0 Economy / Core Midscale
 *   1 Upper Mid / Mainstream Upscale
 *   2 Premium / Upper Upscale
 *   3 Luxury & Lifestyle Flagship
 */
import { AIRTABLE_TO_PROFILE_NAME } from "./choice-chi-brand-resolve.mjs";

/** @typedef {0|1|2|3} LadderTier */

/**
 * @typedef {Object} ChiPortfolioContext
 * @property {LadderTier} ladderTier
 * @property {string} relativePositioning
 * @property {string} hotelChainScale — Airtable Brand Basics value (sanity reference)
 */

/** Mirror public/js ladderIndexForScale — keep tier logic in sync. */
export function ladderTierFromChainScale(scale) {
  const s = String(scale || "").toLowerCase();
  if (!s) return 2;
  if (s.includes("luxury") || s.includes("upper upscale")) return 3;
  if (s.includes("upscale") && !s.includes("upper")) return 2;
  if (s.includes("upper mid") || s.includes("midscale")) return 1;
  if (s.includes("economy")) return 0;
  return 2;
}

export const CHI_LADDER_TIER_LABELS = [
  "Economy / Core Midscale",
  "Upper Mid / Mainstream Upscale",
  "Premium / Upper Upscale",
  "Luxury & Lifestyle Flagship",
];

/**
 * Canonical portfolio context per CHI brand (keys = Brand Basics "Brand Name" in Airtable).
 * @type {Record<string, ChiPortfolioContext>}
 */
export const CHI_PORTFOLIO_BY_BRAND_NAME = {
  "Econo Lodge": {
    ladderTier: 0,
    hotelChainScale: "Economy",
    relativePositioning:
      "Leftmost economy tier in the Choice portfolio—roadside essentials below midscale Value Q flags (Quality, Sleep) and all extended-stay and upscale brands; not a conversion play for upper-midscale prototypes.",
  },
  "Rodeway Inn": {
    ladderTier: 0,
    hotelChainScale: "Economy",
    relativePositioning:
      "Core economy companion to Econo Lodge on the portfolio spectrum—no-frills savings at the low end; below midscale select-service and far below upscale Radisson and Cambria tiers.",
  },
  "Suburban Studios": {
    ladderTier: 0,
    hotelChainScale: "Economy",
    relativePositioning:
      "Economy extended-stay at the portfolio base—kitchen-led longer stays below midscale MainStay and Everhome; not nightly midscale or upscale full-service conversion targets.",
  },
  "WoodSpring Suites": {
    ladderTier: 0,
    hotelChainScale: "Economy",
    relativePositioning:
      "Economy extended-stay flagship within CHI—weeks-long value stays at the left of the spectrum; above only in length-of-stay economics, not chain scale versus Quality, Comfort, or Radisson tiers.",
  },
  "Quality Inn": {
    ladderTier: 1,
    hotelChainScale: "Midscale",
    relativePositioning:
      "Founding midscale Value Q brand—center-left of the CHI ladder above economy roadside flags, below upper-midscale Comfort and upscale Cambria/Radisson; not upper-upscale lifestyle or collection positioning.",
  },
  "Sleep Inn": {
    ladderTier: 1,
    hotelChainScale: "Midscale",
    relativePositioning:
      "Midscale “Dream Better Here” select-service—same portfolio band as Quality Inn, below Comfort upper-midscale and upscale tiers; not economy extended-stay or premium full-service.",
  },
  "Clarion Pointe": {
    ladderTier: 1,
    hotelChainScale: "Midscale",
    relativePositioning:
      "Midscale select-service extension of Clarion—modernist essentials tier below full-service Clarion upscale and Cambria; above economy flags, not Radisson upper-upscale collection plays.",
  },
  "Everhome Suites": {
    ladderTier: 1,
    hotelChainScale: "Midscale",
    relativePositioning:
      "Midscale extended-stay in the upper-mid/mainstream band—kitchen suites below upscale full-service and Radisson tiers; above economy WoodSpring/Suburban, not Cambria or Blu flagship positioning.",
  },
  "MainStay Suites": {
    ladderTier: 1,
    hotelChainScale: "Midscale",
    relativePositioning:
      "Mature midscale extended-stay—portfolio mainstream for week+ stays, paired with Everhome and below nightly upscale; not economy roadside or upper-upscale Radisson lifestyle flags.",
  },
  "Comfort Inn & Suites": {
    ladderTier: 1,
    hotelChainScale: "Upper Midscale",
    relativePositioning:
      "Upper-midscale breakfast-led flagship—right of Quality/Sleep midscale, left of upscale Cambria and Radisson full-service; not economy, extended-stay economy, or upper-upscale Blu/Collection tier.",
  },
  "Country Inn & Suites by Radisson (Choice)": {
    ladderTier: 1,
    hotelChainScale: "Upper Midscale",
    relativePositioning:
      "Upper-midscale homelike select-service in the mainstream band with Comfort and Park Inn—below Radisson core upscale and all upper-upscale Radisson sub-brands; not economy or Cambria-style upscale indulgence.",
  },
  "Park Inn by Radisson (Choice)": {
    ladderTier: 1,
    hotelChainScale: "Upper Midscale",
    relativePositioning:
      "Upper-midscale vibrant select-service—bold design in the mainstream tier, below Park Plaza and Radisson upscale full-service; not Blu, RED, Individuals, or Collection flagship positioning.",
  },
  "Radisson Inn & Suites": {
    ladderTier: 1,
    hotelChainScale: "Upper Midscale",
    relativePositioning:
      "Newest upper-midscale Radisson select-service—café-lobby mainstream tier with Comfort and Park Inn, below Radisson (Choice) upscale and upper-upscale Radisson siblings.",
  },
  "Ascend Hotel Collection": {
    ladderTier: 2,
    hotelChainScale: "Upscale",
    relativePositioning:
      "Upscale soft collection—premium tier for independent character with Choice scale, below upper-upscale Radisson Blu/Individuals/Collection and lifestyle RED; above midscale and upper-midscale hard brands.",
  },
  "Cambria Hotels": {
    ladderTier: 2,
    hotelChainScale: "Upscale",
    relativePositioning:
      "Upscale by Choice—approachable indulgence in the premium band, below upper-upscale Radisson Blu and collection/lifestyle flagships; above Comfort/Quality midscale and economy extended-stay.",
  },
  "Clarion": {
    ladderTier: 2,
    hotelChainScale: "Upscale",
    relativePositioning:
      "Upscale full-service meetings and social positioning—premium tier with Cambria and Radisson core, below upper-upscale Blu/Collection; above Clarion Pointe midscale and all economy flags.",
  },
  "Park Plaza (Choice)": {
    ladderTier: 2,
    hotelChainScale: "Upscale",
    relativePositioning:
      "Upscale full-service city and resort meetings brand—premium CHI tier with Radisson (Choice) and Cambria, below Radisson upper-upscale and soft-collection flagships; not upper-midscale select-service.",
  },
  "Radisson (Choice)": {
    ladderTier: 2,
    hotelChainScale: "Upscale",
    relativePositioning:
      "Core upscale Radisson in the premium band—charming simplicity above upper-midscale flags, below Blu, Collection, Individuals, and RED on the right side of the CHI spectrum; not economy or midscale Value Q.",
  },
  "Radisson Blu (Choice)": {
    ladderTier: 3,
    hotelChainScale: "Upper Upscale",
    relativePositioning:
      "Upper-upscale design flagship at the top of the CHI ladder—Nordic Nouveau above Radisson upscale and RED lifestyle select-service; not midscale, economy, or extended-stay formats.",
  },
  "Radisson Collection  (Choice)": {
    ladderTier: 3,
    hotelChainScale: "Upper Upscale",
    relativePositioning:
      "Upper-upscale luxury collection at the portfolio apex—distinctive character hotels with Blu and Individuals, above all midscale and premium upscale hard brands.",
  },
  "Radisson Individual (Choice)": {
    ladderTier: 3,
    hotelChainScale: "Upper Upscale",
    relativePositioning:
      "Upper-upscale soft brand for hand-selected independents—rightmost lifestyle/collection band with Blu and Collection, above Ascend upscale soft brand and all midscale tiers.",
  },
  "Radisson RED  (Choice)": {
    ladderTier: 3,
    hotelChainScale: "Upper Upscale",
    relativePositioning:
      "Upper-upscale lifestyle select-service at the high end—playful urban RED alongside Blu and Collection, above core Radisson upscale and every midscale/economy flag (despite select-service operating model).",
  },
};

/**
 * @param {string} airtableBrandName
 * @returns {ChiPortfolioContext | null}
 */
export function portfolioContextForAirtableBrand(airtableBrandName) {
  const name = String(airtableBrandName || "").trim();
  if (!name) return null;
  if (CHI_PORTFOLIO_BY_BRAND_NAME[name]) return CHI_PORTFOLIO_BY_BRAND_NAME[name];
  const profileKey = AIRTABLE_TO_PROFILE_NAME[name];
  if (profileKey && CHI_PORTFOLIO_BY_BRAND_NAME[profileKey]) {
    return CHI_PORTFOLIO_BY_BRAND_NAME[profileKey];
  }
  return null;
}

/**
 * One presentation row per brand: Title = ladder tier (0–3), Body = relative positioning.
 * (Sort Order is not used to split tiers—only for ordering multiple rows of the same slot key elsewhere.)
 *
 * @param {string} airtableBrandName
 * @returns {{ slotKey: string, title: string, body: string, sort: number }[]}
 */
export function portfolioPresentationRowsForBrand(airtableBrandName) {
  const ctx = portfolioContextForAirtableBrand(airtableBrandName);
  if (!ctx) return [];
  return [
    {
      slotKey: "overview.portfolio_context",
      title: String(ctx.ladderTier),
      body: ctx.relativePositioning,
      sort: 0,
    },
  ];
}

/** @param {string} airtableBrandName @returns {number | undefined} */
export function portfolioLadderTierForAirtableBrandName(airtableBrandName) {
  return portfolioContextForAirtableBrand(airtableBrandName)?.ladderTier;
}
