/**
 * IHG Hotels & Resorts — Portfolio Context ladder (Overview tab).
 * Static tier map when sibling IHG brands are not yet in Brand Basics.
 * Keep tier labels aligned with public/js/brand-explorer-atelier-from-api.js.
 */

export const IHG_LADDER_TIER_LABELS = [
  "Essential & Extended Stay",
  "Mainstream Upscale",
  "Premium Upscale",
  "Luxury & Lifestyle",
];

/** Representative IHG flags per tier (current brand omitted in UI when active). */
export const IHG_LADDER_TIER_BRANDS = [
  ["avid hotels", "Candlewood Suites", "Holiday Inn Express", "Staybridge Suites"],
  ["Holiday Inn", "Garner Hotels", "Atwell Suites"],
  ["Crowne Plaza", "Hotel Indigo", "voco", "EVEN Hotels", "HUALUXE"],
  ["InterContinental", "Regent", "Six Senses", "Vignette Collection"],
];

/**
 * @typedef {Object} IhgPortfolioContext
 * @property {0|1|2|3} ladderTier
 * @property {string} relativePositioning
 * @property {string} hotelChainScale
 */

/** @type {Record<string, IhgPortfolioContext>} */
export const IHG_PORTFOLIO_BY_BRAND_NAME = {
  "Kimpton Hotels": {
    ladderTier: 3,
    hotelChainScale: "Upper Upscale",
    relativePositioning:
      "Luxury & lifestyle flagship within IHG—Kimpton sits with InterContinental, Regent, and Six Senses at the experiential apex; above Hotel Indigo, voco, and Crowne Plaza upscale tiers—not midscale Holiday Inn Express, avid, or limited-service formats.",
  },
};

/** @param {string} parentCompany */
export function isIhgParentCompany(parentCompany) {
  const key = String(parentCompany || "")
    .trim()
    .toLowerCase();
  return key.includes("ihg hotels") || key.includes("intercontinental hotels group");
}

/** @param {string} airtableBrandName @returns {number | undefined} */
export function portfolioLadderTierForIhgBrandName(airtableBrandName) {
  return IHG_PORTFOLIO_BY_BRAND_NAME[String(airtableBrandName || "").trim()]?.ladderTier;
}

/** @param {string} airtableBrandName @returns {IhgPortfolioContext | null} */
export function portfolioContextForIhgBrand(airtableBrandName) {
  return IHG_PORTFOLIO_BY_BRAND_NAME[String(airtableBrandName || "").trim()] ?? null;
}

/**
 * @param {string} airtableBrandName
 * @returns {{ slotKey: string, title: string, body: string, sort: number }[]}
 */
export function ihgPortfolioPresentationRowsForBrand(airtableBrandName) {
  const ctx = portfolioContextForIhgBrand(airtableBrandName);
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
