/**
 * Hilton Worldwide — Portfolio Context ladder (Overview tab).
 * Static tier map when sibling Hilton brands are not yet in Brand Basics.
 * Keep tier labels aligned with public/js/brand-explorer-atelier-from-api.js.
 */

export const HILTON_LADDER_TIER_LABELS = [
  "Focused Service & Extended Stay",
  "Mainstream Upscale",
  "Premium Full-Service",
  "Luxury & Lifestyle Collections",
];

/** Representative Hilton flags per tier (current brand omitted in UI when active). */
export const HILTON_LADDER_TIER_BRANDS = [
  ["Tru by Hilton", "Spark by Hilton", "Hampton by Hilton", "Home2 Suites", "Homewood Suites", "LivSmart Studios"],
  ["Hilton Garden Inn", "Embassy Suites by Hilton"],
  ["Hilton Hotels & Resorts", "DoubleTree by Hilton", "Signia by Hilton", "Canopy by Hilton"],
  ["Waldorf Astoria", "Conrad Hotels & Resorts", "LXR Hotels & Resorts", "Tapestry Collection", "Motto by Hilton", "Tempo by Hilton"],
];

/**
 * @typedef {Object} HiltonPortfolioContext
 * @property {0|1|2|3} ladderTier
 * @property {string} relativePositioning
 * @property {string} hotelChainScale
 */

/** @type {Record<string, HiltonPortfolioContext>} */
export const HILTON_PORTFOLIO_BY_BRAND_NAME = {
  "Curio Collection by Hilton": {
    ladderTier: 3,
    hotelChainScale: "Upper Upscale",
    relativePositioning:
      "Upper-upscale soft collection within Hilton—Curio sits with Tapestry as the independent-character tier; below Waldorf Astoria, Conrad, and LXR luxury; above Hilton Hotels & Resorts core full-service, DoubleTree, and Garden Inn—not Hampton, Tru, Spark, or Homewood extended-stay formats.",
  },
};

/** @param {string} parentCompany */
export function isHiltonParentCompany(parentCompany) {
  const key = String(parentCompany || "")
    .trim()
    .toLowerCase();
  return key.includes("hilton worldwide") || key === "hilton" || key.includes("hilton ");
}

/** @param {string} airtableBrandName @returns {HiltonPortfolioContext | null} */
export function portfolioContextForHiltonBrand(airtableBrandName) {
  return HILTON_PORTFOLIO_BY_BRAND_NAME[String(airtableBrandName || "").trim()] ?? null;
}

/**
 * @param {string} airtableBrandName
 * @returns {{ slotKey: string, title: string, body: string, sort: number }[]}
 */
export function hiltonPortfolioPresentationRowsForBrand(airtableBrandName) {
  const ctx = portfolioContextForHiltonBrand(airtableBrandName);
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
