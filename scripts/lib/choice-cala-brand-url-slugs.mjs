/**
 * Match choicehotels.com property URLs to Brand Setup / census profile names.
 * Only URLs matching these path segments count as that brand's CALA listing.
 */

/** Census / fixture profile name → URL path must contain one of these segments */
export const BRAND_URL_SLUGS = {
  "Ascend Hotel Collection": ["ascend-hotels", "ascend-collection"],
  "Cambria Hotels": ["cambria-hotels"],
  "Clarion": ["clarion-hotels"],
  "Clarion Pointe": ["clarion-pointe"],
  "Comfort Inn & Suites": ["comfort-inn-hotels", "comfort-suites-hotels"],
  "Country Inn & Suites by Radisson (Choice)": ["country-hotels"],
  "Econo Lodge": ["econo-lodge-hotels"],
  "Everhome Suites": ["everhome-hotels", "everhome-suites"],
  "MainStay Suites": ["mainstay-hotels"],
  "Park Inn by Radisson (Choice)": ["park-inn-hotels"],
  "Park Plaza (Choice)": ["park-plaza-hotels"],
  "Quality Inn": ["quality-inn-hotels"],
  "Radisson (Choice)": ["radisson-hotels"],
  "Radisson Blu (Choice)": ["radisson-blu-hotels"],
  "Radisson Collection  (Choice)": ["radisson-collection"],
  "Radisson Individual (Choice)": ["radisson-individuals-hotels"],
  "Radisson Inn & Suites": ["radisson-inn", "radisson-inn-suites"],
  "Radisson RED  (Choice)": ["radisson-red-hotels"],
  "Rodeway Inn": ["rodeway-inn-hotels"],
  "Sleep Inn": ["sleep-inn-hotels"],
  "Suburban Studios": ["suburban-hotels", "suburban-studios"],
  "WoodSpring Suites": ["woodspring-hotels"],
};

/**
 * Brands that must not show footprint.openings until they have real CALA properties on choicehotels.com.
 */
export const BRANDS_NO_CALA_OPENINGS = new Set([
  "Clarion Pointe",
]);

/**
 * @param {string} profileName — census / fixture profile key
 * @param {string} url
 */
export function urlMatchesBrandSlug(profileName, url) {
  const key = String(profileName || "").trim();
  if (BRANDS_NO_CALA_OPENINGS.has(key)) return false;
  const slugs = BRAND_URL_SLUGS[key];
  if (!slugs?.length) return false;
  const path = String(url || "").toLowerCase();
  if (!path.includes("choicehotels.com")) return false;
  return slugs.some((slug) => path.includes(`/${slug}/`) || path.includes(`/${slug}`));
}
