/**
 * Governance defaults for Costa Rica countrywide demand anchor candidates.
 */

/** @type {Record<string, string[]>} */
const POINT_TYPE_USE_CASE_TAGS = {
  "Convention Center": ["Group / Convention", "Urban / Corporate"],
  "Medical Campus": ["Medical / Education", "Urban / Corporate"],
  "University / College": ["Medical / Education", "Urban / Corporate"],
  "Sports Venue": ["Group / Convention", "Urban / Corporate"],
  "Entertainment District": ["Urban / Corporate", "Resort / Leisure"],
  "Tourist Attraction": ["Resort / Leisure", "Nature / Eco-Tourism"],
  "Beach / Waterfront": ["Resort / Leisure", "Nature / Eco-Tourism"],
  "Business District": ["Urban / Corporate", "Mixed-Use / Growth"],
  "Industrial / Logistics Zone": ["Industrial / Logistics", "Airport / Transit"],
  "Government / Civic Center": ["Government / Institutional", "Urban / Corporate"],
  "Mixed-Use Development": ["Mixed-Use / Growth", "Resort / Leisure"],
  "Future Growth Node": ["Future Growth", "Airport / Transit"],
};

const TIER_1_POINT_TYPES = new Set([
  "Convention Center",
  "Business District",
  "Tourist Attraction",
  "Mixed-Use Development",
  "Future Growth Node",
  "Beach / Waterfront",
]);

export function applyCostaRicaGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags ||
    POINT_TYPE_USE_CASE_TAGS[pointType] ||
    ["Resort / Leisure"];

  return {
    ...point,
    scopeLevel: overrides.scopeLevel || "Country",
    relevanceTier:
      overrides.relevanceTier || (TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    useCaseTags,
    defaultMapVisibility: overrides.defaultMapVisibility || "Visible",
    externalVisibilityLevel: overrides.externalVisibilityLevel || "Member",
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Costa Rica countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

export const COSTA_RICA_SUBMARKETS = [
  "San José Metro",
  "Guanacaste / Papagayo",
  "Tamarindo / North Pacific",
  "Jacó / Herradura",
  "Manuel Antonio / Central Pacific",
  "Arenal / La Fortuna",
  "Caribbean Coast",
  "Monteverde",
  "Other",
];
