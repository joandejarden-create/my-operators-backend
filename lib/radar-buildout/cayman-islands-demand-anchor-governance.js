/**
 * Governance defaults for Cayman Islands countrywide demand anchor candidates.
 */

/** @type {Record<string, string[]>} */
const POINT_TYPE_USE_CASE_TAGS = {
  "Convention Center": ["Group / Convention", "Resort / Leisure"],
  "Medical Campus": ["Medical / Education", "Urban / Corporate"],
  "University / College": ["Medical / Education", "Government / Institutional"],
  "Sports Venue": ["Group / Convention", "Resort / Leisure"],
  "Entertainment District": ["Resort / Leisure", "Mixed-Use / Growth"],
  "Tourist Attraction": ["Resort / Leisure", "Nature / Eco-Tourism"],
  "Beach / Waterfront": ["Resort / Leisure", "Cruise / Port"],
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

export function applyCaymanIslandsGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

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
      `Cayman Islands countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

export const CAYMAN_ISLANDS_SUBMARKETS = [
  "Grand Cayman",
  "Cayman Brac",
  "Little Cayman",
  "Other",
];
