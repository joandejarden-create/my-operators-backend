/**
 * Governance defaults for Panama countrywide demand anchor candidates.
 */

/** @type {Record<string, string[]>} */
const POINT_TYPE_USE_CASE_TAGS = {
  "Convention Center": ["Group / Convention", "Urban / Corporate"],
  "Medical Campus": ["Medical / Education", "Urban / Corporate"],
  "University / College": ["Medical / Education", "Urban / Corporate"],
  "Sports Venue": ["Group / Convention", "Urban / Corporate"],
  "Entertainment District": ["Urban / Corporate", "Resort / Leisure"],
  "Tourist Attraction": ["Resort / Leisure", "Heritage / Cultural Tourism"],
  "Beach / Waterfront": ["Resort / Leisure", "Mixed-Use / Growth"],
  "Business District": ["Urban / Corporate", "Mixed-Use / Growth"],
  "Industrial / Logistics Zone": ["Industrial / Logistics", "Airport / Transit"],
  "Government / Civic Center": ["Government / Institutional", "Urban / Corporate"],
  "Mixed-Use Development": ["Mixed-Use / Growth", "Urban / Corporate"],
  "Future Growth Node": ["Future Growth", "Airport / Transit"],
};

const TIER_1_POINT_TYPES = new Set([
  "Convention Center",
  "Business District",
  "Industrial / Logistics Zone",
  "Tourist Attraction",
  "Mixed-Use Development",
  "Future Growth Node",
  "Beach / Waterfront",
]);

const MARKET = "Panama Countrywide";

export function applyPanamaGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags ||
    POINT_TYPE_USE_CASE_TAGS[pointType] ||
    ["Urban / Corporate"];

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
      `Panama countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

export const PANAMA_SUBMARKETS = [
  "Panama City",
  "Tocumen / Airport Corridor",
  "Canal / Logistics Corridor",
  "Casco Viejo / Waterfront",
  "Costa del Este",
  "Pacific Beaches",
  "Boquete / Highlands",
  "Bocas del Toro",
  "Other",
];
