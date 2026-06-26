/**
 * Governance defaults for Chile — Santiago demand anchor candidates.
 */

/** @type {Record<string, string[]>} */
const POINT_TYPE_USE_CASE_TAGS = {
  "Convention Center": ["Group / Convention", "Urban / Corporate"],
  "Medical Campus": ["Medical / Education", "Urban / Corporate"],
  "University / College": ["Medical / Education", "Urban / Corporate"],
  "Sports Venue": ["Group / Convention", "Urban / Corporate"],
  "Entertainment District": ["Urban / Corporate", "Mixed-Use / Growth"],
  "Tourist Attraction": ["Urban / Corporate", "Heritage / Cultural Tourism"],
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
  "Mixed-Use Development",
  "Government / Civic Center",
  "Medical Campus",
  "Future Growth Node",
]);

export function applyChileGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags ||
    POINT_TYPE_USE_CASE_TAGS[pointType] ||
    ["Urban / Corporate"];

  return {
    ...point,
    scopeLevel: overrides.scopeLevel || "Market",
    relevanceTier:
      overrides.relevanceTier || (TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    useCaseTags,
    defaultMapVisibility: overrides.defaultMapVisibility || "Visible",
    externalVisibilityLevel: overrides.externalVisibilityLevel || "Member",
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Chile Santiago build — ${submarket} ${pointType} anchor for hotel demand.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

export const CHILE_SANTIAGO_SUBMARKETS = [
  "Las Condes",
  "Providencia",
  "Vitacura",
  "Santiago Centro",
  "Airport Corridor",
  "Convention / Events Corridor",
  "Costanera / Financial District",
  "El Golf / Sanhattan",
  "Parque Arauco / Nueva Las Condes",
  "Other",
];
