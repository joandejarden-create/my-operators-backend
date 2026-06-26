/**
 * Default governance fields for Colombia demand anchor candidates (fixture only).
 * Values align with CALA Radar governance model; not Google-specific.
 */

/** @type {Record<string, string[]>} */
const POINT_TYPE_USE_CASE_TAGS = {
  "Convention Center": ["Group / Convention", "Urban / Corporate"],
  "Medical Campus": ["Medical / Education", "Urban / Corporate"],
  "University / College": ["Medical / Education", "Urban / Corporate"],
  "Sports Venue": ["Group / Convention", "Resort / Leisure"],
  "Entertainment District": ["Resort / Leisure", "Urban / Corporate"],
  "Tourist Attraction": ["Resort / Leisure", "Heritage / Cultural Tourism"],
  "Beach / Waterfront": ["Resort / Leisure", "Luxury / Resort"],
  "Business District": ["Urban / Corporate", "Mixed-Use / Growth"],
  "Industrial / Logistics Zone": ["Industrial / Logistics", "Mixed-Use / Growth"],
  "Government / Civic Center": ["Government / Institutional", "Urban / Corporate"],
  "Mixed-Use Development": ["Mixed-Use / Growth", "Urban / Corporate"],
  "Future Growth Node": ["Mixed-Use / Growth", "Future Growth"],
};

const TIER_1_POINT_TYPES = new Set([
  "Convention Center",
  "Medical Campus",
  "Entertainment District",
  "Tourist Attraction",
  "Business District",
  "Beach / Waterfront",
]);

/**
 * @param {object} point
 * @param {object} [overrides]
 */
export function applyColombiaGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || point.city || "").trim();
  const useCaseTags =
    overrides.useCaseTags ||
    POINT_TYPE_USE_CASE_TAGS[pointType] ||
    ["Mixed-Use / Growth"];

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
      `Colombia Phase 1 ${submarket} market-by-market build — ${pointType} anchor for hotel demand screening.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

export const COLOMBIA_PHASE_1_SUBMARKETS = ["Cartagena", "Bogotá", "Medellín"];

export const COLOMBIA_PHASE_2_SUBMARKETS = [
  "Barranquilla",
  "Cali",
  "Santa Marta",
  "Coffee Region / Pereira",
  "San Andrés",
];
