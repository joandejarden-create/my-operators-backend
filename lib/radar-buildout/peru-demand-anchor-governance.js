/**
 * Governance defaults for Peru — Lima / Cusco demand anchor candidates.
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
  "Tourist Attraction",
  "Mixed-Use Development",
  "Future Growth Node",
  "Beach / Waterfront",
  "Industrial / Logistics Zone",
]);

export function applyPeruGovernanceDefaults(point, overrides = {}) {
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
      `Peru Lima / Cusco build — ${submarket} ${pointType} anchor for hotel demand.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

export const PERU_LIMA_SUBMARKETS = [
  "Miraflores",
  "San Isidro",
  "Barranco",
  "Lima Historic Center",
  "Jorge Chávez Airport Corridor",
  "Surco / Convention / Business Corridor",
  "Callao / Port",
  "Other",
];

export const PERU_CUSCO_SUBMARKETS = [
  "Cusco Historic Center",
  "Sacred Valley",
  "Machu Picchu Access",
  "Urubamba",
  "Ollantaytambo",
  "Other",
];

export const PERU_LIMA_CUSCO_SUBMARKETS = [...PERU_LIMA_SUBMARKETS, ...PERU_CUSCO_SUBMARKETS];
