/**
 * Empty research-ready demand anchor fixture templates (no sample points).
 */

export const DEFAULT_SOURCE_QUALITY_RULES = [
  "Every point requires a public or analyst source URL (sourceReference).",
  "No sample, placeholder, or lorem names.",
  "Coordinates required before import; use Google Places pre-import verification where recommended.",
  "Governance fields required on import: Scope Level, Relevance Tier, Use Case Tags, visibility fields.",
  "Do not import until verification report reviewed and requireVerifiedFile passes.",
];

export const RESORT_CORRIDOR_POINT_TYPE_TARGETS = [
  "Convention Center",
  "Tourist Attraction",
  "Beach / Waterfront",
  "Medical Campus",
  "University / College",
  "Sports Venue",
  "Entertainment District",
  "Industrial / Logistics Zone",
  "Government / Civic Center",
  "Business District",
  "Mixed-Use Development",
  "Future Growth Node",
];

export const URBAN_CORPORATE_POINT_TYPE_TARGETS = [
  "Convention Center",
  "Business District",
  "Medical Campus",
  "University / College",
  "Government / Civic Center",
  "Entertainment District",
  "Mixed-Use Development",
  "Industrial / Logistics Zone",
  "Tourist Attraction",
  "Future Growth Node",
];

export const CORRIDOR_ECO_POINT_TYPE_TARGETS = [
  "Tourist Attraction",
  "Beach / Waterfront",
  "Medical Campus",
  "University / College",
  "Entertainment District",
  "Industrial / Logistics Zone",
  "Government / Civic Center",
  "Mixed-Use Development",
  "Future Growth Node",
  "Sports Venue",
];

/**
 * @param {object} meta
 */
export function buildDemandAnchorFixtureTemplate(meta) {
  const today = new Date().toISOString().slice(0, 10);
  return {
    country: meta.country,
    region: meta.region,
    market: meta.market || "",
    buildStrategy: meta.buildStrategy,
    submarkets: meta.submarkets || [],
    firstPassTargets: meta.firstPassTargets || {},
    matureTargets: meta.matureTargets || {},
    pointTypeTargets: meta.pointTypeTargets || [],
    primaryHotelDemandProfiles: meta.primaryHotelDemandProfiles || [],
    sourceQualityRules: meta.sourceQualityRules || DEFAULT_SOURCE_QUALITY_RULES,
    governanceRequired: true,
    googlePreImportVerificationRecommended: meta.googlePreImportVerificationRecommended !== false,
    generatedAt: today,
    status: "template_research_ready",
    points: [],
  };
}
