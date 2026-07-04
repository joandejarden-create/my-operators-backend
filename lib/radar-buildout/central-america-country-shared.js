/**
 * Shared helpers for Central America countrywide demand anchor builds.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
  createIslandCandidateBuilder,
} from "./island-country-shared.js";

export const CENTRAL_AMERICA_REGION = "Central America";

export function createCentralAmericaGovernance(countryDisplay) {
  return function applyGovernanceDefaults(point, overrides = {}) {
    const pointType = String(point.pointType || "").trim();
    const useCaseTags =
      overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

    return applyIslandGovernanceDefaults(countryDisplay, point, {
      ...overrides,
      scopeLevel: overrides.scopeLevel || "Country",
      useCaseTags,
      relevanceTier:
        overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
      projectRelevanceLogic:
        overrides.projectRelevanceLogic ||
        `${countryDisplay} countrywide build — ${point.submarket || "Other"} ${pointType} anchor for hotel demand.`,
    });
  };
}

export function createCentralAmericaCandidateBuilder(countryDisplay, applyGovernance) {
  return createIslandCandidateBuilder(countryDisplay, CENTRAL_AMERICA_REGION, applyGovernance);
}

export function createEmptyGoogleCorrectionsModule(countrySlug, countryDisplay, pascalName) {
  return `/**
 * Google Places review corrections for ${countryDisplay} candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const ${countrySlug.toUpperCase().replace(/-/g, "_")}_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function apply${pascalName}PlaceReviewCorrection(point) {
  const fix = ${countrySlug.toUpperCase().replace(/-/g, "_")}_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = \`\${point.notes || ""} \${REVIEW_TAG}\`.trim();
  return merged;
}

export function apply${pascalName}PlaceReviewCorrections(points) {
  return points.map(apply${pascalName}PlaceReviewCorrection);
}
`;
}
