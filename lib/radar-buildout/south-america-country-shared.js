/**
 * Shared helpers for South America countrywide demand anchor builds.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
  createIslandCandidateBuilder,
} from "./island-country-shared.js";

export const SOUTH_AMERICA_REGION = "South America";

export function createSouthAmericaGovernance(countryDisplay) {
  return function applyGovernanceDefaults(point, overrides = {}) {
    const pointType = String(point.pointType || "").trim();
    const useCaseTags =
      overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Urban / Corporate"];

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

export function createSouthAmericaCandidateBuilder(countryDisplay, applyGovernance) {
  return createIslandCandidateBuilder(countryDisplay, SOUTH_AMERICA_REGION, applyGovernance);
}
