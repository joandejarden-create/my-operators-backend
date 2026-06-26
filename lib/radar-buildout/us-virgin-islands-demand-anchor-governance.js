/**
 * Governance defaults for U.S. Virgin Islands Countrywide demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyUsVirginIslandsGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("U.S. Virgin Islands", point, {
    ...overrides,
    scopeLevel: overrides.scopeLevel || "Country",
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `U.S. Virgin Islands Countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const US_VIRGIN_ISLANDS_SUBMARKETS = ["St. Thomas","St. Croix","St. John","Other"];
