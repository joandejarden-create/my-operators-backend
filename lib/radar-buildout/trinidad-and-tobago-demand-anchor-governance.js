/**
 * Governance defaults for Trinidad and Tobago countrywide demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyTrinidadAndTobagoGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Trinidad and Tobago", point, {
    ...overrides,
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Trinidad and Tobago countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const TRINIDAD_AND_TOBAGO_SUBMARKETS = ["Port of Spain","East-West Corridor","Tobago","South Trinidad","Other"];
