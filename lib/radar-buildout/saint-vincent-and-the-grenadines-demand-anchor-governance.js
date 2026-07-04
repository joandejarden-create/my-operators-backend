/**
 * Governance defaults for Saint Vincent and the Grenadines countrywide demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applySaintVincentAndTheGrenadinesGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Saint Vincent and the Grenadines", point, {
    ...overrides,
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Saint Vincent and the Grenadines countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const SAINT_VINCENT_AND_THE_GRENADINES_SUBMARKETS = ["Kingstown","Grenadines","North Coast","Other"];
