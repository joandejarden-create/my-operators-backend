/**
 * Governance defaults for Antigua and Barbuda countrywide demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyAntiguaAndBarbudaGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Antigua and Barbuda", point, {
    ...overrides,
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Antigua and Barbuda countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const ANTIGUA_AND_BARBUDA_SUBMARKETS = ["St. John's","English Harbour","Dickenson Bay","Barbuda","Other"];
