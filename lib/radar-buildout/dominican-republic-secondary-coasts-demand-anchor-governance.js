/**
 * Governance defaults for DR Secondary Coasts Mature Pass demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyDominicanRepublicSecondaryCoastsGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Dominican Republic", point, {
    ...overrides,
    scopeLevel: overrides.scopeLevel || "Country",
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `DR Secondary Coasts Mature Pass build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const DOMINICAN_REPUBLIC_SECONDARY_COASTS_SUBMARKETS = [
  "Miches / Costa Esmeralda",
  "Barahona / Pedernales",
  "Jarabacoa / Constanza",
];
