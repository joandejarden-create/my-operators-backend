/**
 * Governance defaults for Peru — Arequipa / Paracas mature-pass demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyPeruArequipaParacasGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Peru", point, {
    ...overrides,
    scopeLevel: overrides.scopeLevel || "Market",
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Peru Arequipa / Paracas mature pass — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const PERU_AREQUIPA_PARACAS_SUBMARKETS = ["Arequipa", "Paracas"];
