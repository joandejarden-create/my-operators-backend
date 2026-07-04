/**
 * Governance defaults for Chile — Valparaíso / Patagonia mature-pass demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyChileValparaisoPatagoniaGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Chile", point, {
    ...overrides,
    scopeLevel: overrides.scopeLevel || "Market",
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Chile Valparaíso / Patagonia mature pass — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const CHILE_VALPARAISO_PATAGONIA_SUBMARKETS = [
  "Valparaíso / Viña del Mar",
  "Patagonia Lakes",
  "Puerto Natales",
];
