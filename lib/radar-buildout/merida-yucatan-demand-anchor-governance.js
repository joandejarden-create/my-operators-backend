/**
 * Governance defaults for Mérida / Yucatán demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyMeridaYucatanGovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("Mexico", point, {
    ...overrides,
    scopeLevel: overrides.scopeLevel || "Market",
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `Mérida / Yucatán build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const MERIDA_YUCATAN_SUBMARKETS = ["Centro Histórico / Paseo de Montejo","Siglo XXI / Convention Zone","Airport Corridor","Progreso / Costa Yucateca","Industrial / Periférico","Other"];
