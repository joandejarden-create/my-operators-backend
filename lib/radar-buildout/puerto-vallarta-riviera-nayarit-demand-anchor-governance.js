/**
 * Governance defaults for Puerto Vallarta / Riviera Nayarit demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function applyPuertoVallartaRivieraNayaritGovernanceDefaults(point, overrides = {}) {
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
      `Puerto Vallarta / Riviera Nayarit build — ${submarket} ${pointType} anchor for hotel demand.`,
  });
}

export const PUERTO_VALLARTA_RIVIERA_NAYARIT_SUBMARKETS = ["Zona Hotelera / Malecón","Marina Vallarta","Nuevo Vallarta","Riviera Nayarit North Coast","Sayulita / Punta de Mita","Airport Corridor","Other"];
