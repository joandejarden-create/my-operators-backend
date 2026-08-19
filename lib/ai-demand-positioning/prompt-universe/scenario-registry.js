/**
 * AI Demand Positioning — Scenario Registry.
 * Merges standard + property-specific scenarios into a single universe.
 */

import { getStandardScenarios } from "./standard-scenarios.js";
import { generatePropertyScenarios } from "./property-scenarios.js";

export function buildScenarioUniverse(propertyProfile) {
  const rawMarket = propertyProfile.market || "";
  const submarket = propertyProfile.submarket || "";
  const market = submarket.toLowerCase().includes("boca") || rawMarket.toLowerCase().includes("boca")
    ? "boca_raton" : rawMarket;
  const chainScale = (propertyProfile.chainScale || "").toLowerCase().replace(/\s+/g, "_");

  const standard = getStandardScenarios(market, chainScale).map((s) => ({
    ...s,
    source: "standard",
    propertyId: propertyProfile.propertyId,
  }));

  const specific = generatePropertyScenarios(propertyProfile).map((s) => ({
    ...s,
    source: "property_specific",
    propertyId: propertyProfile.propertyId,
  }));

  return [...standard, ...specific];
}

export function getScenarioById(universe, scenarioId) {
  return universe.find((s) => s.scenarioId === scenarioId) || null;
}

export function getScenariosByIntent(universe, intent) {
  return universe.filter((s) => s.intent === intent);
}
