/**
 * AI Demand Positioning — Scenario Registry.
 * Merges standard + property-specific scenarios into a single universe.
 */

import { getStandardScenarios } from "./standard-scenarios.js";
import { generatePropertyScenarios } from "./property-scenarios.js";

const DOWNTOWN_NYC_SUBMARKET_HINTS = [
  "noho", "soho", "tribeca", "lower manhattan", "greenwich village",
  "east village", "west village", "financial district", "fidi",
  "downtown manhattan", "lower east", "chinatown", "little italy",
  "hudson square", "bowery",
];

const MIDTOWN_NYC_SUBMARKET_HINTS = [
  "times square", "midtown", "theater district", "theatre district",
  "hell's kitchen", "hells kitchen", "garment district", "hudson yards",
];

/**
 * Resolve which standard scenario pack applies to a property profile.
 * Downtown NYC properties get the downtown pack; Times Square/Midtown keep Midtown pack.
 */
export function resolveStandardScenarioMarket(propertyProfile) {
  const rawMarket = String(propertyProfile?.market || "");
  const submarket = String(propertyProfile?.submarket || "").toLowerCase();
  const marketLow = rawMarket.toLowerCase();

  if (submarket.includes("boca") || marketLow.includes("boca")) {
    return "boca_raton";
  }
  if (marketLow.includes("bermuda")) {
    return "bermuda";
  }
  if (
    marketLow.includes("kansas city") ||
    submarket.includes("power & light") ||
    submarket.includes("power and light") ||
    submarket.includes("downtown") && marketLow.includes("kansas")
  ) {
    return "kansas_city_downtown";
  }
  if (marketLow.includes("new york") || marketLow === "nyc") {
    if (isDowntownNycProfile(propertyProfile)) return "nyc_downtown";
    return "nyc_times_square";
  }
  return rawMarket;
}

export function isDowntownNycProfile(propertyProfile) {
  const submarket = String(propertyProfile?.submarket || "").toLowerCase();
  if (MIDTOWN_NYC_SUBMARKET_HINTS.some((hint) => submarket.includes(hint))) {
    return false;
  }
  return DOWNTOWN_NYC_SUBMARKET_HINTS.some((hint) => submarket.includes(hint));
}

export function buildScenarioUniverse(propertyProfile) {
  const market = resolveStandardScenarioMarket(propertyProfile);
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
