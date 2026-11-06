/**
 * AI Demand Positioning — Scenario Registry.
 * Merges standard + property-specific scenarios into a single universe.
 */

import { getStandardScenarios } from "./standard-scenarios.js";
import { generatePropertyScenarios } from "./property-scenarios.js";
import {
  generateGenericProfileScenarios,
  generateGenericPropertyCapabilityScenarios,
  buildGenericParityScenarioUniverse,
  GENERIC_PROFILE_SCENARIOS_VERSION,
} from "./generic-profile-scenarios.js";

export {
  generateGenericProfileScenarios,
  generateGenericPropertyCapabilityScenarios,
  buildGenericParityScenarioUniverse,
  GENERIC_PROFILE_SCENARIOS_VERSION,
};

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

  if (
    marketLow.includes("boca raton") ||
    submarket.includes("boca raton") ||
    (submarket.includes("boca") && !submarket.includes("bocagrande") && !marketLow.includes("cartagena"))
  ) {
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
  if (
    marketLow.includes("bethesda") ||
    marketLow.includes("montgomery county") ||
    submarket.includes("pooks hill") ||
    submarket.includes("north bethesda") ||
    submarket.includes("rockville pike")
  ) {
    return "bethesda_montgomery";
  }
  if (
    marketLow.includes("monterrey") ||
    submarket.includes("monterrey valle") ||
    submarket.includes("valle oriente") ||
    submarket.includes("san pedro")
  ) {
    return "monterrey_valle";
  }
  if (
    marketLow.includes("mexico city") ||
    submarket.includes("reforma") ||
    submarket.includes("paseo de la reforma")
  ) {
    return "mexico_city_reforma";
  }
  if (
    marketLow.includes("punta cana") ||
    marketLow.includes("cap cana") ||
    submarket.includes("cap cana")
  ) {
    return "cap_cana";
  }
  if (marketLow.includes("santo domingo") || submarket.includes("piantini") || submarket.includes("naco")) {
    return "santo_domingo";
  }
  if (marketLow.includes("cartagena") || submarket.includes("bocagrande")) {
    return "cartagena_bocagrande";
  }
  if (marketLow.includes("bogot") || submarket.includes("bogotá norte") || submarket.includes("bogota norte")) {
    return "bogota_norte";
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

  const combined = [...standard, ...specific];
  // Generic parity fallback when no market pack / property pack — synthesizes from
  // profile fields only (destination/archetype ~48 + property-capability ~15).
  // Thin 20-row onboarding packs are not methodologically comparable to certified hotels.
  if (!combined.length && propertyProfile?.propertyId) {
    const pack = buildGenericParityScenarioUniverse(propertyProfile);
    return pack.scenarios.map((s) => ({
      ...s,
      source: s.layer === "property_capability" ? "generic_property_capability" : "generic_profile",
      propertyId: propertyProfile.propertyId,
      scenarioUniverseVersion: GENERIC_PROFILE_SCENARIOS_VERSION,
    }));
  }

  // PROPERTY_CAPABILITY fill-in: only when a certified *market* pack exists but
  // the hotel has no property-specific pack. Never stack generic capability on
  // top of an existing property_specific pack — that produced 78/80/93 density
  // skew vs the ~63 parity target (mature = market+property; new = generic v2).
  if (combined.length && propertyProfile?.propertyId && specific.length === 0) {
    const existingIds = new Set(
      combined.map((s) => s.scenarioId || s.id).filter(Boolean)
    );
    const capability = generateGenericPropertyCapabilityScenarios(propertyProfile)
      .filter((s) => {
        const id = s.scenarioId || s.id;
        return id && !existingIds.has(id);
      })
      .map((s) => ({
        ...s,
        source: "generic_property_capability",
        propertyId: propertyProfile.propertyId,
        scenarioUniverseVersion: GENERIC_PROFILE_SCENARIOS_VERSION,
      }));
    if (capability.length) {
      return [...combined, ...capability];
    }
  }

  return combined;
}

export function getScenarioById(universe, scenarioId) {
  return universe.find((s) => s.scenarioId === scenarioId) || null;
}

export function getScenariosByIntent(universe, intent) {
  return universe.filter((s) => s.intent === intent);
}
