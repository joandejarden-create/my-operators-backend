/**
 * Portable Demand Territory Fit — REUSABLE_PRODUCT_LOGIC.
 *
 * Canonical class roles (function, not market name):
 *   CORE | NEARBY | COMPETITIVE | STRETCH | OUTSIDE
 *
 * Hotel/market display names and keyword maps live in hotel config (HOTEL_SPECIFIC_DATA).
 * Bethesda-era enum values remain valid aliases and normalize to the same roles.
 */

import { DEMAND_TERRITORY_FIT } from "./claim-types.js";

/** Canonical portable territory class codes */
export const TERRITORY_CLASS = Object.freeze({
  TERRITORY_CORE: "TERRITORY_CORE",
  TERRITORY_NEARBY: "TERRITORY_NEARBY",
  TERRITORY_COMPETITIVE: "TERRITORY_COMPETITIVE",
  TERRITORY_STRETCH: "TERRITORY_STRETCH",
  OUTSIDE_REALISTIC_TERRITORY: "OUTSIDE_REALISTIC_TERRITORY",
});

export const TERRITORY_CLASS_LABEL = Object.freeze({
  TERRITORY_CORE: "Core",
  TERRITORY_NEARBY: "Nearby / Adjacent",
  TERRITORY_COMPETITIVE: "Competitive",
  TERRITORY_STRETCH: "Stretch",
  OUTSIDE_REALISTIC_TERRITORY: "Outside Realistic Territory",
});

/** Functional role used by scoring / qualification (hotel-agnostic). */
export const TERRITORY_ROLE = Object.freeze({
  CORE: "CORE",
  NEARBY: "NEARBY",
  COMPETITIVE: "COMPETITIVE",
  STRETCH: "STRETCH",
  OUTSIDE: "OUTSIDE",
});

/**
 * Map any stored demandTerritoryFit code → portable role.
 * Bethesda aliases normalize without rewriting historical opportunity rows.
 */
export function toTerritoryRole(code) {
  const c = String(code || "").trim();
  if (!c) return null;
  if (
    c === TERRITORY_CLASS.TERRITORY_CORE ||
    c === DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE ||
    c === DEMAND_TERRITORY_FIT.NORTH_DC_MEDICAL_CORRIDOR
  ) {
    return TERRITORY_ROLE.CORE;
  }
  if (c === TERRITORY_CLASS.TERRITORY_NEARBY) {
    return TERRITORY_ROLE.NEARBY;
  }
  if (
    c === TERRITORY_CLASS.TERRITORY_COMPETITIVE ||
    c === DEMAND_TERRITORY_FIT.DMV_COMPETITIVE
  ) {
    return TERRITORY_ROLE.COMPETITIVE;
  }
  if (
    c === TERRITORY_CLASS.TERRITORY_STRETCH ||
    c === DEMAND_TERRITORY_FIT.DMV_STRETCH
  ) {
    return TERRITORY_ROLE.STRETCH;
  }
  if (
    c === TERRITORY_CLASS.OUTSIDE_REALISTIC_TERRITORY ||
    c === DEMAND_TERRITORY_FIT.OUTSIDE_REALISTIC_TERRITORY
  ) {
    return TERRITORY_ROLE.OUTSIDE;
  }
  return null;
}

export function isCoreTerritory(code) {
  const role = toTerritoryRole(code);
  return role === TERRITORY_ROLE.CORE || role === TERRITORY_ROLE.NEARBY;
}

export function isCompetitiveOrStretchTerritory(code) {
  const role = toTerritoryRole(code);
  return role === TERRITORY_ROLE.COMPETITIVE || role === TERRITORY_ROLE.STRETCH;
}

export function isOutsideTerritory(code) {
  return toTerritoryRole(code) === TERRITORY_ROLE.OUTSIDE;
}

/**
 * Resolve customer-facing label: hotel config override → generic → raw code.
 * @param {string|null} code
 * @param {object|null} hotelConfig
 */
export function resolveTerritoryFitLabel(code, hotelConfig = null) {
  if (!code) return null;
  const custom = hotelConfig?.demandTerritory?.displayLabels?.[code];
  if (custom) return custom;
  if (TERRITORY_CLASS_LABEL[code]) return TERRITORY_CLASS_LABEL[code];
  // Bethesda-era labels remain in DEMAND_TERRITORY_FIT_LABEL via claim-types import site
  return code;
}

/**
 * Classify Demand Territory Fit from hotel config keyword maps when present.
 * Returns null when config has no keyword maps (caller may use legacy heuristic).
 *
 * @param {object} opportunity
 * @param {object} hotelConfig
 * @returns {{ demandTerritoryFit: string, demandTerritoryRationale: string } | null}
 */
export function classifyDemandTerritoryFitFromConfig(opportunity, hotelConfig) {
  if (
    opportunity?.demandTerritoryFitLocked &&
    opportunity.demandTerritoryFit &&
    toTerritoryRole(opportunity.demandTerritoryFit)
  ) {
    return {
      demandTerritoryFit: opportunity.demandTerritoryFit,
      demandTerritoryRationale:
        opportunity.demandTerritoryRationale ||
        "Curated Demand Territory Fit locked from research.",
      demandTerritoryFitLocked: true,
    };
  }

  const keywords = hotelConfig?.demandTerritory?.classificationKeywords;
  if (!keywords || typeof keywords !== "object") return null;

  const blob = [
    opportunity.title,
    opportunity.destinationStatus,
    opportunity.venueStatus,
    opportunity.summaryWhat,
    opportunity.summaryWhyMatters,
    opportunity.whyNow,
    opportunity.organizationName,
    opportunity.segment,
    opportunity.eventLocationSummary,
    ...(opportunity.meetingHistory || []).map((h) => `${h.city} ${h.venue}`),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  const order = [
    TERRITORY_CLASS.OUTSIDE_REALISTIC_TERRITORY,
    TERRITORY_CLASS.TERRITORY_STRETCH,
    TERRITORY_CLASS.TERRITORY_COMPETITIVE,
    TERRITORY_CLASS.TERRITORY_NEARBY,
    TERRITORY_CLASS.TERRITORY_CORE,
  ];

  // Prefer most-specific / tightest geography that matches (CORE last so OUTSIDE wins when both hit).
  // Scan OUTSIDE → STRETCH → COMPETITIVE → NEARBY → CORE; first keyword hit wins so
  // distant metros disqualify before local tokens falsely elevate.
  for (const classCode of order) {
    const list = keywords[classCode] || [];
    for (const kw of list) {
      const needle = String(kw || "").toLowerCase().trim();
      if (needle && blob.includes(needle)) {
        const label =
          hotelConfig.demandTerritory?.displayLabels?.[classCode] ||
          TERRITORY_CLASS_LABEL[classCode] ||
          classCode;
        return {
          demandTerritoryFit: classCode,
          demandTerritoryRationale: `Matched hotel territory keyword "${needle}" → ${label}.`,
        };
      }
    }
  }

  // No keyword match — default COMPETITIVE (ask whether hotel can compete) not OUTSIDE.
  return {
    demandTerritoryFit: TERRITORY_CLASS.TERRITORY_COMPETITIVE,
    demandTerritoryRationale:
      "No territory keyword match — defaulted to Competitive pending stronger destination evidence.",
  };
}
