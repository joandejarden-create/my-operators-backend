/**
 * Customer-safe demand territory labels — governed from scenario registry intents.
 * Do not invent categories outside TRAVELER_INTENTS.
 *
 * CUSTOMER_TERMINOLOGY_PATCH (2026-08-20): leisure display label
 * "Resort Leisure" → "Leisure Travel". Internal key remains `leisure`.
 * Not a measurement-contract-breaking change.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";

export const CUSTOMER_TERMINOLOGY_VERSION = "adp_customer_terminology_v2";
export const LEISURE_CUSTOMER_LABEL = "Leisure Travel";
export const LEGACY_LEISURE_CUSTOMER_LABEL = "Resort Leisure";

export const DEMAND_TERRITORY_TOOLTIP_DEFINITION =
  "Which type of traveler demand is being measured (business, leisure, couples, meetings, and so on)?";

export const LEISURE_TRAVEL_CUSTOMER_DEFINITION =
  "Leisure-oriented stays such as vacations, weekend trips, city breaks, resort stays, sightseeing and entertainment-led travel.";

export const INTENT_TERRITORY_LABELS = Object.freeze({
  [TRAVELER_INTENTS.BUSINESS]: "Business Travel",
  [TRAVELER_INTENTS.LEISURE]: LEISURE_CUSTOMER_LABEL,
  [TRAVELER_INTENTS.COUPLES]: "Couples / Romantic Stay",
  [TRAVELER_INTENTS.FAMILY]: "Family Travel",
  [TRAVELER_INTENTS.GROUP_MEETING]: "Meetings & Groups",
  [TRAVELER_INTENTS.WELLNESS]: "Wellness",
  [TRAVELER_INTENTS.ADVENTURE]: "Adventure & Experiences",
  [TRAVELER_INTENTS.CELEBRATION]: "Celebrations & Events",
});

export function territoryLabelForIntent(intent) {
  return INTENT_TERRITORY_LABELS[intent] || intent.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Remap legacy customer-facing leisure label without touching internal keys.
 */
export function normalizeCustomerTerritoryLabel(label) {
  if (label == null) return label;
  const s = String(label);
  if (s === LEGACY_LEISURE_CUSTOMER_LABEL) return LEISURE_CUSTOMER_LABEL;
  if (s.toLowerCase() === "resort leisure") return LEISURE_CUSTOMER_LABEL;
  return s;
}

/**
 * Deep-replace legacy "Resort Leisure" strings in a customer payload.
 * Does not mutate observation arrays' semantic fields beyond exact label strings.
 */
export function applyLeisureTerritoryCustomerLabelPatch(payload) {
  if (!payload || typeof payload !== "object") return payload;
  return patchLegacyLeisureLabels(payload);
}

function patchLegacyLeisureLabels(value) {
  if (typeof value === "string") {
    return normalizeCustomerTerritoryLabel(value);
  }
  if (Array.isArray(value)) {
    return value.map(patchLegacyLeisureLabels);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = patchLegacyLeisureLabels(v);
    }
    return out;
  }
  return value;
}
