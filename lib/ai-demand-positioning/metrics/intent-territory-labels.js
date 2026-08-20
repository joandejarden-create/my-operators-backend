/**
 * Customer-safe demand territory labels — governed from scenario registry intents.
 * Do not invent categories outside TRAVELER_INTENTS.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";

export const INTENT_TERRITORY_LABELS = Object.freeze({
  [TRAVELER_INTENTS.BUSINESS]: "Business Travel",
  [TRAVELER_INTENTS.LEISURE]: "Resort Leisure",
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
