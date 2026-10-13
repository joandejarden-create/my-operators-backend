/**
 * Market-specific base weighting — do not research all bases equally.
 */

import { GDI_BASE_OF_DEMAND } from "./taxonomy.js";
import { V5_HOTELS } from "../opportunity-discovery-v5/hotels.js";
import { COMP_SET_TARGET_HOTELS } from "../comp-set-demand-mining-v1/index.js";

const B = GDI_BASE_OF_DEMAND;

const WEIGHTS = Object.freeze({
  YOTEL: {
    HIGH: [
      B.PUBLISHED_EVENT_DECOMPOSITION,
      B.INTERNATIONAL_ORG_RECURRING_GROUPS,
      B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
      B.HISTORIC_ROTATION_PREDICTION,
      B.RECURRING_CORPORATE_MEETINGS,
      B.CORPORATE_TRIGGER_DEMAND,
      B.PHARMA_MEDICAL_ECOSYSTEM,
      B.PROJECT_WORKFORCE_DEMAND,
    ],
    MEDIUM: [B.SPORTS_ENTERTAINMENT_PRODUCTION, B.HOTEL_HISTORY_LOOKALIKE],
  },
  AC: {
    HIGH: [
      B.PUBLISHED_EVENT_DECOMPOSITION,
      B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
      B.HISTORIC_ROTATION_PREDICTION,
      B.RECURRING_CORPORATE_MEETINGS,
      B.CORPORATE_TRIGGER_DEMAND,
      B.PHARMA_MEDICAL_ECOSYSTEM,
      B.PROJECT_WORKFORCE_DEMAND,
      B.SPORTS_ENTERTAINMENT_PRODUCTION,
      B.HOTEL_HISTORY_LOOKALIKE,
    ],
    MEDIUM: [B.INTERNATIONAL_ORG_RECURRING_GROUPS],
  },
  SPICE: {
    HIGH: [
      B.RECURRING_CORPORATE_MEETINGS,
      B.CORPORATE_TRIGGER_DEMAND,
      B.PROJECT_WORKFORCE_DEMAND,
      B.SPORTS_ENTERTAINMENT_PRODUCTION,
      B.HOTEL_HISTORY_LOOKALIKE,
    ],
    MEDIUM: [
      B.PUBLISHED_EVENT_DECOMPOSITION,
      B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
      B.HISTORIC_ROTATION_PREDICTION,
      B.PHARMA_MEDICAL_ECOSYSTEM,
      B.INTERNATIONAL_ORG_RECURRING_GROUPS,
    ],
  },
  CAMBRIDGE: {
    HIGH: [
      B.RECURRING_CORPORATE_MEETINGS,
      B.CORPORATE_TRIGGER_DEMAND,
      B.PROJECT_WORKFORCE_DEMAND,
      B.SPORTS_ENTERTAINMENT_PRODUCTION,
      B.HOTEL_HISTORY_LOOKALIKE,
    ],
    MEDIUM: [
      B.PUBLISHED_EVENT_DECOMPOSITION,
      B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
      B.HISTORIC_ROTATION_PREDICTION,
      B.PHARMA_MEDICAL_ECOSYSTEM,
      B.INTERNATIONAL_ORG_RECURRING_GROUPS,
    ],
  },
  NOW_NOW: {
    HIGH: [
      B.PUBLISHED_EVENT_DECOMPOSITION,
      B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
      B.RECURRING_CORPORATE_MEETINGS,
      B.CORPORATE_TRIGGER_DEMAND,
      B.PHARMA_MEDICAL_ECOSYSTEM,
      B.SPORTS_ENTERTAINMENT_PRODUCTION,
      B.HOTEL_HISTORY_LOOKALIKE,
    ],
    MEDIUM: [
      B.HISTORIC_ROTATION_PREDICTION,
      B.PROJECT_WORKFORCE_DEMAND,
      B.INTERNATIONAL_ORG_RECURRING_GROUPS,
    ],
  },
});

export function getHotelBaseWeighting(hotelKey) {
  const w = WEIGHTS[hotelKey] || WEIGHTS.YOTEL;
  return {
    hotelKey,
    high: [...w.HIGH],
    medium: [...w.MEDIUM],
    priorityOf(base) {
      if (w.HIGH.includes(base)) return "HIGH";
      if (w.MEDIUM.includes(base)) return "MEDIUM";
      return "LOW";
    },
  };
}

/** Merge V5 hotel config with comp-set pilot hotels. */
export function getTenBasesPilotHotels() {
  const byKey = new Map(V5_HOTELS.map((h) => [h.hotelKey, { ...h }]));
  for (const c of COMP_SET_TARGET_HOTELS || []) {
    const existing = byKey.get(c.hotelKey);
    if (existing) {
      byKey.set(c.hotelKey, {
        ...existing,
        ...c,
        competitors: c.competitors || existing.competitors,
        languages: c.languages || existing.languages,
        feederMarkets: c.feederMarkets || existing.feederMarkets,
      });
    } else {
      byKey.set(c.hotelKey, { ...c });
    }
  }
  return ["YOTEL", "AC", "SPICE", "CAMBRIDGE", "NOW_NOW"]
    .map((k) => byKey.get(k))
    .filter(Boolean);
}
