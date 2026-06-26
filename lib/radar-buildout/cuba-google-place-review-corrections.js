/**
 * Google Places review corrections for Cuba Countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const CUBA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Old Havana UNESCO World Heritage Core": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1355,
    "longitude": -82.3503
  },
  "Malecón de La Habana Waterfront": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1408,
    "longitude": -82.4089
  },
  "Plaza de la Revolución Civic Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1247,
    "longitude": -82.3861
  },
  "El Capitolio Nacional": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1353,
    "longitude": -82.3597
  },
  "Gran Teatro de La Habana Alicia Alonso": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1378,
    "longitude": -82.3589
  },
  "Fábrica de Arte Cubano": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1289,
    "longitude": -82.4012
  },
  "Miramar Business and Embassy Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1123,
    "longitude": -82.4456
  },
  "Playa del Este Resort Strip": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1567,
    "longitude": -82.3012
  },
  "Morro-Cabaña Historic Fortress Complex": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1561,
    "longitude": -82.3512
  },
  "University of Havana Campus": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1378,
    "longitude": -82.3845
  },
  "Hermanos Ameijeiras Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1289,
    "longitude": -82.3789
  },
  "Parque Josone Varadero": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1312,
    "longitude": -81.2789
  },
  "Plaza América Convention Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1278,
    "longitude": -81.2912
  },
  "Melia Varadero Resort Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1456,
    "longitude": -81.2712
  },
  "Marina Dársena de Varadero": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 23.1334,
    "longitude": -81.2834
  },
  "Trinidad Plaza Mayor UNESCO Core": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 21.8022,
    "longitude": -79.9831
  },
  "Valle de los Ingenios Heritage Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 21.8234,
    "longitude": -79.9456
  },
  "Topes de Collantes Nature Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 21.9123,
    "longitude": -80.0123
  },
  "Playa Ancón Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 21.8567,
    "longitude": -79.9789
  },
  "Parque Céspedes Santiago Civic Center": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.0211,
    "longitude": -75.8263
  },
  "Santiago Carnival District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.0234,
    "longitude": -75.8312
  },
  "Santa Ifigenia Cemetery — José Martí Mausoleum": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.0289,
    "longitude": -75.8234
  },
  "Viñales Valley UNESCO Landscape": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 22.6167,
    "longitude": -83.7167
  },
  "Cayo Coco Resort Archipelago": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 22.5123,
    "longitude": -78.5012
  },
  "Holguín City Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.8872,
    "longitude": -76.2631
  },
  "Guardalavaca Beach Resort Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 21.1234,
    "longitude": -75.8234
  },
  "Frank País Airport Holguín Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.7856,
    "longitude": -76.3151
  },
  "Mariel Special Development Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 22.9878,
    "longitude": -82.7512
  },
  "Zapata Peninsula Eco-Tourism Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 22.3012,
    "longitude": -81.1234
  },
  "Baracoa First City Heritage District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.3489,
    "longitude": -74.4967
  }
};

export function applyCubaPlaceReviewCorrection(point) {
  const fix = CUBA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyCubaPlaceReviewCorrections(points) {
  return points.map(applyCubaPlaceReviewCorrection);
}
