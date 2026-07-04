/**
 * Google Places review corrections for Haiti Countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const HAITI_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Champ de Mars Civic Plaza": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5434,
    "longitude": -72.3378
  },
  "Iron Market Marché en Fer": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5445,
    "longitude": -72.3356
  },
  "Pétion-Ville Business and Dining Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5123,
    "longitude": -72.2845
  },
  "Karibe Convention Center": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5089,
    "longitude": -72.2812
  },
  "Université d'État d'Haïti": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5389,
    "longitude": -72.3345
  },
  "Hôpital Universitaire de la Paix": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5312,
    "longitude": -72.3012
  },
  "Musée du Panthéon National Haïtien": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5467,
    "longitude": -72.3367
  },
  "Tabarre Industrial and Logistics Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5712,
    "longitude": -72.2678
  },
  "Kenscoff Mountain Retreat Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4512,
    "longitude": -72.1989
  },
  "Cap-Haïtien Historic Waterfront": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.7589,
    "longitude": -72.2012
  },
  "Bassin Bleu Waterfall Attraction": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3234,
    "longitude": -72.4123
  },
  "Cap-Haïtien Cathedral and Central Square": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.7598,
    "longitude": -72.2034
  },
  "Fort Picolet Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.7712,
    "longitude": -72.1912
  },
  "Jacmel Carnival Arts Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.2367,
    "longitude": -72.5334
  },
  "Jacmel Beach Waterfront": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.2289,
    "longitude": -72.5412
  },
  "Bassin Bleu Jacmel Eco-Attraction": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.2512,
    "longitude": -72.5234
  },
  "Cyvadier Beach Resort Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.2189,
    "longitude": -72.5489
  },
  "Port-de-Paix North Coast Gateway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.9389,
    "longitude": -72.8312
  },
  "Les Cayes South Coast Hub": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.1934,
    "longitude": -73.7456
  },
  "Port-Salut Beach Tourism Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.0789,
    "longitude": -73.9234
  },
  "Île-à-Vache Island Resort Destination": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.0712,
    "longitude": -73.6912
  },
  "Gonaïves Independence Heritage City": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4512,
    "longitude": -72.6889
  },
  "Jérémie South Peninsula Gateway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.6512,
    "longitude": -74.1234
  },
  "Côte des Arcadins Resort Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.1012,
    "longitude": -72.7012
  },
  "Wahoo Bay Beach Resort Area": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.1912,
    "longitude": -72.5234
  },
  "Saut-d'Eau Pilgrimage and Tourism Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.8234,
    "longitude": -72.2189
  },
  "Fort Jacques Mountain Heritage Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4389,
    "longitude": -72.2123
  },
  "Ouanaminthe Border Commerce Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.5489,
    "longitude": -71.7234
  },
  "Hinche Central Plateau Hub": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.1512,
    "longitude": -72.0167
  },
  "Port-au-Prince Convention and NGO District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.5189,
    "longitude": -72.2889
  },
  "Tortuga Island Heritage Access": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 20.0289,
    "longitude": -72.7912
  }
};

export function applyHaitiPlaceReviewCorrection(point) {
  const fix = HAITI_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyHaitiPlaceReviewCorrections(points) {
  return points.map(applyHaitiPlaceReviewCorrection);
}
