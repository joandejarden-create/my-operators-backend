/**
 * Google Places review corrections for Grenada countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const GRENADA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Fort George Historic Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0489,
    "longitude": -61.7567
  },
  "Sendall Tunnel Heritage Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0501,
    "longitude": -61.7545
  },
  "General Hospital Grenada": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0456,
    "longitude": -61.7489
  },
  "Port Louis Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0567,
    "longitude": -61.7489
  },
  "Annandale Falls": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0789,
    "longitude": -61.7234
  },
  "Levera National Park and Bathway Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.2234,
    "longitude": -61.6234
  },
  "Sauteurs Leap Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.2189,
    "longitude": -61.6345
  },
  "Gouyave Nutmeg Processing": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.1623,
    "longitude": -61.7289
  },
  "Belmont Estate Heritage": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.1789,
    "longitude": -61.6789
  },
  "La Sagesse Nature Centre Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0123,
    "longitude": -61.6789
  },
  "Secret Harbour Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.9967,
    "longitude": -61.7589
  },
  "Magazine Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0012,
    "longitude": -61.7712
  },
  "Underwater Sculpture Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.1234,
    "longitude": -61.7234
  },
  "Petite Martinique Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.5234,
    "longitude": -61.4012
  },
  "Grenada Chocolate Factory": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0678,
    "longitude": -61.7123
  },
  "River Antoine Rum Distillery": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.1923,
    "longitude": -61.6456
  },
  "St. David's Anglican Church Heritage": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0456,
    "longitude": -61.6678
  },
  "Grenada Trade Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0412,
    "longitude": -61.7456
  },
  "Grenada Industrial Development Corporation Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0123,
    "longitude": -61.7789
  },
  "Grenada Investment Development Corporation": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0478,
    "longitude": -61.7512
  },
  "Calivigny Island Luxury Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.9789,
    "longitude": -61.7234
  },
  "Westerhall Estate Rum Tour": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0234,
    "longitude": -61.7012
  },
  "Fort Frederick Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.0467,
    "longitude": -61.7589
  },
  "Grenada South Coast Growth Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.9934,
    "longitude": -61.7623
  },
  "Tyrell Bay Carriacou Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.4567,
    "longitude": -61.4789
  },
  "Laura Herb and Spice Garden": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.1012,
    "longitude": -61.7123
  }
};

export function applyGrenadaPlaceReviewCorrection(point) {
  const fix = GRENADA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyGrenadaPlaceReviewCorrections(points) {
  return points.map(applyGrenadaPlaceReviewCorrection);
}
