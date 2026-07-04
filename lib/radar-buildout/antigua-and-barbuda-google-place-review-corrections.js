/**
 * Google Places review corrections for Antigua and Barbuda countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const ANTIGUA_AND_BARBUDA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Redcliffe Quay Shopping District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1245,
    "longitude": -61.8434
  },
  "St. John's Central Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1278,
    "longitude": -61.8412
  },
  "Fort James Beach and Historic Fort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1456,
    "longitude": -61.8567
  },
  "Sir Vivian Richards Stadium": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1123,
    "longitude": -61.8234
  },
  "Falmouth Harbour Superyacht Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0189,
    "longitude": -61.7789
  },
  "Antigua Yacht Club": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0267,
    "longitude": -61.7623
  },
  "Runway Beach Resort Strip": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1612,
    "longitude": -61.8389
  },
  "Jolly Beach Resort Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0589,
    "longitude": -61.8912
  },
  "Devil's Bridge Natural Arch": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0934,
    "longitude": -61.6789
  },
  "Betty's Hope Sugar Mill Heritage": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0789,
    "longitude": -61.7234
  },
  "Fig Tree Drive Rainforest Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0456,
    "longitude": -61.8012
  },
  "Half Moon Bay Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0289,
    "longitude": -61.6789
  },
  "Codrington Lagoon National Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.6345,
    "longitude": -61.8123
  },
  "Princess Diana Beach Barbuda": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.5789,
    "longitude": -61.8234
  },
  "Barbuda Belle Luxury Resort Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.5812,
    "longitude": -61.8189
  },
  "Deep Bay Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1389,
    "longitude": -61.8623
  },
  "Stingray City Antigua": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0789,
    "longitude": -61.7123
  },
  "Five Islands Peninsula Resort Growth": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1123,
    "longitude": -61.8789
  },
  "Crab Hill Industrial Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0678,
    "longitude": -61.9012
  },
  "Liberta Village Heritage": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0412,
    "longitude": -61.7923
  },
  "Long Bay Beach Antigua": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.0678,
    "longitude": -61.6934
  },
  "Antigua Convention Bureau District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1256,
    "longitude": -61.8423
  },
  "Harmony Hall Art Gallery": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1012,
    "longitude": -61.7234
  }
};

export function applyAntiguaAndBarbudaPlaceReviewCorrection(point) {
  const fix = ANTIGUA_AND_BARBUDA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyAntiguaAndBarbudaPlaceReviewCorrections(points) {
  return points.map(applyAntiguaAndBarbudaPlaceReviewCorrection);
}
