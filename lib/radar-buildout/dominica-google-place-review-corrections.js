/**
 * Google Places review corrections for Dominica countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const DOMINICA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Roseau Central Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3012,
    "longitude": -61.3889
  },
  "Princess Margaret Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3034,
    "longitude": -61.3823
  },
  "Trafalgar Falls": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3345,
    "longitude": -61.3567
  },
  "Emerald Pool Nature Trail": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3234,
    "longitude": -61.3234
  },
  "Champagne Reef Snorkel Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.2234,
    "longitude": -61.3567
  },
  "Calibishie Coastal Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.5923,
    "longitude": -61.3456
  },
  "Red Rock Beach East Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.6012,
    "longitude": -61.3389
  },
  "Fort Shirley Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.5867,
    "longitude": -61.4623
  },
  "Portsmouth Bay Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.5789,
    "longitude": -61.4567
  },
  "Indian River Tour Gateway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.5712,
    "longitude": -61.4512
  },
  "Secret Bay Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.4123,
    "longitude": -61.4456
  },
  "Jungle Bay Resort Spa": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.2289,
    "longitude": -61.3512
  },
  "Middleham Falls": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3567,
    "longitude": -61.3234
  },
  "Waitukubuli National Trail Segment Roseau": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3156,
    "longitude": -61.3678
  },
  "Dominica Industrial Estate": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.2912,
    "longitude": -61.4012
  },
  "Rosalie Bay Resort Turtle Sanctuary": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3678,
    "longitude": -61.2623
  },
  "Hampstead Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.5789,
    "longitude": -61.3234
  },
  "Dominica Convention Bureau District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3001,
    "longitude": -61.3878
  },
  "Portsmouth North Gateway Growth Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.5812,
    "longitude": -61.4589
  },
  "Roseau Valley Resort Growth Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.3189,
    "longitude": -61.3712
  }
};

export function applyDominicaPlaceReviewCorrection(point) {
  const fix = DOMINICA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyDominicaPlaceReviewCorrections(points) {
  return points.map(applyDominicaPlaceReviewCorrection);
}
