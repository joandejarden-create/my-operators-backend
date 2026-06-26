/**
 * Google Places review corrections for Trinidad and Tobago countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const TRINIDAD_AND_TOBAGO_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Port of Spain Waterfront District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6512,
    "longitude": -61.5123
  },
  "Hyatt Regency Port of Spain Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6523,
    "longitude": -61.5101
  },
  "Eric Williams Medical Sciences Complex": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6412,
    "longitude": -61.4012
  },
  "University of Trinidad and Tobago": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.4123,
    "longitude": -61.4567
  },
  "International Financial Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6567,
    "longitude": -61.5067
  },
  "Chaguaramas Boardwalk and Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6789,
    "longitude": -61.6234
  },
  "Point Lisas Industrial Estate": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.4012,
    "longitude": -61.4678
  },
  "San Fernando City Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.2789,
    "longitude": -61.4567
  },
  "Pitch Lake La Brea": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.2345,
    "longitude": -61.6234
  },
  "Caroni Bird Sanctuary": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6012,
    "longitude": -61.4567
  },
  "Mount St. Benedict Monastery": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6567,
    "longitude": -61.3789
  },
  "Scarborough Tobago Civic Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.1812,
    "longitude": -60.7345
  },
  "Store Bay Tobago": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.1512,
    "longitude": -60.8389
  },
  "Buccoo Reef and Nylon Pool": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.1789,
    "longitude": -60.8234
  },
  "Tobago Plantations Beach Resort Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.1923,
    "longitude": -60.7789
  },
  "Main Ridge Forest Reserve Tobago": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.2678,
    "longitude": -60.5789
  },
  "Argyle Waterfall Tobago": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.2567,
    "longitude": -60.6012
  },
  "Port of Spain Convention Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6556,
    "longitude": -61.5078
  },
  "Trinidad Hilton Conference Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6623,
    "longitude": -61.5167
  },
  "Gulf City Shopping and Entertainment": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.2812,
    "longitude": -61.4512
  },
  "InvesTT Business Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6539,
    "longitude": -61.5098
  },
  "Port of Spain Cruise Ship Complex": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6498,
    "longitude": -61.5134
  },
  "Moka Trinidad Resort Growth Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.7012,
    "longitude": -61.5234
  },
  "Tobago Eco Resort Growth Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 11.1612,
    "longitude": -60.8312
  },
  "Lady Young Road Entertainment Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 10.6589,
    "longitude": -61.5212
  }
};

export function applyTrinidadAndTobagoPlaceReviewCorrection(point) {
  const fix = TRINIDAD_AND_TOBAGO_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyTrinidadAndTobagoPlaceReviewCorrections(points) {
  return points.map(applyTrinidadAndTobagoPlaceReviewCorrection);
}
