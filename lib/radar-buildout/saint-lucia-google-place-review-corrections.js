/**
 * Google Places review corrections for Saint Lucia countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const SAINT_LUCIA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Castries Central Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0101,
    "longitude": -60.9895
  },
  "Duty Free Pointe Seraphine": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0139,
    "longitude": -60.9878
  },
  "OECS Commission Headquarters Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0078,
    "longitude": -60.9934
  },
  "Owen King EU Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0056,
    "longitude": -60.9789
  },
  "Sir Arthur Lewis Community College": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0189,
    "longitude": -60.9812
  },
  "Daren Sammy Cricket Ground": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0689,
    "longitude": -60.9534
  },
  "Pigeon Island National Landmark": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0923,
    "longitude": -60.9612
  },
  "Gros Islet Friday Night Street Party": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0798,
    "longitude": -60.9456
  },
  "Cap Estate Golf and Resort Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.1012,
    "longitude": -60.9478
  },
  "Atlantic Rally for Cruisers Finish": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0756,
    "longitude": -60.9523
  },
  "Soufrière Town Waterfront": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8567,
    "longitude": -61.0567
  },
  "Sulphur Springs Drive-In Volcano": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8412,
    "longitude": -61.0456
  },
  "Diamond Falls Botanical Gardens": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8534,
    "longitude": -61.0512
  },
  "Anse Chastanet Resort Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8623,
    "longitude": -61.0789
  },
  "Sugar Beach Pitons View Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8645,
    "longitude": -61.0756
  },
  "Fond Doux Plantation Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8489,
    "longitude": -61.0389
  },
  "Vieux Fort Industrial Free Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.7289,
    "longitude": -60.9612
  },
  "Micoud Bay Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8123,
    "longitude": -60.9012
  },
  "Dennery Fishing Village East Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.9012,
    "longitude": -60.8934
  },
  "Choiseul Heritage Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.7723,
    "longitude": -61.0456
  },
  "Laborie Bay South Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.7512,
    "longitude": -60.9934
  },
  "Canaries Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.9012,
    "longitude": -61.0623
  },
  "Saint Lucia Conference Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0723,
    "longitude": -60.9534
  },
  "Baywalk Shopping Plaza Rodney Bay": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0767,
    "longitude": -60.9501
  },
  "Windward Islands Research and Education Foundation": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.8578,
    "longitude": -61.0545
  },
  "Rodney Bay Resort Expansion Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0812,
    "longitude": -60.9478
  },
  "Castries Waterfront Redevelopment": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0098,
    "longitude": -60.9889
  },
  "Anse La Raye Fish Fry": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.9456,
    "longitude": -61.0389
  },
  "Babonneau Community Tourism Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0123,
    "longitude": -60.9234
  },
  "Ti Rocher Rainforest Adventure Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.9789,
    "longitude": -60.9678
  },
  "Saint Lucia National Stadium Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 14.0234,
    "longitude": -60.9712
  }
};

export function applySaintLuciaPlaceReviewCorrection(point) {
  const fix = SAINT_LUCIA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applySaintLuciaPlaceReviewCorrections(points) {
  return points.map(applySaintLuciaPlaceReviewCorrection);
}
