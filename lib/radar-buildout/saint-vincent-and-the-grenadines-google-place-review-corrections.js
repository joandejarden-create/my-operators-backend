/**
 * Google Places review corrections for Saint Vincent and the Grenadines countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const SAINT_VINCENT_AND_THE_GRENADINES_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Kingstown Central Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1589,
    "longitude": -61.2267
  },
  "Botanic Gardens St. Vincent": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1612,
    "longitude": -61.2289
  },
  "Fort Charlotte Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1623,
    "longitude": -61.2312
  },
  "Milton Cato Memorial Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1545,
    "longitude": -61.2189
  },
  "St. Vincent and the Grenadines Community College": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1523,
    "longitude": -61.2212
  },
  "Arnos Vale Sports Complex": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1489,
    "longitude": -61.2156
  },
  "Villa Beach Resort Strip": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1456,
    "longitude": -61.2012
  },
  "Indian Bay Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1423,
    "longitude": -61.1989
  },
  "Young Island Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1389,
    "longitude": -61.2123
  },
  "Owia Salt Pond": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.3678,
    "longitude": -61.1234
  },
  "Dark View Falls": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.3123,
    "longitude": -61.1567
  },
  "Falls of Baleine": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.3789,
    "longitude": -61.1456
  },
  "Bequia Admiralty Bay Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.0089,
    "longitude": -61.2412
  },
  "Princess Margaret Beach Bequia": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.0067,
    "longitude": -61.2389
  },
  "Canouan Island Resort Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.7012,
    "longitude": -61.3234
  },
  "Union Island Clifton Harbour": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.5989,
    "longitude": -61.4234
  },
  "Palm Island Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.5789,
    "longitude": -61.3912
  },
  "Mayreau Salt Whistle Bay": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.6456,
    "longitude": -61.3912
  },
  "Petit St. Vincent Private Island Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 12.5234,
    "longitude": -61.3789
  },
  "Chateaubelair Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.2912,
    "longitude": -61.2456
  },
  "Barrouallie Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.2234,
    "longitude": -61.2678
  },
  "Layou Petroglyph Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.2123,
    "longitude": -61.2567
  },
  "Buccament Bay Resort Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1789,
    "longitude": -61.2678
  },
  "Georgetown Market District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.2789,
    "longitude": -61.1234
  },
  "Mesopotamia Valley Agriculture Tourism": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.2012,
    "longitude": -61.2123
  },
  "Invest SVG Business Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1578,
    "longitude": -61.2245
  },
  "Kingstown Market Vendors District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1598,
    "longitude": -61.2278
  },
  "Blue Lagoon Marina St. Vincent": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1345,
    "longitude": -61.1934
  },
  "Grenadines Yacht Charter Hub": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.0112,
    "longitude": -61.2434
  },
  "St. Vincent Conference Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1556,
    "longitude": -61.2201
  },
  "Calliaqua Industrial Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1289,
    "longitude": -61.1912
  },
  "Wallilabou Bay Pirates of the Caribbean Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.2567,
    "longitude": -61.2567
  },
  "Argyle South Coast Growth Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 13.1512,
    "longitude": -61.1489
  }
};

export function applySaintVincentAndTheGrenadinesPlaceReviewCorrection(point) {
  const fix = SAINT_VINCENT_AND_THE_GRENADINES_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applySaintVincentAndTheGrenadinesPlaceReviewCorrections(points) {
  return points.map(applySaintVincentAndTheGrenadinesPlaceReviewCorrection);
}
