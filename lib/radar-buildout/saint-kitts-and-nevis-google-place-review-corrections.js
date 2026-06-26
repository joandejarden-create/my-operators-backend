/**
 * Google Places review corrections for Saint Kitts and Nevis countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const SAINT_KITTS_AND_NEVIS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Basseterre Central Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2956,
    "longitude": -62.7267
  },
  "Independence Square Basseterre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2945,
    "longitude": -62.7289
  },
  "St. Kitts National Museum": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2934,
    "longitude": -62.7278
  },
  "Joseph N. France General Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2912,
    "longitude": -62.7312
  },
  "Clarence Fitzroy Bryant College": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2889,
    "longitude": -62.7234
  },
  "Warner Park Cricket Stadium": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3012,
    "longitude": -62.7189
  },
  "South Frigate Bay Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2767,
    "longitude": -62.6767
  },
  "North Frigate Bay Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2812,
    "longitude": -62.6812
  },
  "St. Kitts Marriott Resort Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2801,
    "longitude": -62.6798
  },
  "Christophe Harbour Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2567,
    "longitude": -62.6234
  },
  "Sandy Point National Marine Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3567,
    "longitude": -62.8512
  },
  "Black Rocks Formation": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3678,
    "longitude": -62.7789
  },
  "Romney Manor and Caribelle Batik": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3234,
    "longitude": -62.8012
  },
  "St. Kitts Scenic Railway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2989,
    "longitude": -62.7212
  },
  "Fairview Great House and Botanical Garden": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3123,
    "longitude": -62.7456
  },
  "Pinney's Beach Nevis": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1512,
    "longitude": -62.6189
  },
  "Four Seasons Resort Nevis": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1678,
    "longitude": -62.6012
  },
  "Nevis Peak Trailhead": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1567,
    "longitude": -62.5789
  },
  "Bath Spring Historic Site Nevis": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1423,
    "longitude": -62.6267
  },
  "Alexander Hamilton Museum Nevis": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1434,
    "longitude": -62.6256
  },
  "Nevis Botanical Gardens": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1612,
    "longitude": -62.6123
  },
  "Oualie Beach Resort Nevis": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1789,
    "longitude": -62.5934
  },
  "St. Kitts Ferry Terminal": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2978,
    "longitude": -62.7245
  },
  "Carambola Beach Club": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3456,
    "longitude": -62.8123
  },
  "Turtle Beach St. Kitts": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2512,
    "longitude": -62.6567
  },
  "South East Peninsula Growth Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2456,
    "longitude": -62.6489
  },
  "St. Kitts Economic Citizenship Investment Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2923,
    "longitude": -62.7256
  },
  "Kennedy Avenue Entertainment District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2941,
    "longitude": -62.7298
  },
  "Dieppe Bay Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.4123,
    "longitude": -62.8234
  },
  "St. Kitts Industrial Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2867,
    "longitude": -62.7345
  },
  "St. Kitts Convention Centre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.2798,
    "longitude": -62.6778
  },
  "Mount Liamuiga Volcano Trail": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.3789,
    "longitude": -62.8012
  },
  "Nevis Heritage Trail Charlestown": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.1445,
    "longitude": -62.6245
  }
};

export function applySaintKittsAndNevisPlaceReviewCorrection(point) {
  const fix = SAINT_KITTS_AND_NEVIS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applySaintKittsAndNevisPlaceReviewCorrections(points) {
  return points.map(applySaintKittsAndNevisPlaceReviewCorrection);
}
