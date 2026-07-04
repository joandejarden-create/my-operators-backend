/**
 * Google Places review corrections for U.S. Virgin Islands Countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const US_VIRGIN_ISLANDS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Charlotte Amalie Historic District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3412,
    "longitude": -64.9312
  },
  "Main Street Duty-Free Shopping Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3423,
    "longitude": -64.9334
  },
  "Sapphire Beach Resort Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3234,
    "longitude": -64.8567
  },
  "Coki Beach and Coral World": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3512,
    "longitude": -64.8678
  },
  "University of the Virgin Islands — St. Thomas": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3389,
    "longitude": -64.9812
  },
  "Schneider Regional Medical Center": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3456,
    "longitude": -64.9456
  },
  "Emancipation Garden Civic Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3401,
    "longitude": -64.9345
  },
  "Yacht Haven Grande Marina": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3356,
    "longitude": -64.9267
  },
  "Drake's Seat Scenic Overlook": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3612,
    "longitude": -64.9123
  },
  "Fort Christiansvaern Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7478,
    "longitude": -64.7012
  },
  "Buck Island Reef National Monument": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7867,
    "longitude": -64.6234
  },
  "Sandy Point National Wildlife Refuge": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.6734,
    "longitude": -64.8912
  },
  "University of the Virgin Islands — St. Croix": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7234,
    "longitude": -64.7512
  },
  "Juan F. Luis Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7312,
    "longitude": -64.7589
  },
  "Hovensa Industrial Heritage Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.6989,
    "longitude": -64.7612
  },
  "Point Udall — Easternmost U.S. Point": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7567,
    "longitude": -64.5667
  },
  "Cinnamon Bay Beach Campground": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3567,
    "longitude": -64.7512
  },
  "Maho Bay Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3612,
    "longitude": -64.7456
  },
  "Coral Bay Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3234,
    "longitude": -64.7234
  },
  "Estate Whim Plantation Museum": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7012,
    "longitude": -64.8234
  },
  "Cane Bay Beach Dive Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7634,
    "longitude": -64.7912
  },
  "Water Island Ferry Access": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3189,
    "longitude": -64.9567
  },
  "St. Thomas Skyride to Paradise Point": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3389,
    "longitude": -64.9389
  },
  "Bolongo Bay Beach Resort Area": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3012,
    "longitude": -64.8912
  },
  "Frenchtown Dining and Marina District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3334,
    "longitude": -64.9412
  },
  "Renaissance St. Croix Convention Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7512,
    "longitude": -64.7089
  },
  "Tutu Park Mall Commercial Hub": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3489,
    "longitude": -64.9612
  },
  "Hull Bay Surf and Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3712,
    "longitude": -64.9567
  },
  "Salt River Bay National Historical Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7789,
    "longitude": -64.7612
  },
  "Annaberg Plantation Ruins": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3612,
    "longitude": -64.7289
  },
  "St. Thomas Legislature and Government District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3434,
    "longitude": -64.9289
  },
  "St. Croix Renaissance Park Growth Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 17.7189,
    "longitude": -64.7489
  }
};

export function applyUsVirginIslandsPlaceReviewCorrection(point) {
  const fix = US_VIRGIN_ISLANDS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyUsVirginIslandsPlaceReviewCorrections(points) {
  return points.map(applyUsVirginIslandsPlaceReviewCorrection);
}
