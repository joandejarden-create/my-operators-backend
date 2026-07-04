/**
 * Google Places review corrections for British Virgin Islands countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const BRITISH_VIRGIN_ISLANDS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Wickham's Cay Marina District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4212,
    "longitude": -64.6156
  },
  "Soper's Hole Marina West End": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4123,
    "longitude": -64.6678
  },
  "Nanny Cay Marina and Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3789,
    "longitude": -64.6234
  },
  "Smuggler's Cove Beach": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4567,
    "longitude": -64.6789
  },
  "Long Bay Beach Tortola": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4456,
    "longitude": -64.5678
  },
  "J.R. O'Neal Botanic Gardens": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4289,
    "longitude": -64.6123
  },
  "Peebles Hospital": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4256,
    "longitude": -64.6234
  },
  "Oil Nut Bay Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4789,
    "longitude": -64.4012
  },
  "Jost Van Dyke Great Harbour": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4456,
    "longitude": -64.7512
  },
  "White Bay Beach Jost Van Dyke": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4389,
    "longitude": -64.7567
  },
  "Foxy's Tamarind Bar District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4467,
    "longitude": -64.7523
  },
  "Anegada Reef Hotel Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.7234,
    "longitude": -64.3234
  },
  "Anegada Flamingo Pond": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.7123,
    "longitude": -64.3345
  },
  "Loblolly Bay Anegada": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.7456,
    "longitude": -64.3123
  },
  "Anegada Airport Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.7272,
    "longitude": -64.3296
  },
  "Peter Island Resort Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3512,
    "longitude": -64.6234
  },
  "Norman Island Pirate Bight": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3234,
    "longitude": -64.6123
  },
  "BVI Finance Business Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4278,
    "longitude": -64.6212
  },
  "Tortola Yacht Charter Hub": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4251,
    "longitude": -64.6178
  },
  "BVI Convention Centre Road Town": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4267,
    "longitude": -64.6198
  },
  "Tortola Cruise Pier Expansion Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4239,
    "longitude": -64.6167
  },
  "East End Tortola Growth Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4478,
    "longitude": -64.5612
  },
  "BVI Port Authority Logistics Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4223,
    "longitude": -64.6145
  },
  "Rhone Marine Park Dive Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.3678,
    "longitude": -64.5345
  },
  "Sage Mountain National Park": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4567,
    "longitude": -64.6012
  },
  "Treasure Point Resort Growth": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 18.4412,
    "longitude": -64.5789
  }
};

export function applyBritishVirginIslandsPlaceReviewCorrection(point) {
  const fix = BRITISH_VIRGIN_ISLANDS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyBritishVirginIslandsPlaceReviewCorrections(points) {
  return points.map(applyBritishVirginIslandsPlaceReviewCorrection);
}
