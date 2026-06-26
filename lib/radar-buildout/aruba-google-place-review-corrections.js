/**
 * Google Places review corrections for Aruba countrywide candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const ARUBA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Palm Beach High-Rise Resort Corridor": {
    googleSearchQuery: "Palm Beach Aruba high rise hotels",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Palm Beach Boardwalk": {
    googleSearchQuery: "Palm Beach Boardwalk Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Palm Beach Casino Entertainment Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Divi Phoenix Palm Beach Zone": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Holiday Inn Resort Aruba Beach Zone": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Palm Beach Water Sports Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Eagle Beach Low-Rise Resort Corridor": {
    googleSearchQuery: "Eagle Beach Aruba resort hotels",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Manchebo Beach": {
    googleSearchQuery: "Manchebo Beach Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Amsterdam Manor Beach Zone": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Eagle Beach Public Beach Access": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Eagle Beach Events and Festival Zone": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Oranjestad Downtown Shopping Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Wilhelmina Park": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Parliament of Aruba": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Noord Commercial Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Boca Catalina Snorkel Beach": {
    googleSearchQuery: "Boca Catalina Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Noord Hotel Growth Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "San Nicolas Art and Culture District": {
    googleSearchQuery: "San Nicolas murals Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Baby Beach": {
    googleSearchQuery: "Baby Beach Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Rodgers Beach": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Seroe Colorado Growth Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Queen Beatrix International Airport Corridor": {
    googleSearchQuery: "Queen Beatrix International Airport Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Arashi Beach": {
    googleSearchQuery: "Arashi Beach Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Malmok Beach": {
    googleSearchQuery: "Malmok Beach Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Arashi Reef Snorkel Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Port of Aruba Cruise Terminal": {
    googleSearchQuery: "Port of Aruba cruise terminal Oranjestad",
    reviewAction: "google_canonical",
  },
  "Renaissance Convention Center Aruba": {
    googleSearchQuery: "Renaissance Convention Center Aruba",
    reviewAction: "google_canonical",
  },
  "California Lighthouse": {
    googleSearchQuery: "California Lighthouse Aruba",
    reviewAction: "google_canonical",
  },
  "Fort Zoutman and Willem III Tower": {
    googleSearchQuery: "Fort Zoutman Oranjestad Aruba",
    reviewAction: "google_canonical",
  },
  "Dr. Horacio Oduber Hospital": {
    googleSearchQuery: "Dr Horacio Oduber Hospital Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Palm Beach Plaza Entertainment District": {
    name: "Palm Beach Plaza Mall",
    latitude: 12.5741587,
    longitude: -70.0426636,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Ritz-Carlton Aruba": {
    name: "The Ritz-Carlton Aruba",
    latitude: 12.5831193,
    longitude: -70.043683,
    city: "Noord",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Paseo Herencia Entertainment District": {
    name: "Paseo Herencia Mall",
    latitude: 12.5741389,
    longitude: -70.0438694,
    city: "Noord",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Tropicana Aruba Resort Zone": {
    latitude: 12.5510715,
    longitude: -70.0531967,
    city: "Eagle Beach",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Bubali Bird Sanctuary": {
    latitude: 12.5617003,
    longitude: -70.0482337,
    city: "Noord",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Fort Zoutman and Willem III Tower": {
    latitude: 12.517757,
    longitude: -70.035697,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Aruba Aloe Factory and Museum": {
    name: "Aruba Aloe Factory Museum and Store",
    latitude: 12.541061,
    longitude: -70.037567,
    city: "Oranjestad",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Alto Vista Chapel": {
    latitude: 12.5759481,
    longitude: -70.0109732,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Balashi Gold Mill Ruins": {
    name: "Balashi Gold Mills",
    latitude: 12.4834805,
    longitude: -69.973045,
    city: "Balashi",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Barcadera Port Logistics Zone": {
    name: "Port of Barcadera",
    latitude: 12.4799061,
    longitude: -69.9971581,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Port of Barcadera": {
    latitude: 12.4799061,
    longitude: -69.9971581,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Arikok National Park Main Gateway": {
    googleSearchQuery: "Arikok National Park Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Natural Pool Conchi": {
    googleSearchQuery: "Natural Pool Conchi Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "De Palm Island": {
    googleSearchQuery: "De Palm Island Aruba",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyArubaPlaceReviewCorrection(point) {
  const fix = ARUBA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;

  let notes = String(point.notes || "");
  if (!notes.includes(REVIEW_TAG)) notes = `${notes} ${REVIEW_TAG}`.trim();
  if (fix.manuallyVerified && !/Manually verified using official/i.test(notes)) {
    notes = `${notes} Manually verified using official/public source; Google Maps match was not used as final authority.`;
  }

  return {
    ...point,
    name: fix.name ?? point.name,
    latitude: fix.latitude ?? point.latitude,
    longitude: fix.longitude ?? point.longitude,
    city: fix.city ?? point.city,
    submarket: fix.submarket ?? point.submarket,
    googleSearchQuery: fix.googleSearchQuery ?? point.googleSearchQuery,
    dataConfidence: fix.dataConfidence || (fix.manuallyVerified ? "High" : point.dataConfidence || "High"),
    manuallyVerified: fix.manuallyVerified === true || point.manuallyVerified,
    notes,
  };
}

export function applyArubaPlaceReviewCorrections(points) {
  return points.map(applyArubaPlaceReviewCorrection);
}
