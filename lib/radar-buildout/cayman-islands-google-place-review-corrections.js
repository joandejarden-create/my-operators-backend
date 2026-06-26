/**
 * Google Places review corrections for Cayman Islands countrywide candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const CAYMAN_ISLANDS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Owen Roberts International Airport Corridor": {
    googleSearchQuery: "Owen Roberts International Airport Cayman Islands",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Seven Mile Beach Resort Corridor": {
    googleSearchQuery: "Seven Mile Beach Grand Cayman",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "East End Dive Resort Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "West Bay Growth Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Charles Kirkconnell International Airport": {
    googleSearchQuery: "Charles Kirkconnell International Airport",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Brac Reef Dive Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Edward Bodden Airfield": {
    googleSearchQuery: "Edward Bodden Airfield Little Cayman",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Point of Sand Beach": {
    googleSearchQuery: "Point of Sand Little Cayman",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Little Cayman Eco-Tourism Growth Node": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Camana Bay Mixed-Use District": {
    latitude: 19.3220089,
    longitude: -81.3781772,
    googleSearchQuery: "Camana Bay George Town Cayman Islands",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Starfish Point": {
    latitude: 19.356321,
    longitude: -81.2834532,
    googleSearchQuery: "Starfish Point Rum Point Cayman Islands",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Bodden Town Heritage Waterfront": {
    googleSearchQuery: "Bodden Town Cayman Islands waterfront",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Health City Cayman Islands": {
    latitude: 19.2980906,
    longitude: -81.146629,
    googleSearchQuery: "Health City Cayman Islands East End",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Islands Parliament Precinct": {
    googleSearchQuery: "Parliament of the Cayman Islands George Town",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Islands Hospital": {
    latitude: 19.289253,
    longitude: -81.3802478,
    googleSearchQuery: "Health Services Authority Hospital George Town Cayman Islands",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Brac Bluff Trail Zone": {
    googleSearchQuery: "Cayman Brac Bluff Trail",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Brac Sports Complex": {
    latitude: 19.7274007,
    longitude: -79.7753739,
    googleSearchQuery: "Cayman Brac Sports Complex",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Brac District Admin Centre": {
    googleSearchQuery: "District Administration Cayman Brac",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "South Hole Sound Anchorage": {
    googleSearchQuery: "South Hole Sound Little Cayman",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Bloody Bay Marine Park": {
    latitude: 19.6595723,
    longitude: -80.0840892,
    googleSearchQuery: "Bloody Bay Marine Park Little Cayman",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Little Cayman Dive Resort Zone": {
    googleSearchQuery: "Little Cayman dive resorts",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Pedro St. James Castle": {
    latitude: 19.2665181,
    longitude: -81.2910442,
    googleSearchQuery: "Pedro St James Castle Savannah Cayman Islands",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cayman Enterprise City Tech Corridor": {
    latitude: 19.2773508,
    longitude: -81.3683134,
    googleSearchQuery: "Cayman Enterprise City George Town",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyCaymanIslandsPlaceReviewCorrection(point) {
  const fix = CAYMAN_ISLANDS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

export function applyCaymanIslandsPlaceReviewCorrections(points) {
  return points.map(applyCaymanIslandsPlaceReviewCorrection);
}
