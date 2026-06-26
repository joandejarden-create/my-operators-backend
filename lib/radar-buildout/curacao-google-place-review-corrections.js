/**
 * Google Places review corrections for Curaçao countrywide candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const CURACAO_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Willemstad UNESCO Historic District": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Handelskade Waterfront Promenade": {
    googleSearchQuery: "Handelskade Willemstad Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Punda Shopping and Heritage Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Mambo Beach Boulevard": {
    googleSearchQuery: "Mambo Beach Boulevard Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Jan Thiel Beach Resort Corridor": {
    googleSearchQuery: "Jan Thiel Beach Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Jan Thiel Lagoon Marina": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Blue Bay Beach": {
    googleSearchQuery: "Blue Bay Beach Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Piscadera Bay Snorkel Area": {
    googleSearchQuery: "Piscadera Bay Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Piscadera Beach Resort Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Curaçao International Airport Hotel Corridor": {
    googleSearchQuery: "Curaçao International Airport Hato",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Airport to Willemstad Transit Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Spanish Water Lagoon": {
    googleSearchQuery: "Spanish Water Curaçao lagoon",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Caracasbaai Bay": {
    googleSearchQuery: "Caracas Bay Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Spanish Water Marina": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Playa Kalki Westpunt": {
    googleSearchQuery: "Playa Kalki Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grote Knip Kenepa Beach": {
    googleSearchQuery: "Kenepa Beach Grote Knip Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Westpunt Coastal Resort Growth Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Bullenbaai Port Logistics Zone": {
    googleSearchQuery: "Bullenbaai port Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Isla Refinery Industrial Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Rif Fort": {
    latitude: 12.1055416,
    longitude: -68.9372305,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Curaçao Maritime Museum": {
    latitude: 12.1071566,
    longitude: -68.9326545,
    googleSearchQuery: "Curacao Maritime Museum Willemstad",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Curaçao Cruise Terminal Mathey Wharf": {
    latitude: 12.1085579,
    longitude: -68.934273,
    googleSearchQuery: "Mathey Wharf cruise terminal Willemstad Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Renaissance Curaçao Resort and Casino": {
    name: "Renaissance Wind Creek Curaçao Resort",
    latitude: 12.1054667,
    longitude: -68.9388484,
    googleSearchQuery: "Renaissance Wind Creek Curacao Resort",
    reviewAction: "google_canonical",
  },
  "Curaçao Sea Aquarium": {
    latitude: 12.084199,
    longitude: -68.8965639,
    city: "Bapor Kibra",
    googleSearchQuery: "Curaçao Sea Aquarium Bapor Kibra",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "LionsDive Beach Resort Area": {
    latitude: 12.0857116,
    longitude: -68.8972472,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Chogogo Party Beach": {
    googleSearchQuery: "Chogogo Party Beach Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Landhuis Chobolobo Distillery": {
    googleSearchQuery: "Landhuis Chobolobo Blue Curaçao distillery",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Livingstone Jan Thiel Resort Area": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Papagayo Beach Jan Thiel": {
    googleSearchQuery: "Papagayo Beach Resort Jan Thiel Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Zuikertuin Shopping and Entertainment": {
    googleSearchQuery: "Zuikertuin Tower Jan Thiel Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Santa Barbara Plantation Resort Zone": {
    googleSearchQuery: "Santa Barbara Plantation Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Blue Bay Golf Beach Resort": {
    googleSearchQuery: "Blue Bay Golf Beach Resort Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Blue Bay Village Retail District": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Hato Airport Business Access Zone": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "World Trade Center Curaçao": {
    latitude: 12.1185846,
    longitude: -68.9642051,
    googleSearchQuery: "World Trade Center Curaçao Willemstad",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Boka Sami Beach": {
    latitude: 12.1465488,
    longitude: -68.9988713,
    googleSearchQuery: "Boka Sami Beach Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Q Club Caracasbaai Entertainment": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Christoffel National Park": {
    latitude: 12.3504857,
    longitude: -69.1067084,
    googleSearchQuery: "Christoffel National Park Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Landhuis Daniel West Coast": {
    googleSearchQuery: "Landhuis Daniel Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Mega Pier Cruise Terminal": {
    latitude: 12.1046355,
    longitude: -68.9417393,
    googleSearchQuery: "Mega Pier cruise terminal Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "University of Curaçao": {
    latitude: 12.157384,
    longitude: -68.961038,
    googleSearchQuery: "University of Curaçao Dr Moises Da Costa Gomez",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Ergilio Hato Stadium": {
    latitude: 12.1527714,
    longitude: -68.8876774,
    googleSearchQuery: "Ergilio Hato Stadium Curaçao",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Curaçao Convention Centre": {
    latitude: 12.1185846,
    longitude: -68.9642051,
    googleSearchQuery: "World Trade Center Curaçao convention centre",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Queen Emma Pontoon Bridge": {
    googleSearchQuery: "Queen Emma Bridge Willemstad",
    reviewAction: "google_canonical",
  },
  "Curaçao Medical Center": {
    googleSearchQuery: "Curaçao Medical Center hospital",
    reviewAction: "google_canonical",
  },
};

export function applyCuracaoPlaceReviewCorrection(point) {
  const fix = CURACAO_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

export function applyCuracaoPlaceReviewCorrections(points) {
  return points.map(applyCuracaoPlaceReviewCorrection);
}
