/**
 * Google Places review corrections for Bahamas countrywide candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const BAHAMAS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Lynden Pindling International Airport Corridor": {
    googleSearchQuery: "Lynden Pindling International Airport Nassau Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Nassau Downtown Business District": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Junkanoo Beach": {
    googleSearchQuery: "Junkanoo Beach Nassau Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Festival Place Welcome Centre": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Thomas A Robinson National Stadium": {
    googleSearchQuery: "Thomas A Robinson National Stadium Nassau Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Bahamas Financial Centre": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Nassau Harbour Marina District": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Nassau Container Port Logistics Zone": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cabbage Beach": {
    googleSearchQuery: "Cabbage Beach Paradise Island Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Sir Sidney Poitier Bridge Access Corridor": {
    googleSearchQuery: "Sir Sidney Poitier Bridge Paradise Island Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Paradise Island Entertainment Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cable Beach Resort Corridor": {
    googleSearchQuery: "Cable Beach Nassau Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Goodman Bay": {
    googleSearchQuery: "Goodman Bay Nassau Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cable Beach Airport West Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Saunders Beach": {
    googleSearchQuery: "Saunders Beach Nassau Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Melía Nassau Beach Resort Zone": {
    googleSearchQuery: "Melia Nassau Beach Resort Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grand Bahama International Airport Corridor": {
    googleSearchQuery: "Grand Bahama International Airport Freeport",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Freeport Harbour Cruise Port": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "International Bazaar Freeport": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Pelican Bay Hotel District": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grand Lucayan Waterpark Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Exuma International Airport Corridor": {
    googleSearchQuery: "Exuma International Airport George Town Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "George Town Exuma Harbour": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Stocking Island": {
    googleSearchQuery: "Stocking Island Exuma Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Exuma Cays Resort Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Staniel Cay Yacht Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "North Eleuthera Airport Corridor": {
    googleSearchQuery: "North Eleuthera Airport Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Governor's Harbour Airport Access": {
    googleSearchQuery: "Governor's Harbour Airport Eleuthera Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Harbour Island Pink Sands Beach": {
    googleSearchQuery: "Pink Sands Beach Harbour Island Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Governor's Harbour Waterfront": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Marsh Harbour Airport Corridor": {
    googleSearchQuery: "Marsh Harbour Airport Abaco Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Man-O-War Cay": {
    googleSearchQuery: "Man-O-War Cay Abaco Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Treasure Cay Beach": {
    googleSearchQuery: "Treasure Cay Beach Abaco Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "South Bimini Airport Ferry Corridor": {
    googleSearchQuery: "South Bimini Airport Bahamas",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Alice Town Entertainment District": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "San Salvador Island Resort Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Prince George Wharf Cruise Port": {
    googleSearchQuery: "Prince George Wharf Nassau Bahamas",
    reviewAction: "google_canonical",
  },
  "Atlantis Paradise Island Resort Complex": {
    googleSearchQuery: "Atlantis Paradise Island Bahamas",
    reviewAction: "google_canonical",
  },
  "Baha Mar Resort Complex": {
    googleSearchQuery: "Baha Mar Nassau Bahamas",
    reviewAction: "google_canonical",
  },
  "Thunderball Grotto": {
    googleSearchQuery: "Thunderball Grotto Exuma Bahamas",
    reviewAction: "google_canonical",
  },
  "Glass Window Bridge": {
    googleSearchQuery: "Glass Window Bridge Eleuthera Bahamas",
    reviewAction: "google_canonical",
  },
  "Doctors Hospital Nassau": {
    googleSearchQuery: "Doctors Hospital Nassau Bahamas",
    reviewAction: "google_canonical",
  },
  "Rand Memorial Hospital": {
    googleSearchQuery: "Rand Memorial Hospital Freeport Bahamas",
    reviewAction: "google_canonical",
  },
  "Prince George Wharf Cruise Port": {
    name: "Prince George Wharf",
    latitude: 25.0792324,
    longitude: -77.3399657,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Queen's Staircase and Fort Fincastle": {
    name: "Queen's Staircase",
    latitude: 25.0729572,
    longitude: -77.3376087,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Arawak Cay Fish Fry": {
    name: "Arawak Cay Fish Fry",
    latitude: 25.0796897,
    longitude: -77.3586825,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Doctors Hospital Nassau": {
    name: "Doctor's Hospital",
    latitude: 25.0750121,
    longitude: -77.3329809,
    reviewAction: "google_canonical",
  },
  "Atlantis Paradise Island Resort Complex": {
    latitude: 25.0837819,
    longitude: -77.321198,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Ocean Club Golf Course": {
    name: "Ocean Club Golf Course",
    latitude: 25.08145,
    longitude: -77.2987943,
    city: "Paradise Island",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Paradise Island Marina": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "One and Only Ocean Club": {
    latitude: 25.0826632,
    longitude: -77.3104366,
    city: "Paradise Island",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Baha Mar Resort Complex": {
    latitude: 25.071,
    longitude: -77.396,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Sandals Royal Bahamian": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Lucayan National Park": {
    latitude: 26.614,
    longitude: -78.399,
    city: "Freeport",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Thunderball Grotto": {
    latitude: 24.178,
    longitude: -76.439,
    city: "Staniel Cay",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Glass Window Bridge": {
    latitude: 25.448,
    longitude: -76.571,
    city: "Gregory Town",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "French Leave Beach": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Hope Town Lighthouse": {
    googleSearchQuery: "Elbow Reef Lighthouse Hope Town Abaco Bahamas",
    latitude: 26.536,
    longitude: -76.958,
    city: "Hope Town",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Resorts World Bimini": {
    latitude: 25.768,
    longitude: -79.298,
    city: "North Bimini",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Andros West Side National Park Gateway": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyBahamasPlaceReviewCorrection(point) {
  const fix = BAHAMAS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

export function applyBahamasPlaceReviewCorrections(points) {
  return points.map(applyBahamasPlaceReviewCorrection);
}
