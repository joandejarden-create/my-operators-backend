/**
 * Google Places review corrections for Turks & Caicos countrywide candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const TURKS_AND_CAICOS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Providenciales International Airport Corridor": {
    googleSearchQuery: "Providenciales International Airport",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grace Bay Beach Resort Corridor": {
    googleSearchQuery: "Grace Bay Beach Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Sapodilla Bay": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Providenciales Resort Expansion Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "JAGS McCartney International Airport": {
    googleSearchQuery: "JAGS McCartney International Airport",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Governor's Beach": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "North Caicos Ferry Gateway": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "South Caicos Airport Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Leeward Marina and Yacht Basin": {
    latitude: 21.8153147,
    longitude: -72.1624611,
    googleSearchQuery: "Leeward Marina Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "The Bight and Turtle Cove Marina": {
    latitude: 21.7852323,
    longitude: -72.227506,
    googleSearchQuery: "Turtle Cove Marina Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Long Bay Beach": {
    latitude: 21.7708411,
    longitude: -72.1639269,
    googleSearchQuery: "Long Bay Beach Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Chalk Sound National Park": {
    latitude: 21.7690533,
    longitude: -72.2978297,
    googleSearchQuery: "Chalk Sound National Park Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Taylor Bay": {
    latitude: 21.7493493,
    longitude: -72.2952215,
    googleSearchQuery: "Taylor Bay Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Blue Haven Marina District": {
    latitude: 21.8183583,
    longitude: -72.1472649,
    googleSearchQuery: "Blue Haven Marina Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grace Bay Retail and Dining District": {
    latitude: 21.7950432,
    longitude: -72.1791643,
    googleSearchQuery: "Grace Bay Providenciales restaurants",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "South Dock Logistics Zone": {
    googleSearchQuery: "South Dock Providenciales Turks and Caicos",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cockburn Town Road Business Corridor": {
    googleSearchQuery: "Leeward Highway Providenciales business district",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Turks and Caicos Hospital Providenciales": {
    googleSearchQuery: "Cheshire Hall Medical Centre Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "TCI Community College Providenciales Campus": {
    latitude: 21.7795416,
    longitude: -72.2530243,
    googleSearchQuery: "Turks and Caicos Islands Community College Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "National Stadium Providenciales": {
    latitude: 21.7738487,
    longitude: -72.216277,
    googleSearchQuery: "TCIFA National Stadium Providenciales",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grand Turk Cruise Center": {
    latitude: 21.4674584,
    longitude: -71.1389101,
    googleSearchQuery: "Grand Turk Cruise Center",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cockburn Town Heritage District": {
    latitude: 21.4688811,
    longitude: -71.1457927,
    googleSearchQuery: "Cockburn Town Grand Turk heritage district",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grand Turk Lighthouse": {
    latitude: 21.5118515,
    longitude: -71.1329985,
    googleSearchQuery: "Grand Turk Lighthouse",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Salt Raking Historic Sites Corridor": {
    googleSearchQuery: "Salt raking heritage Grand Turk Turks and Caicos",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Columbus Landfall Marine Zone": {
    googleSearchQuery: "Columbus Landfall National Park Grand Turk",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grand Turk Civic and Government Precinct": {
    latitude: 21.4656363,
    longitude: -71.1462802,
    googleSearchQuery: "Government offices Cockburn Town Grand Turk",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Middle Caicos Mudjin Harbour": {
    latitude: 21.8362658,
    longitude: -71.8135124,
    googleSearchQuery: "Mudjin Harbour Middle Caicos",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "South Caicos Fishing Port District": {
    latitude: 21.5112365,
    longitude: -71.51898,
    googleSearchQuery: "South Caicos fishing port Cockburn Harbour",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Salt Cay Heritage Waterfront": {
    latitude: 21.3331516,
    longitude: -71.2056228,
    googleSearchQuery: "Salt Cay Turks and Caicos waterfront",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Parrot Cay Luxury Resort Zone": {
    googleSearchQuery: "COMO Parrot Cay North Caicos",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyTurksAndCaicosPlaceReviewCorrection(point) {
  const fix = TURKS_AND_CAICOS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

export function applyTurksAndCaicosPlaceReviewCorrections(points) {
  return points.map(applyTurksAndCaicosPlaceReviewCorrection);
}
