/**
 * Google Places review corrections for Mexico — Cancún / Riviera Maya candidates.
 * Keys match pre-review candidate names.
 */

/** @type {Record<string, object>} */
export const MEXICO_CANCUN_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Cancún Center Convention Complex": {
    name: "Cancun Center",
    latitude: 21.1347875,
    longitude: -86.7468712,
    googleSearchQuery: "Cancun Center Convention Center Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Playa Delfines": {
    name: "Playa Delfines",
    googleSearchQuery: "Playa Delfines Cancún Quintana Roo Mexico",
    latitude: 21.0623358,
    longitude: -86.7787514,
    reviewAction: "search_query",
  },
  "Forum by the Sea Entertainment Complex": {
    name: "Forum By The Sea",
    latitude: 21.1323639,
    longitude: -86.7471389,
    reviewAction: "google_canonical",
  },
  "Plaza Kukulcán Commercial Corridor": {
    name: "Kukulcan Plaza",
    latitude: 21.1023779,
    longitude: -86.7653465,
    googleSearchQuery: "Kukulcan Plaza Cancún Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Puerto Cancún Marina & Golf": {
    name: "Marina Puerto Cancún",
    latitude: 21.1608034,
    longitude: -86.8079775,
    googleSearchQuery: "Marina Puerto Cancún Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Parque Tarja Puerto Cancún": {
    name: "Marina Town Center Puerto Cancún",
    latitude: 21.1725,
    longitude: -86.8033,
    googleSearchQuery: "Marina Town Center Puerto Cancún Mexico",
    reviewAction: "rename_search",
  },
  "Playa Mujeres Beach Resort Corridor": {
    name: "Playa Mujeres",
    latitude: 21.2811059,
    longitude: -86.8200595,
    googleSearchQuery: "Playa Mujeres Costa Mujeres Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Costa Mujeres Tourism Development Corridor": {
    name: "Costa Mujeres Tourism Corridor",
    latitude: 21.2811059,
    longitude: -86.8200595,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Quinta Avenida Playa del Carmen": {
    name: "Quinta Avenida",
    latitude: 20.6291523,
    longitude: -87.0705708,
    reviewAction: "google_canonical",
  },
  "Parque Fundadores Playa del Carmen": {
    name: "Parque Los Fundadores",
    latitude: 20.6220415,
    longitude: -87.0750064,
    reviewAction: "google_canonical",
  },
  "Centro de Convenciones Riviera Maya": {
    name: "Centro de Convenciones Riviera Maya",
    latitude: 20.6158,
    longitude: -87.0892,
    manuallyVerified: true,
    dataConfidence: "High",
    reviewAction: "manual_corridor",
  },
  "Playacar Resort & Business District": {
    name: "Playacar",
    latitude: 20.6078,
    longitude: -87.0988,
    googleSearchQuery: "Playacar Playa del Carmen Quintana Roo Mexico",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Tulum Archaeological Zone": {
    name: "Zona Arqueológica de Tulum",
    latitude: 20.2149504,
    longitude: -87.4294212,
    googleSearchQuery: "Zona Arqueológica de Tulum Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Tulum Beach Hotel Zone": {
    name: "Zona Hotelera Tulum",
    latitude: 20.1985,
    longitude: -87.4342,
    googleSearchQuery: "Zona Hotelera Tulum Beach Quintana Roo Mexico",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Aldea Zamá Mixed-Use District": {
    name: "Aldea Premium by Aldea Zamá",
    latitude: 20.1961269,
    longitude: -87.4628219,
    reviewAction: "google_canonical",
  },
  "Tulum Municipal Palace Civic Center": {
    name: "PALACIO MUNICIPAL TULUM",
    latitude: 20.2106264,
    longitude: -87.4627787,
    reviewAction: "google_canonical",
  },
  "San Miguel de Cozumel Downtown Waterfront": {
    name: "Malecon San Miguel",
    latitude: 20.5125319,
    longitude: -86.9484001,
    reviewAction: "google_canonical",
  },
  "Punta Sur Eco Beach Park": {
    name: "Punta Sur Eco Beach Park",
    latitude: 20.2925317,
    longitude: -86.958828,
    reviewAction: "google_canonical",
  },
  "Punta Langosta Cruise Pier Plaza": {
    name: "Plaza Punta Langosta",
    latitude: 20.5069781,
    longitude: -86.9549236,
    reviewAction: "google_canonical",
  },
  "Playa Norte Isla Mujeres": {
    name: "Playa Norte",
    latitude: 21.2610351,
    longitude: -86.750881,
    reviewAction: "google_canonical",
  },
  "Garrafón Reef Park": {
    name: "Garrafon Park - MX",
    latitude: 21.2060497,
    longitude: -86.7175093,
    reviewAction: "google_canonical",
  },
  "Hacienda Mundaca Heritage Park": {
    name: "Hacienda Mundaca",
    latitude: 21.2199429,
    longitude: -86.7271937,
    reviewAction: "google_canonical",
  },

  // Batch 2 / delta corrections
  "Marina Town Center Puerto Cancún": {
    name: "Marina Town Center Puerto Cancún",
    latitude: 21.1725,
    longitude: -86.8033,
    googleSearchQuery: "Marina Town Center Puerto Cancún Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Playa Mujeres Beach Resort Corridor": {
    name: "Playa Mujeres Resort Corridor",
    latitude: 21.2811059,
    longitude: -86.8200595,
    manuallyVerified: true,
    dataConfidence: "High",
    reviewAction: "manual_corridor",
  },
  "Amerimed Hospital Cancún": {
    name: "Hospital Amerimed Cancún",
    googleSearchQuery: "Hospital Amerimed Cancún Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Universidad del Caribe Cancún Campus": {
    name: "Universidad del Caribe",
    latitude: 21.1618,
    longitude: -86.8515,
    manuallyVerified: true,
    dataConfidence: "Medium",
    reviewAction: "manual_corridor",
  },
  "Mayakoba Resort Corridor": {
    name: "Mayakoba Resort Corridor",
    latitude: 20.6912,
    longitude: -87.0285,
    manuallyVerified: true,
    dataConfidence: "High",
    reviewAction: "manual_corridor",
  },
  "Mayakoba El Camaleón Golf Club": {
    name: "El Camaleón Golf Course at Mayakoba",
    googleSearchQuery: "El Camaleón Golf Course at Mayakoba Playa del Carmen Mexico",
    reviewAction: "google_canonical",
  },
  "Mayakoba Mixed-Use Village Center": {
    name: "Ciudad Mayakoba",
    googleSearchQuery: "Ciudad Mayakoba Playa del Carmen Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Akumal Bay Beach & Snorkel Corridor": {
    name: "Akumal Bay Snorkel Corridor",
    latitude: 20.3958,
    longitude: -87.3152,
    manuallyVerified: true,
    dataConfidence: "High",
    reviewAction: "manual_corridor",
  },
  "Puerto Aventuras Marina & Resort District": {
    name: "Marina Puerto Aventuras",
    googleSearchQuery: "Marina Puerto Aventuras Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Xel-Há Natural Park": {
    name: "Xel-Há",
    googleSearchQuery: "Xel-Há Park Akumal Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Xplor Adventure Park": {
    name: "Xplor Park",
    googleSearchQuery: "Xplor Park Riviera Maya Playa del Carmen Mexico",
    reviewAction: "search_query",
  },
  "Playa Mamitas Beach Club Corridor": {
    name: "Playa Mamitas Beach Corridor",
    latitude: 20.6315,
    longitude: -87.0688,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Tulum National Park Sian Ka'an Buffer Gateway": {
    name: "Sian Ka'an Biosphere Gateway (Tulum)",
    latitude: 20.1288,
    longitude: -87.4625,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Tulum Hotel Zone Future Growth Corridor": {
    name: "Tulum Beach Hotel Zone Corridor",
    latitude: 20.1925,
    longitude: -87.4288,
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Tulum International Airport Gateway Area": {
    name: "Felipe Carrillo Puerto Airport Gateway (Tulum)",
    latitude: 20.1722,
    longitude: -87.6608,
    manuallyVerified: true,
    dataConfidence: "High",
    reviewAction: "manual_corridor",
  },
  "Cozumel Municipal Palace Civic Center": {
    name: "Palacio Municipal de Cozumel",
    googleSearchQuery: "Palacio Municipal Cozumel Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Isla Mujeres Downtown Malecón": {
    name: "Malecón de Isla Mujeres",
    googleSearchQuery: "Malecón Isla Mujeres Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Moon Palace Convention Center Complex": {
    name: "Moon Palace Convention Center",
    latitude: 21.0285,
    longitude: -86.8752,
    manuallyVerified: true,
    dataConfidence: "High",
    reviewAction: "manual_corridor",
  },
  "Cancún Municipal Palace Civic Center": {
    name: "Palacio Municipal de Cancún",
    googleSearchQuery: "Palacio Municipal Cancún Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Xoximilco Cancún Entertainment Park": {
    name: "Xoximilco by Xcaret",
    googleSearchQuery: "Xoximilco by Xcaret Cancún Quintana Roo Mexico",
    reviewAction: "google_canonical",
  },
  "Ventura Park Cancún": {
    name: "Ventura Park",
    googleSearchQuery: "Ventura Park Cancún Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Playa del Carmen Municipal Palace": {
    name: "Palacio Municipal de Solidaridad",
    googleSearchQuery: "Palacio Municipal Playa del Carmen Quintana Roo Mexico",
    reviewAction: "search_query",
  },
  "Puerto Aventuras Golf Club & Sports Corridor": {
    name: "Puerto Aventuras Golf Club",
    googleSearchQuery: "Puerto Aventuras Golf Club Quintana Roo Mexico",
    reviewAction: "search_query",
  },
};

const REVIEW_TAG = "Google Places review pass 2026-06-23.";

/**
 * @param {object} point
 */
export function applyMexicoCancunPlaceReviewCorrection(point) {
  const fix = MEXICO_CANCUN_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

/**
 * @param {object[]} points
 */
export function applyMexicoCancunPlaceReviewCorrections(points) {
  return points.map(applyMexicoCancunPlaceReviewCorrection);
}
