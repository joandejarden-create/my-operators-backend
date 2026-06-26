/**
 * Google Places review corrections for Peru — Lima / Cusco candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const PERU_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Clínica Internacional Miraflores": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Clínica Ricardo Palma Miraflores": {
    name: "Clínica Ricardo Palma",
    latitude: -12.0906021,
    longitude: -77.0182762,
    submarket: "San Isidro",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Jockey Plaza Shopping & Convention Hub": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Clínica San Felipe": {
    googleSearchQuery: "Clínica San Felipe San Isidro Lima Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Clínica El Golf": {
    googleSearchQuery: "Clínica El Golf San Isidro Lima Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Museo de Arte Contemporáneo MAC": {
    googleSearchQuery: "Museo de Arte Contemporáneo MAC Barranco Lima Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Catedral de Lima": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Monasterio de San Francisco": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Centro de Convenciones de Lima": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Universidad de Lima": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Pontificia Universidad Católica del Perú": {
    googleSearchQuery: "Pontificia Universidad Católica del Perú San Miguel Lima",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Hospital Nacional Edgardo Rebagliati": {
    googleSearchQuery: "Hospital Nacional Edgardo Rebagliati Essalud Lima Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Fortaleza del Real Felipe": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Estadio Monumental": {
    googleSearchQuery: "Estadio Monumental Lima Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Real Plaza Salaverry": {
    googleSearchQuery: "Real Plaza Salaverry San Isidro Lima Peru",
    reviewAction: "search_query",
  },
  "Puente de los Suspiros": {
    googleSearchQuery: "Puente de los Suspiros Barranco Lima Peru",
    reviewAction: "search_query",
  },
  "Museo Larco": {
    googleSearchQuery: "Museo Larco Pueblo Libre Lima Peru",
    reviewAction: "search_query",
  },
  "Jockey Plaza Convention Center": {
    googleSearchQuery: "Centro de Convenciones Jockey Plaza Lima Peru",
    reviewAction: "search_query",
  },
  "Qorikancha Temple": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Sacsayhuamán Archaeological Park": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Mercado de San Pedro": {
    googleSearchQuery: "Mercado de San Pedro Cusco Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Pisac Archaeological Park": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Moray Archaeological Site": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Salineras de Maras": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Chinchero Heritage Site": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Machu Picchu Citadel Gateway": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Centro de Convenciones de Cusco": {
    googleSearchQuery: "Centro de Convenciones Cusco Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Tambo del Inka Luxury Resort": {
    googleSearchQuery: "Tambo del Inka Urubamba Sacred Valley Peru",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Aranwa Sacred Valley Hotel & Wellness": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyPeruPlaceReviewCorrection(point) {
  const fix = PERU_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

export function applyPeruPlaceReviewCorrections(points) {
  return points.map(applyPeruPlaceReviewCorrection);
}
