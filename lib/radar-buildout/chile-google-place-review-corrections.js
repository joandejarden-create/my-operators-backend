/**
 * Google Places review corrections for Chile — Santiago candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const CHILE_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Clínica Las Condes": {
    googleSearchQuery: "Clínica Las Condes Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Clínica Alemana de Santiago": {
    googleSearchQuery: "Clínica Alemana Santiago Las Condes Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Mall Sport Las Condes": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Clínica Santa María": {
    googleSearchQuery: "Clínica Santa María Providencia Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Clínica MEDS Vitacura": {
    googleSearchQuery: "Clínica MEDS Vitacura Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Universidad de Chile": {
    googleSearchQuery: "Universidad de Chile Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Pontificia Universidad Católica de Chile": {
    googleSearchQuery: "Pontificia Universidad Católica de Chile Santiago",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Universidad Andrés Bello": {
    googleSearchQuery: "Universidad Andrés Bello Providencia Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Metro Baquedano Transit Hub": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Casas Costanera": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Museo Ralli": {
    googleSearchQuery: "Museo Ralli Vitacura Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Estadio Nacional Julio Martínez Prádanos": {
    googleSearchQuery: "Estadio Nacional Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Movistar Arena": {
    googleSearchQuery: "Movistar Arena Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "ENEA Business Park Airport Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Espacio Riesco": {
    googleSearchQuery: "Espacio Riesco Las Condes Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Teatro Municipal de Santiago": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "CentroParque Events Complex": {
    googleSearchQuery: "CentroParque Las Condes Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Costanera Center": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Mall Costanera Center": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Metro Tobalaba Financial Access": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Parque Arauco": {
    googleSearchQuery: "Parque Arauco Las Condes Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Aeropuerto Internacional Arturo Merino Benítez": {
    googleSearchQuery: "Aeropuerto Internacional Arturo Merino Benítez Santiago Chile",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Palacio de La Moneda": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Congreso Nacional de Chile": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyChilePlaceReviewCorrection(point) {
  const fix = CHILE_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
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

export function applyChilePlaceReviewCorrections(points) {
  return points.map(applyChilePlaceReviewCorrection);
}
