/**
 * Google Places review corrections for Mexico City candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const MEXICO_CITY_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Antara Fashion Hall Polanco": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4383,
    "longitude": -99.2017
  },
  "Museo Jumex Polanco": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4401,
    "longitude": -99.2041
  },
  "Polanco Business and Embassy Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4336,
    "longitude": -99.1991
  },
  "Centro Citibanamex Convention Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4378,
    "longitude": -99.2045
  },
  "Lago Mayor Chapultepec Polanco Edge": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4245,
    "longitude": -99.1945
  },
  "Ángel de la Independencia Reforma": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.427,
    "longitude": -99.1677
  },
  "Paseo de la Reforma Financial Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4286,
    "longitude": -99.1611
  },
  "Torre Mayor Reforma CBD": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4245,
    "longitude": -99.1758
  },
  "Diana Cazadora Roundabout Reforma": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4252,
    "longitude": -99.1708
  },
  "Zona Rosa Entertainment Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4268,
    "longitude": -99.1635
  },
  "Centro Santa Fe": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3594,
    "longitude": -99.2767
  },
  "Hospital ABC Santa Fe Campus": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3678,
    "longitude": -99.2634
  },
  "Torre Corporativa Santa Fe": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3567,
    "longitude": -99.2745
  },
  "Tecnológico de Monterrey Santa Fe Campus": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3523,
    "longitude": -99.2812
  },
  "Condesa Dining and Nightlife Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4123,
    "longitude": -99.1756
  },
  "Roma Norte Art and Design District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4189,
    "longitude": -99.1623
  },
  "Mercado Roma Food Hall": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4167,
    "longitude": -99.1612
  },
  "Templo Mayor Archaeological Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.435,
    "longitude": -99.1313
  },
  "Torre Latinoamericana Observation Deck": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4338,
    "longitude": -99.1406
  },
  "Palacio Nacional Government Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4324,
    "longitude": -99.1315
  },
  "Mercado de San Juan Gourmet Market": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4278,
    "longitude": -99.1389
  },
  "Insurgentes Sur Corporate Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3812,
    "longitude": -99.1789
  },
  "Torre Insignia WTC District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3934,
    "longitude": -99.1712
  },
  "Centro Banorte WTC Convention Annex": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3923,
    "longitude": -99.1767
  },
  "Aeropuerto Cargo and Logistics Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4289,
    "longitude": -99.0656
  },
  "Peñón de los Baños Transit Hub": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4512,
    "longitude": -99.0789
  },
  "Pantitlán Multimodal Station": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4156,
    "longitude": -99.0723
  },
  "Museo Frida Kahlo Casa Azul": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.355,
    "longitude": -99.1623
  },
  "UNAM Ciudad Universitaria Campus": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3244,
    "longitude": -99.2
  },
  "San Ángel Bazaar and Cultural District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3456,
    "longitude": -99.1945
  },
  "Estadio Azteca": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.3029,
    "longitude": -99.1505
  },
  "Arena Ciudad de México": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4931,
    "longitude": -99.2386
  },
  "Foro Sol Entertainment Complex": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 19.4056,
    "longitude": -99.0923
  }
};

export function applyMexicoCityPlaceReviewCorrection(point) {
  const fix = MEXICO_CITY_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyMexicoCityPlaceReviewCorrections(points) {
  return points.map(applyMexicoCityPlaceReviewCorrection);
}
