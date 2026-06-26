/**
 * Google Places review corrections for Guadeloupe Countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const GUADELOUPE_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Pointe-à-Pitre Central Market and Downtown": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2412,
    "longitude": -61.5312
  },
  "Centre Hospitalier Universitaire de Pointe-à-Pitre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2512,
    "longitude": -61.5289
  },
  "Université des Antilles — Guadeloupe Campus": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2534,
    "longitude": -61.5412
  },
  "Destreland Shopping Center": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2689,
    "longitude": -61.5712
  },
  "Jarry Industrial and Port Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2412,
    "longitude": -61.5589
  },
  "Stade René Serge Nabajoth": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2712,
    "longitude": -61.5089
  },
  "Place de la Victoire Civic Precinct": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0012,
    "longitude": -61.7312
  },
  "Saint-François Marina and Golf Resort": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2512,
    "longitude": -61.2712
  },
  "Pointe des Châteaux Scenic Headland": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2489,
    "longitude": -61.1789
  },
  "Le Gosier Beach and Fort Fleur d'Épée": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2012,
    "longitude": -61.4912
  },
  "Datcha Beach Le Gosier": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.1989,
    "longitude": -61.5012
  },
  "Porte d'Enfer Coastal Arch": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.4712,
    "longitude": -61.4512
  },
  "Moule East Coast Fishing Village": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.3312,
    "longitude": -61.3412
  },
  "Morne-à-l'Eau Heritage Town": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.3289,
    "longitude": -61.4712
  },
  "Parc National de la Guadeloupe": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0189,
    "longitude": -61.6912
  },
  "Cascade aux Écrevisses Waterfall": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0289,
    "longitude": -61.6789
  },
  "Deshaies Botanical Garden": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.3112,
    "longitude": -61.7912
  },
  "Bouillante Hot Springs and Cousteau Reserve": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.1289,
    "longitude": -61.7689
  },
  "Plage de Malendure Snorkel Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.1212,
    "longitude": -61.7712
  },
  "Fort Delgrès Historic Site": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0012,
    "longitude": -61.7289
  },
  "Trois-Rivières South Basse-Terre Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.9789,
    "longitude": -61.6412
  },
  "Marie-Galante Distillery Heritage": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.8812,
    "longitude": -61.3189
  },
  "Plage de la Feuillère Marie-Galante": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 15.8689,
    "longitude": -61.2512
  },
  "La Désirade Island Nature Reserve": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.3189,
    "longitude": -61.0512
  },
  "Carbet Falls — Première Chute": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0389,
    "longitude": -61.7012
  },
  "Rivière-Sens Convention and Events Zone": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2612,
    "longitude": -61.5189
  },
  "Petit-Bourg Rainforest Gateway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0312,
    "longitude": -61.6712
  },
  "Lamentin Commercial Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2712,
    "longitude": -61.5512
  },
  "Capesterre-Belle-Eau East Basse-Terre": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0512,
    "longitude": -61.5712
  },
  "Vieux-Habitants Coffee Heritage Coast": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.0589,
    "longitude": -61.7612
  },
  "Guadeloupe Grande-Terre Resort Growth Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 16.2489,
    "longitude": -61.2812
  }
};

export function applyGuadeloupePlaceReviewCorrection(point) {
  const fix = GUADELOUPE_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyGuadeloupePlaceReviewCorrections(points) {
  return points.map(applyGuadeloupePlaceReviewCorrection);
}
