/**
 * Google Places review corrections for Panama candidates.
 */
const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const PANAMA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Atlapa Convention Center": {
    "latitude": 8.9896587,
    "longitude": -79.5000509,
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Panama Convention Center": {
    "latitude": 8.9378881,
    "longitude": -79.5480273,
    "googleSearchQuery": "Panama Convention Center Amador Panama City Panama",
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Calle 50 Financial District": {
    "name": "Calle 50 Financial District",
    "latitude": 8.9804815,
    "longitude": -79.5230171,
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Obarrio Banking District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Via España Business Corridor": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Ciudad de Salud Medical Complex": {
    "name": "Ciudad de la Salud",
    "latitude": 9.0271359,
    "longitude": -79.5812085,
    "googleSearchQuery": "Ciudad de la Salud Panamá Panama",
    "reviewAction": "rename_search"
  },
  "Universidad de Panamá": {
    "latitude": 8.9826298,
    "longitude": -79.5374849,
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Universidad Santa María La Antigua": {
    "name": "Universidad Católica Santa María La Antigua",
    "latitude": 9.0289232,
    "longitude": -79.5213638,
    "reviewAction": "google_canonical"
  },
  "Multiplaza Pacific Mall": {
    "name": "Multiplaza Panamá",
    "latitude": 8.9862739,
    "longitude": -79.5112424,
    "reviewAction": "google_canonical"
  },
  "Albrook Mall Mixed-Use Hub": {
    "name": "Albrook Mall",
    "latitude": 8.9743549,
    "longitude": -79.5526376,
    "reviewAction": "google_canonical"
  },
  "Palacio de las Garzas": {
    "latitude": 8.9530421,
    "longitude": -79.5345733,
    "googleSearchQuery": "Palacio de las Garzas Casco Viejo Panama City Panama",
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Panama City Municipal Palace": {
    "name": "Alcaldía de Panamá",
    "latitude": 8.9512,
    "longitude": -79.5342,
    "googleSearchQuery": "Alcaldía de Panamá Casco Viejo Panama",
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Panama Metro Line 1 Hub (Iglesia del Carmen)": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Panama Pacifico Business Park": {
    "name": "Panamá Pacífico International Airport",
    "latitude": 8.91479,
    "longitude": -79.599602,
    "googleSearchQuery": "Panamá Pacífico International Airport Panama",
    "reviewAction": "google_canonical"
  },
  "City of Knowledge Innovation District": {
    "name": "City of Knowledge Foundation",
    "latitude": 8.9993209,
    "longitude": -79.584566,
    "reviewAction": "google_canonical"
  },
  "PanAmerica Corporate Center": {
    "latitude": 9.0186,
    "longitude": -79.4712,
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Logistics City Panama": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Tocumen Airport Gateway Growth Node": {
    "latitude": 9.0690876,
    "longitude": -79.3830103,
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Miraflores Visitor Center Panama Canal": {
    "name": "Miraflores Visitor Center",
    "latitude": 8.9972715,
    "longitude": -79.5913604,
    "reviewAction": "google_canonical"
  },
  "Agua Clara Locks Visitor Center": {
    "latitude": 9.2723378,
    "longitude": -79.9060823,
    "googleSearchQuery": "Centro de Visitantes de Agua Clara Colón Panama",
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Panama Canal Administration Building": {
    "name": "Edificio de La Administración del Canal de Panamá",
    "latitude": 8.9594695,
    "longitude": -79.5550504,
    "reviewAction": "google_canonical"
  },
  "Colon Free Zone": {
    "latitude": 9.3500294,
    "longitude": -79.8824052,
    "reviewAction": "google_canonical"
  },
  "Manzanillo International Terminal Zone": {
    "name": "Manzanillo International Terminal",
    "latitude": 9.3651477,
    "longitude": -79.8811514,
    "reviewAction": "google_canonical"
  },
  "Fuerte Amador Cruise Terminal": {
    "name": "Terminal de Cruceros de Amador",
    "googleSearchQuery": "Terminal de Cruceros de Amador Panama City Panama",
    "reviewAction": "rename_search"
  },
  "Bastimentos Island Tourism Node": {
    "name": "Bastimentos Island",
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Starfish Beach Bastimentos": {
    "name": "Playa Estrella",
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Volcán Barú Trail Gateway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Hospital San Fernando Costa del Este": {
    "name": "Clínica Hospital San Fernando",
    "googleSearchQuery": "Clínica Hospital San Fernando Costa del Este Panama",
    "reviewAction": "rename_search"
  },
  "Town Center Costa del Este": {
    "googleSearchQuery": "Town Center Costa del Este Panama City Panama",
    "reviewAction": "search_query"
  },
  "Panamá Design Plaza": {
    "name": "Panamá Design Center",
    "googleSearchQuery": "Panamá Design Center Costa del Este Panama",
    "reviewAction": "rename_search"
  },
  "MultiCentro Costa del Este": {
    "googleSearchQuery": "MultiCentro Costa del Este Panama City Panama",
    "reviewAction": "manual_corridor",
    "manuallyVerified": true,
    "latitude": 9.0108,
    "longitude": -79.4618
  },
  "Casco Viejo Historic District": {
    "name": "Casco Viejo",
    "latitude": 8.9526205,
    "longitude": -79.5372465,
    "reviewAction": "google_canonical"
  },
  "Smithsonian Tropical Research Institute (Amador)": {
    "name": "Smithsonian Tropical Research Institution",
    "googleSearchQuery": "Smithsonian Tropical Research Institute Amador Panama",
    "reviewAction": "rename_search"
  },
  "Costa del Este Business District": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Buenaventura Golf & Beach Resort": {
    "latitude": 8.339916,
    "longitude": -80.171416,
    "googleSearchQuery": "Buenaventura Golf Beach Resort Riviera Pacifica Panama",
    "reviewAction": "manual_corridor",
    "manuallyVerified": true
  },
  "Finca Lerida Coffee Tourism Estate": {
    "name": "Finca Lérida",
    "googleSearchQuery": "Finca Lérida Boquete Panama",
    "reviewAction": "rename_search"
  },
  "David Chiriquí Gateway Growth Node": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Bocas Town Waterfront": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Bocas del Toro Airport Gateway": {
    "latitude": 9.34085,
    "longitude": -82.250801,
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Panama Viejo Archaeological Park": {
    "name": "Panamá Viejo",
    "latitude": 9.0066593,
    "longitude": -79.4851348,
    "googleSearchQuery": "Panamá Viejo archaeological site Panama City Panama",
    "reviewAction": "google_canonical"
  },
  "Colon Cruise Port Tourism Gateway": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor"
  },
  "Casco Viejo": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 8.9526205,
    "longitude": -79.5372465
  },
  "Panamá Design Center": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 9.0086,
    "longitude": -79.4588
  },
  "Clínica Hospital San Fernando": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 9.0162,
    "longitude": -79.4624
  },
  "Finca Lérida": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 8.7942,
    "longitude": -82.4512
  },
  "Panamá Viejo": {
    "manuallyVerified": true,
    "reviewAction": "manual_corridor",
    "latitude": 9.0066593,
    "longitude": -79.4851348
  }
};

export function applyPanamaPlaceReviewCorrection(point) {
  const fix = PANAMA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyPanamaPlaceReviewCorrections(points) {
  return points.map(applyPanamaPlaceReviewCorrection);
}
