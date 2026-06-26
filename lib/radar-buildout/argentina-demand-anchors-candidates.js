/**
 * Argentina countrywide demand anchor candidates (source-backed).
 */
import { createSouthAmericaCandidateBuilder } from "./south-america-country-shared.js";
import {
  applyArgentinaGovernanceDefaults,
  ARGENTINA_SUBMARKETS,
} from "./argentina-demand-anchor-governance.js";

const COUNTRY = "Argentina";
const pt = createSouthAmericaCandidateBuilder(COUNTRY, applyArgentinaGovernanceDefaults);

export { ARGENTINA_SUBMARKETS };

export const ARGENTINA_CANDIDATES = [
  // Buenos Aires
  pt({ name: "Ministro Pistarini International Airport (EZE)", pointType: "Future Growth Node", city: "Ezeiza", submarket: "Buenos Aires", latitude: -34.8222, longitude: -58.5358, sourceReference: "https://www.aa2000.com.ar/ezeiza", manuallyVerified: true }),
  pt({ name: "Aeroparque Jorge Newbery (AEP)", pointType: "Future Growth Node", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5592, longitude: -58.4156, sourceReference: "https://www.aa2000.com.ar/aeroparque", manuallyVerified: true }),
  pt({ name: "Puerto Madero Waterfront District", pointType: "Mixed-Use Development", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6106, longitude: -58.3614, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/waterfront-puerto-madero", manuallyVerified: true }),
  pt({ name: "Recoleta Cultural District", pointType: "Tourist Attraction", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5875, longitude: -58.3930, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/recoleta", manuallyVerified: true }),
  pt({ name: "San Telmo Historic Quarter", pointType: "Entertainment District", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6206, longitude: -58.3716, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/san-telmo", manuallyVerified: true }),
  pt({ name: "La Boca Caminito Tourism Corridor", pointType: "Tourist Attraction", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6394, longitude: -58.3631, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/la-boca", manuallyVerified: true }),
  pt({ name: "Teatro Colón", pointType: "Tourist Attraction", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6010, longitude: -58.3831, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/teatro-colon", manuallyVerified: true }),
  pt({ name: "Centro Costa Salguero Convention Center", pointType: "Convention Center", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5836, longitude: -58.4189, sourceReference: "https://www.centrocostasalguero.com/" }),
  pt({ name: "La Rural Predio Ferial Exhibition Center", pointType: "Convention Center", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5734, longitude: -58.4188, sourceReference: "https://larural.com.ar/" }),
  pt({ name: "Hospital Italiano de Buenos Aires", pointType: "Medical Campus", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6033, longitude: -58.4214, sourceReference: "https://www.hospitalitaliano.org.ar/" }),
  pt({ name: "Hospital Alemán", pointType: "Medical Campus", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5946, longitude: -58.4025, sourceReference: "https://www.hospitalaleman.org.ar/" }),
  pt({ name: "Universidad de Buenos Aires Ciudad Universitaria", pointType: "University / College", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5420, longitude: -58.4435, sourceReference: "https://www.uba.ar/" }),
  pt({ name: "Pontificia Universidad Católica Argentina", pointType: "University / College", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6098, longitude: -58.3732, sourceReference: "https://www.uca.edu.ar/" }),
  pt({ name: "Microcentro Obelisco Business District", pointType: "Business District", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6037, longitude: -58.3816, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/obelisco-and-9-de-julio-avenue" }),
  pt({ name: "Palermo Soho Entertainment District", pointType: "Entertainment District", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5873, longitude: -58.4256, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/palermo" }),
  pt({ name: "Congreso de la Nación Civic Center", pointType: "Government / Civic Center", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6097, longitude: -58.3925, sourceReference: "https://turismo.buenosaires.gob.ar/en/other-great-ideas/congress" }),
  pt({ name: "Universidad Torcuato Di Tella", pointType: "University / College", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5748, longitude: -58.4416, sourceReference: "https://www.utdt.edu/" }),

  // Mendoza
  pt({ name: "Mendoza Wine Capital Tourism Core", pointType: "Entertainment District", city: "Mendoza", submarket: "Mendoza", latitude: -32.8895, longitude: -68.8440, sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza", manuallyVerified: true }),
  pt({ name: "Maipú Wine Route Corridor", pointType: "Tourist Attraction", city: "Maipú", submarket: "Mendoza", latitude: -33.0167, longitude: -68.7833, sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza/maipu-wine-route", manuallyVerified: true }),
  pt({ name: "Luján de Cuyo Wine Valley", pointType: "Tourist Attraction", city: "Luján de Cuyo", submarket: "Mendoza", latitude: -33.0333, longitude: -68.8833, sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza/lujan-de-cuyo" }),
  pt({ name: "Uco Valley Wine Region", pointType: "Tourist Attraction", city: "Tunuyán", submarket: "Mendoza", latitude: -33.5833, longitude: -69.0167, sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza/uco-valley" }),
  pt({ name: "Governor Francisco Gabrielli International Airport", pointType: "Future Growth Node", city: "Mendoza", submarket: "Mendoza", latitude: -32.8317, longitude: -68.7928, sourceReference: "https://www.aeropuertomendoza.gob.ar/", manuallyVerified: true }),
  pt({ name: "Plaza Independencia Mendoza Historic Center", pointType: "Tourist Attraction", city: "Mendoza", submarket: "Mendoza", latitude: -32.8908, longitude: -68.8272, sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza" }),

  // Bariloche
  pt({ name: "Nahuel Huapi Lake Waterfront", pointType: "Beach / Waterfront", city: "San Carlos de Bariloche", submarket: "Bariloche", latitude: -41.1333, longitude: -71.3000, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche", manuallyVerified: true }),
  pt({ name: "Cerro Catedral Ski Basin", pointType: "Tourist Attraction", city: "San Carlos de Bariloche", submarket: "Bariloche", latitude: -41.1667, longitude: -71.4500, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche/cerro-catedral" }),
  pt({ name: "Bariloche City Tourism Core", pointType: "Entertainment District", city: "San Carlos de Bariloche", submarket: "Bariloche", latitude: -41.1335, longitude: -71.3103, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche" }),
  pt({ name: "San Carlos de Bariloche Airport Corridor", pointType: "Future Growth Node", city: "San Carlos de Bariloche", submarket: "Bariloche", latitude: -41.1512, longitude: -71.1575, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche", manuallyVerified: true }),
  pt({ name: "Circuito Chico Lakes District", pointType: "Tourist Attraction", city: "San Carlos de Bariloche", submarket: "Bariloche", latitude: -41.1200, longitude: -71.3500, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche/circuito-chico" }),

  // Córdoba
  pt({ name: "Córdoba Historic Jesuit Block UNESCO", pointType: "Tourist Attraction", city: "Córdoba", submarket: "Córdoba", latitude: -31.4167, longitude: -64.1861, sourceReference: "https://whc.unesco.org/en/list/995/", manuallyVerified: true }),
  pt({ name: "Nueva Córdoba University District", pointType: "University / College", city: "Córdoba", submarket: "Córdoba", latitude: -31.4275, longitude: -64.1856, sourceReference: "https://www.argentina.travel/en/destinations/centro/cordoba" }),
  pt({ name: "Ingeniero Ambrosio Taravella Airport Corridor", pointType: "Future Growth Node", city: "Córdoba", submarket: "Córdoba", latitude: -31.3236, longitude: -64.2081, sourceReference: "https://www.argentina.travel/en/destinations/centro/cordoba", manuallyVerified: true }),
  pt({ name: "Córdoba City Business Core", pointType: "Business District", city: "Córdoba", submarket: "Córdoba", latitude: -31.4201, longitude: -64.1886, sourceReference: "https://www.argentina.travel/en/destinations/centro/cordoba" }),

  // Puerto Iguazú
  pt({ name: "Iguazú Falls UNESCO World Heritage Site", pointType: "Tourist Attraction", city: "Puerto Iguazú", submarket: "Puerto Iguazú", latitude: -25.6953, longitude: -54.4367, sourceReference: "https://whc.unesco.org/en/list/303/", manuallyVerified: true }),
  pt({ name: "Puerto Iguazú Tourism Gateway", pointType: "Entertainment District", city: "Puerto Iguazú", submarket: "Puerto Iguazú", latitude: -25.5973, longitude: -54.5785, sourceReference: "https://www.argentina.travel/en/destinations/litoral/iguazu-falls" }),
  pt({ name: "Cataratas del Iguazú Airport Corridor", pointType: "Future Growth Node", city: "Puerto Iguazú", submarket: "Puerto Iguazú", latitude: -25.7373, longitude: -54.4734, sourceReference: "https://www.argentina.travel/en/destinations/litoral/iguazu-falls", manuallyVerified: true }),
  pt({ name: "Hito Tres Fronteras Landmark", pointType: "Tourist Attraction", city: "Puerto Iguazú", submarket: "Puerto Iguazú", latitude: -25.5978, longitude: -54.5906, sourceReference: "https://www.argentina.travel/en/destinations/litoral/iguazu-falls" }),

  // Mar del Plata
  pt({ name: "Mar del Plata Beach Resort Coast", pointType: "Beach / Waterfront", city: "Mar del Plata", submarket: "Mar del Plata", latitude: -38.0055, longitude: -57.5426, sourceReference: "https://www.argentina.travel/en/destinations/atlantic/mar-del-plata", manuallyVerified: true }),
  pt({ name: "Playa Bristol Waterfront", pointType: "Beach / Waterfront", city: "Mar del Plata", submarket: "Mar del Plata", latitude: -38.0083, longitude: -57.5317, sourceReference: "https://www.argentina.travel/en/destinations/atlantic/mar-del-plata" }),
  pt({ name: "Ástor Piazzolla International Airport Corridor", pointType: "Future Growth Node", city: "Mar del Plata", submarket: "Mar del Plata", latitude: -37.9342, longitude: -57.5733, sourceReference: "https://www.argentina.travel/en/destinations/atlantic/mar-del-plata", manuallyVerified: true }),
  pt({ name: "Mar del Plata Casino Entertainment Strip", pointType: "Entertainment District", city: "Mar del Plata", submarket: "Mar del Plata", latitude: -38.0050, longitude: -57.5340, sourceReference: "https://www.argentina.travel/en/destinations/atlantic/mar-del-plata" }),

  // Ushuaia
  pt({ name: "Ushuaia Cruise Port Gateway", pointType: "Mixed-Use Development", city: "Ushuaia", submarket: "Ushuaia", latitude: -54.8019, longitude: -68.3030, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/ushuaia", manuallyVerified: true }),
  pt({ name: "Tierra del Fuego National Park", pointType: "Tourist Attraction", city: "Ushuaia", submarket: "Ushuaia", latitude: -54.8333, longitude: -68.4333, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/ushuaia/tierra-del-fuego-national-park" }),
  pt({ name: "Malvinas Argentinas International Airport Corridor", pointType: "Future Growth Node", city: "Ushuaia", submarket: "Ushuaia", latitude: -54.8433, longitude: -68.2958, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/ushuaia", manuallyVerified: true }),
  pt({ name: "End of the World Maritime District", pointType: "Beach / Waterfront", city: "Ushuaia", submarket: "Ushuaia", latitude: -54.8056, longitude: -68.3042, sourceReference: "https://www.argentina.travel/en/destinations/patagonia/ushuaia" }),

  // Salta
  pt({ name: "Salta Historic Northwest Heritage Center", pointType: "Tourist Attraction", city: "Salta", submarket: "Salta", latitude: -24.7821, longitude: -65.4232, sourceReference: "https://www.argentina.travel/en/destinations/northwest/salta", manuallyVerified: true }),
  pt({ name: "Cafayate Wine Valley Corridor", pointType: "Tourist Attraction", city: "Cafayate", submarket: "Salta", latitude: -26.0667, longitude: -65.9833, sourceReference: "https://www.argentina.travel/en/destinations/northwest/salta/cafayate" }),
  pt({ name: "Quebrada de Humahuaca UNESCO Gateway", pointType: "Tourist Attraction", city: "Purmamarca", submarket: "Salta", latitude: -23.2000, longitude: -65.3167, sourceReference: "https://whc.unesco.org/en/list/1116/", manuallyVerified: true }),
  pt({ name: "Martín Miguel de Güemes International Airport Corridor", pointType: "Future Growth Node", city: "Salta", submarket: "Salta", latitude: -24.8560, longitude: -65.4862, sourceReference: "https://www.argentina.travel/en/destinations/northwest/salta", manuallyVerified: true }),
];

export function getArgentinaCandidates() {
  return ARGENTINA_CANDIDATES;
}
