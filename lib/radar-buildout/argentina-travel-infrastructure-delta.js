/**
 * Argentina Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Argentina";
const MARKET = "Argentina Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const ARGENTINA_TI_DELTA_RECORDS = [
  ti({ name: "Ministro Pistarini International Airport Access", pointType: "Highway Access", city: "Ezeiza", submarket: "Buenos Aires", latitude: -34.8222, longitude: -58.5358, sourceReference: "https://www.aa2000.com.ar/", pointSubtype: "Airport Access", notes: "EZE international gateway highway access for greater Buenos Aires hotel demand." }),
  ti({ name: "Aeroparque Jorge Newbery Airport Access", pointType: "Highway Access", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.5592, longitude: -58.4156, sourceReference: "https://www.aa2000.com.ar/", pointSubtype: "Airport Access", notes: "AEP domestic and regional air access node for Buenos Aires urban lodging markets." }),
  ti({ name: "Buenos Aires Cruise Terminal Rio de la Plata Access", pointType: "Port / Maritime", city: "Buenos Aires", submarket: "Buenos Aires", latitude: -34.6025, longitude: -58.3714, sourceReference: "https://www.buenosaires.gob.ar/", pointSubtype: "Cruise Terminal", notes: "Terminal de Pasajeros Benito Quinquela Martín cruise berth on the Rio de la Plata.", useCaseTags: ["Cruise / Port", "Urban / Corporate"] }),
  ti({ name: "Pan-American Highway RN-7 Mendoza Corridor Access", pointType: "Highway Access", city: "Mendoza", submarket: "Mendoza", latitude: -32.8902, longitude: -68.8441, sourceReference: "https://www.vialidad.gob.ar/", pointSubtype: "Pan-American Corridor", notes: "RN-7 Pan-American node linking Mendoza wine-country and Andes resort hotel markets." }),
  ti({ name: "San Carlos de Bariloche Airport Corridor Access", pointType: "Highway Access", city: "San Carlos de Bariloche", submarket: "Bariloche", latitude: -41.1511, longitude: -71.1575, sourceReference: "https://www.aeropuertosargentina.com/", pointSubtype: "Airport Access", notes: "BRC airport highway corridor for Patagonia lakes district leisure lodging." }),
  ti({ name: "Puerto Iguazú Cataratas Falls Highway Access", pointType: "Highway Access", city: "Puerto Iguazú", submarket: "Puerto Iguazú", latitude: -25.682, longitude: -54.455, sourceReference: "https://www.iguazuargentina.com/", pointSubtype: "Scenic Corridor", notes: "Primary road access from Puerto Iguazú to Iguazú National Park waterfall tourism.", useCaseTags: ["Resort / Leisure", "Nature / Eco-Tourism"] }),
  ti({ name: "Ushuaia Port Maritime Terminal Access", pointType: "Port / Maritime", city: "Ushuaia", submarket: "Ushuaia", latitude: -54.8061, longitude: -68.3036, sourceReference: "https://www.argentina.gob.ar/prefecturanaval", pointSubtype: "Cruise Terminal", notes: "End-of-world cruise and expedition embarkation port for Tierra del Fuego hotel demand.", useCaseTags: ["Cruise / Port", "Resort / Leisure"] }),
  ti({ name: "Cataratas del Iguazú International Airport Access", pointType: "Highway Access", city: "Puerto Iguazú", submarket: "Puerto Iguazú", latitude: -25.7361, longitude: -54.4734, sourceReference: "https://www.aeropuertosargentina.com/", pointSubtype: "Airport Access", notes: "IGR airport highway access complementing falls tourism flows in northeast Misiones." }),
];

export function buildArgentinaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, ARGENTINA_TI_DELTA_RECORDS);
}
