/**
 * Ecuador Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Ecuador";
const MARKET = "Ecuador Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const ECUADOR_TI_DELTA_RECORDS = [
  ti({ name: "Mariscal Sucre International Airport Access", pointType: "Highway Access", city: "Quito", submarket: "Quito", latitude: -0.1292, longitude: -78.3575, sourceReference: "https://www.quitoairport.aero/", pointSubtype: "Airport Access", notes: "UIO Tababela airport highway access for Quito metro and highland tourism lodging." }),
  ti({ name: "José Joaquín de Olmedo International Airport Access", pointType: "Highway Access", city: "Guayaquil", submarket: "Guayaquil", latitude: -2.1578, longitude: -79.8839, sourceReference: "https://www.tagsa.aero/", pointSubtype: "Airport Access", notes: "GYE coastal gateway highway access for Guayaquil business and Pacific resort flows." }),
  ti({ name: "Port of Guayaquil Maritime Terminal Access", pointType: "Port / Maritime", city: "Guayaquil", submarket: "Guayaquil", latitude: -2.2764, longitude: -79.8864, sourceReference: "https://www.puertodeguayaquil.com/", pointSubtype: "Container Port", notes: "Primary Pacific container and cruise port supporting Guayaquil urban hotel demand.", useCaseTags: ["Industrial / Logistics", "Urban / Corporate"] }),
  ti({ name: "Pan-American Highway Quito Norte Corridor Access", pointType: "Highway Access", city: "Quito", submarket: "Quito", latitude: -0.1753, longitude: -78.4675, sourceReference: "https://www.gob.ec/mtop", pointSubtype: "Pan-American Corridor", notes: "Panamericana Norte vehicular node linking Quito valley to northern highland markets." }),
  ti({ name: "Baltra Galápagos Airport Access", pointType: "Highway Access", city: "Baltra", submarket: "Galápagos", latitude: -0.4228, longitude: -90.2861, sourceReference: "https://www.gob.ec/galapagos", pointSubtype: "Airport Access", notes: "GPS island air gateway access for Galápagos expedition and eco-lodge demand." }),
  ti({ name: "Itabaca Channel Ferry Galápagos Crossing Access", pointType: "Port / Maritime", city: "Santa Cruz", submarket: "Galápagos", latitude: -0.4542, longitude: -90.2764, sourceReference: "https://www.gob.ec/galapagos", pointSubtype: "Ferry Terminal", notes: "Baltra–Santa Cruz ferry crossing linking airport arrivals to Puerto Ayora lodging.", useCaseTags: ["Airport / Transit", "Nature / Eco-Tourism"] }),
  ti({ name: "Cuenca Pan-American Highway Sur Access", pointType: "Highway Access", city: "Cuenca", submarket: "Cuenca", latitude: -2.9001, longitude: -79.0059, sourceReference: "https://www.ecuador.travel/", pointSubtype: "Pan-American Corridor", notes: "Panamericana Sur highway node for Cuenca heritage and southern highland tourism.", useCaseTags: ["Heritage / Cultural Tourism", "Resort / Leisure"] }),
  ti({ name: "Ambato Pan-American Central Valley Highway Access", pointType: "Highway Access", city: "Ambato", submarket: "Other", latitude: -1.2419, longitude: -78.6197, sourceReference: "https://www.gob.ec/mtop", pointSubtype: "Pan-American Corridor", notes: "Central valley Pan-American connector between Quito and Cuenca intercity hotel corridors." }),
];

export function buildEcuadorTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, ECUADOR_TI_DELTA_RECORDS);
}
