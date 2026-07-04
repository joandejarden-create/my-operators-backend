/**
 * Nicaragua Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Nicaragua";
const MARKET = "Nicaragua Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const NICARAGUA_TI_DELTA_RECORDS = [
  ti({ name: "Augusto C. Sandino International Airport (MGA) Access", pointType: "Highway Access", city: "Managua", submarket: "Managua", latitude: 12.1415, longitude: -86.1682, sourceReference: "https://www.eana.com.ni/", pointSubtype: "Airport Access", notes: "Primary international gateway for Managua metro corporate and transit hotel demand." }),
  ti({ name: "Port of Corinto Access", pointType: "Port / Maritime", city: "Corinto", submarket: "Other", latitude: 12.4822, longitude: -87.1731, sourceReference: "https://www.epn.com.ni/", pointSubtype: "Commercial Port", notes: "Pacific principal port supporting logistics, crew, and industrial extended-stay demand.", useCaseTags: ["Industrial / Logistics", "Cruise / Port"] }),
  ti({ name: "Granada Lake Nicaragua Lakeshore Access", pointType: "Highway Access", city: "Granada", submarket: "Granada", latitude: 11.9312, longitude: -85.9578, sourceReference: "https://www.visitnicaragua.com/", pointSubtype: "Lakeshore Corridor", notes: "Colonial city lakeshore road access for Granada boutique and leisure lodging." }),
  ti({ name: "San Juan del Sur Coastal Highway Access", pointType: "Highway Access", city: "San Juan del Sur", submarket: "San Juan del Sur", latitude: 11.2589, longitude: -85.8789, sourceReference: "https://www.visitnicaragua.com/", pointSubtype: "Coastal Highway", notes: "Pacific coast road node serving San Juan del Sur surf and beach resort demand." }),
  ti({ name: "Ometepe Moyogalpa Ferry Terminal Access", pointType: "Port / Maritime", city: "Moyogalpa", submarket: "Ometepe", latitude: 11.5382, longitude: -85.6856, sourceReference: "https://www.visitnicaragua.com/", pointSubtype: "Ferry Terminal", notes: "Lake Nicaragua ferry gateway linking Ometepe island eco-lodge and adventure lodging.", useCaseTags: ["Cruise / Port", "Resort / Leisure"] }),
  ti({ name: "Pan-American Highway Managua Corridor Access", pointType: "Highway Access", city: "Managua", submarket: "Managua", latitude: 12.1052, longitude: -86.2681, sourceReference: "https://www.mti.gob.ni/", pointSubtype: "National Highway", notes: "CA-1 Pan-American node through Managua metro linking north-south tourism corridors." }),
  ti({ name: "Granada Masaya Highway Access Corridor", pointType: "Highway Access", city: "Granada", submarket: "Granada", latitude: 11.9344, longitude: -85.956, sourceReference: "https://www.visitnicaragua.com/", pointSubtype: "Heritage Corridor", notes: "Managua–Granada connector road serving colonial heritage and Mombacho volcano tourism." }),
  ti({ name: "San Jorge Rivas Ometepe Ferry Departure Access", pointType: "Port / Maritime", city: "San Jorge", submarket: "Ometepe", latitude: 11.4567, longitude: -85.7891, sourceReference: "https://www.visitnicaragua.com/", pointSubtype: "Ferry Terminal", notes: "Mainland ferry departure point for Ometepe island-bound leisure and eco-tourism traffic.", useCaseTags: ["Cruise / Port", "Airport / Transit"] }),
];

export function buildNicaraguaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, NICARAGUA_TI_DELTA_RECORDS);
}
