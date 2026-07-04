/**
 * Monterrey Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Monterrey";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const MONTERREY_TI_DELTA_RECORDS = [
  ti({ name: "Monterrey International Airport Access", pointType: "Highway Access", city: "Apodaca", submarket: "Airport Corridor", latitude: 25.7785, longitude: -100.1069, sourceReference: "https://www.oma.aero/", pointSubtype: "Airport Access", notes: "Primary international gateway for Monterrey corporate and industrial demand." }),
  ti({ name: "Macroplaza Civic Core Access", pointType: "Highway Access", city: "Monterrey", submarket: "Centro", latitude: 25.6714, longitude: -100.3097, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Urban Core", notes: "Downtown civic and government precinct access." }),
  ti({ name: "Parque Fundidora Convention Access", pointType: "Highway Access", city: "Monterrey", submarket: "Centro", latitude: 25.6789, longitude: -100.2845, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Convention Access", notes: "Fundidora park and Cintermex convention area access." }),
  ti({ name: "Valle Oriente Business Corridor Access", pointType: "Highway Access", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6389, longitude: -100.3234, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Business District", notes: "Eastern San Pedro office and retail corridor access." }),
  ti({ name: "San Pedro Corporate Corridor Access", pointType: "Highway Access", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6514, longitude: -100.3567, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Business District", notes: "Calzada del Valle luxury corporate and retail access." }),
  ti({ name: "Santa Catarina Industrial Highway Access", pointType: "Highway Access", city: "Santa Catarina", submarket: "Santa Catarina", latitude: 25.6734, longitude: -100.4567, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Industrial Corridor", notes: "Western industrial logistics corridor access.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
  ti({ name: "García Airport Industrial Park Access", pointType: "Highway Access", city: "García", submarket: "Airport Corridor", latitude: 25.8012, longitude: -100.5234, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Industrial Corridor", notes: "Northwest industrial park linked to MTY airport logistics." }),
  ti({ name: "Carretera Nacional Scenic Access", pointType: "Highway Access", city: "Monterrey", submarket: "San Pedro", latitude: 25.6234, longitude: -100.3456, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey", pointSubtype: "Scenic Corridor", notes: "Carretera Nacional mountain access for San Pedro leisure and corporate demand." }),
];

export function buildMonterreyTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, MONTERREY_TI_DELTA_RECORDS);
}
