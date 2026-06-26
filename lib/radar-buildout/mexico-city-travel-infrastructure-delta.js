/**
 * Mexico City Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Mexico City";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const MEXICO_CITY_TI_DELTA_RECORDS = [
  ti({ name: "Benito Juárez International Airport Access", pointType: "Highway Access", city: "Mexico City", submarket: "Airport Corridor", latitude: 19.4363, longitude: -99.0721, sourceReference: "https://www.aicm.com.mx/", pointSubtype: "Airport Access", notes: "Primary international gateway for CDMX corporate and leisure demand." }),
  ti({ name: "Paseo de la Reforma Corridor Access", pointType: "Highway Access", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.427, longitude: -99.1677, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", pointSubtype: "Urban Artery", notes: "Main reforma corridor linking Polanco, Reforma hotels, and Chapultepec." }),
  ti({ name: "Centro Santa Fe Highway Access", pointType: "Highway Access", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3594, longitude: -99.2767, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", pointSubtype: "Business District", notes: "West CDMX corporate campus access via Santa Fe toll road network." }),
  ti({ name: "World Trade Center Mexico Access", pointType: "Highway Access", city: "Mexico City", submarket: "Insurgentes / WTC", latitude: 19.3942, longitude: -99.1735, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", pointSubtype: "Convention Access", notes: "Insurgentes Sur access for WTC meetings and group demand." }),
  ti({ name: "Centro Histórico Transit Hub Access", pointType: "Highway Access", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4326, longitude: -99.1332, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", pointSubtype: "Historic Core", notes: "Zócalo and historic core multimodal access for cultural tourism." }),
  ti({ name: "Chapultepec Park Connector Access", pointType: "Highway Access", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.4204, longitude: -99.1817, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", pointSubtype: "Park Corridor", notes: "Chapultepec access linking Reforma hotels to museum and park demand." }),
  ti({ name: "Coyoacán Cultural District Access", pointType: "Highway Access", city: "Coyoacán", submarket: "Coyoacán / San Ángel", latitude: 19.355, longitude: -99.1623, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", pointSubtype: "Cultural Corridor", notes: "Southern cultural corridor access for Frida Kahlo and UNAM visitation." }),
  ti({ name: "Teotihuacán Day-Trip Highway Access", pointType: "Highway Access", city: "San Juan Teotihuacán", submarket: "Other", latitude: 19.6925, longitude: -98.8437, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-state/teotihuacan", pointSubtype: "Tourism Corridor", notes: "Northeast tourism corridor for day-trip and group heritage demand from CDMX hotels." }),
];

export function buildMexicoCityTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, MEXICO_CITY_TI_DELTA_RECORDS);
}
