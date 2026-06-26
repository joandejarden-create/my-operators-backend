/**
 * Trinidad and Tobago countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Trinidad and Tobago";
const MARKET = "Trinidad and Tobago Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const TRINIDAD_AND_TOBAGO_TI_DELTA_RECORDS = [
  ti({ name: "Piarco International Airport Access", pointType: "Highway Access", city: "Piarco", submarket: "East-West Corridor", latitude: 10.5954, longitude: -61.3372, sourceReference: "https://www.ttairport.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Trinidad." }),
  ti({ name: "ANR Robinson Tobago Airport Access", pointType: "Highway Access", city: "Crown Point", submarket: "Tobago", latitude: 11.1497, longitude: -60.8322, sourceReference: "https://www.tobagoairport.com/", pointSubtype: "Airport Access", notes: "Tobago island air gateway at Crown Point." }),
  ti({ name: "Port of Spain Waterfront Access", pointType: "Highway Access", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6512, longitude: -61.5123, sourceReference: "https://www.visittrinidad.tt/", pointSubtype: "Urban Corridor", notes: "Capital waterfront and business district access." }),
  ti({ name: "East-West Corridor Highway Access", pointType: "Highway Access", city: "St. Augustine", submarket: "East-West Corridor", latitude: 10.6412, longitude: -61.3989, sourceReference: "https://www.gov.tt/", pointSubtype: "Regional Corridor", notes: "Main suburban connector between Piarco and Port of Spain." }),
  ti({ name: "Tobago Ferry Terminal Access", pointType: "Ferry Terminal", city: "Scarborough", submarket: "Tobago", latitude: 11.1789, longitude: -60.7289, sourceReference: "https://www.visittobago.gov.tt/", pointSubtype: "Inter-Island Ferry", notes: "Trinidad-Tobago ferry link at Scarborough." }),
  ti({ name: "Pigeon Point Resort Corridor Access", pointType: "Highway Access", city: "Crown Point", submarket: "Tobago", latitude: 11.1678, longitude: -60.8345, sourceReference: "https://www.visittobago.gov.tt/", pointSubtype: "Resort Corridor", notes: "Main Tobago resort beach access." }),
  ti({ name: "Point Lisas Industrial Port Access", pointType: "Port / Maritime", city: "Point Lisas", submarket: "South Trinidad", latitude: 10.4012, longitude: -61.4678, sourceReference: "https://www.gov.tt/", pointSubtype: "Industrial Port", notes: "Industrial port and estate access.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
  ti({ name: "Chaguaramas Marina Access", pointType: "Port / Maritime", city: "Chaguaramas", submarket: "Other", latitude: 10.6789, longitude: -61.6234, sourceReference: "https://www.visittrinidad.tt/", pointSubtype: "Marina", notes: "West coast marina and leisure access.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
];

export function buildTrinidadAndTobagoTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, TRINIDAD_AND_TOBAGO_TI_DELTA_RECORDS);
}
