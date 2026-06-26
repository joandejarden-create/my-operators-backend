/**
 * Honduras Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Honduras";
const MARKET = "Honduras Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const HONDURAS_TI_DELTA_RECORDS = [
  ti({ name: "Ramón Villeda Morales International Airport (SAP) Access", pointType: "Highway Access", city: "San Pedro Sula", submarket: "San Pedro Sula", latitude: 15.4526, longitude: -87.9236, sourceReference: "https://www.interairports.com/en/airports/ramon-villeda-morales-international-airport", pointSubtype: "Airport Access", notes: "Northern industrial gateway supporting San Pedro Sula corporate and logistics hotel demand." }),
  ti({ name: "Toncontín International Airport (TGU) Access", pointType: "Highway Access", city: "Tegucigalpa", submarket: "Tegucigalpa", latitude: 14.0608, longitude: -87.2172, sourceReference: "https://www.interairports.com/en/airports/toncontin-international-airport", pointSubtype: "Airport Access", notes: "Capital metro air gateway for government, NGO, and corporate transient lodging." }),
  ti({ name: "Mahogany Bay Cruise Terminal Access", pointType: "Port / Maritime", city: "Roatán", submarket: "Roatán", latitude: 16.3289, longitude: -86.5389, sourceReference: "https://honduras.travel/en/destination/roatan/", pointSubtype: "Cruise Terminal", notes: "Primary Roatán cruise berth driving pre/post-cruise and day-call resort demand.", useCaseTags: ["Cruise / Port", "Resort / Leisure"] }),
  ti({ name: "Port of Puerto Cortés Access", pointType: "Port / Maritime", city: "Puerto Cortés", submarket: "La Ceiba", latitude: 15.8389, longitude: -87.9512, sourceReference: "https://www.enp.hn/", pointSubtype: "Commercial Port", notes: "Honduras principal seaport supporting logistics crew and industrial extended-stay demand.", useCaseTags: ["Industrial / Logistics", "Cruise / Port"] }),
  ti({ name: "Copán Ruinas Heritage Highway Access", pointType: "Highway Access", city: "Copán Ruinas", submarket: "Copán", latitude: 14.8389, longitude: -89.1412, sourceReference: "https://honduras.travel/en/destination/copan/", pointSubtype: "Heritage Corridor", notes: "Western border road access to Copán Maya UNESCO archaeology tourism lodging." }),
  ti({ name: "CA-5 Pan-American Tegucigalpa Corridor Access", pointType: "Highway Access", city: "Tegucigalpa", submarket: "Tegucigalpa", latitude: 14.0712, longitude: -87.1889, sourceReference: "https://www.sepris.gob.hn/", pointSubtype: "National Highway", notes: "Pan-American highway node through capital metro linking north-south domestic flows." }),
  ti({ name: "Juan Manuel Gálvez International Airport (RTB) Access", pointType: "Highway Access", city: "Roatán", submarket: "Roatán", latitude: 16.3168, longitude: -86.523, sourceReference: "https://www.interairports.com/en/airports/juan-manuel-galvez-international-airport", pointSubtype: "Airport Access", notes: "Bay Islands air gateway for Roatán resort and dive tourism inventory." }),
  ti({ name: "CA-13 North Coast Highway La Ceiba Access", pointType: "Highway Access", city: "La Ceiba", submarket: "La Ceiba", latitude: 15.7812, longitude: -86.7923, sourceReference: "https://honduras.travel/en/destination/la-ceiba/", pointSubtype: "Coastal Highway", notes: "North coast highway connector linking La Ceiba to Utila/Bay Islands ferry and eco-tourism." }),
];

export function buildHondurasTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, HONDURAS_TI_DELTA_RECORDS);
}
