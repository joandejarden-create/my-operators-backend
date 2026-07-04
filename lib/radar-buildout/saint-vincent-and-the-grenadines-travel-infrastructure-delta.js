/**
 * Saint Vincent and the Grenadines countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Saint Vincent and the Grenadines";
const MARKET = "Saint Vincent and the Grenadines Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const SAINT_VINCENT_AND_THE_GRENADINES_TI_DELTA_RECORDS = [
  ti({ name: "Argyle International Airport Access", pointType: "Highway Access", city: "Argyle", submarket: "Other", latitude: 13.1567, longitude: -61.1512, sourceReference: "https://www.svgairport.com/", pointSubtype: "Airport Access", notes: "Primary international gateway since 2017." }),
  ti({ name: "Kingstown Cruise Berth Access", pointType: "Port / Maritime", city: "Kingstown", submarket: "Kingstown", latitude: 13.1567, longitude: -61.2234, sourceReference: "https://www.discoversvg.com/", pointSubtype: "Cruise Terminal", notes: "Capital cruise berth access.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Bequia Ferry and Marina Access", pointType: "Ferry Terminal", city: "Port Elizabeth", submarket: "Grenadines", latitude: 13.0123, longitude: -61.2456, sourceReference: "https://www.discoversvg.com/", pointSubtype: "Inter-Island Ferry", notes: "Main Grenadines ferry and yacht gateway." }),
  ti({ name: "Admiralty Bay Marina Access", pointType: "Port / Maritime", city: "Port Elizabeth", submarket: "Grenadines", latitude: 13.0089, longitude: -61.2412, sourceReference: "https://www.discoversvg.com/", pointSubtype: "Marina", notes: "Bequia marina supporting sailing tourism.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Canouan Airport and Marina Connector", pointType: "Highway Access", city: "Canouan", submarket: "Grenadines", latitude: 12.7012, longitude: -61.3234, sourceReference: "https://www.discoversvg.com/", pointSubtype: "Airport-Resort Corridor", notes: "Grenadines resort island air and marina access." }),
  ti({ name: "Union Island Clifton Harbour Access", pointType: "Ferry Terminal", city: "Clifton", submarket: "Grenadines", latitude: 12.5989, longitude: -61.4234, sourceReference: "https://www.discoversvg.com/", pointSubtype: "Harbour Access", notes: "Southern Grenadines harbour and inter-island ferry node." }),
  ti({ name: "Kingstown Waterfront Access", pointType: "Highway Access", city: "Kingstown", submarket: "Kingstown", latitude: 13.1589, longitude: -61.2267, sourceReference: "https://www.discoversvg.com/", pointSubtype: "Waterfront Corridor", notes: "Capital waterfront and market district access." }),
  ti({ name: "Calliaqua Industrial Zone Access", pointType: "Highway Access", city: "Calliaqua", submarket: "Kingstown", latitude: 13.1289, longitude: -61.1912, sourceReference: "https://www.gov.vc/", pointSubtype: "Industrial Corridor", notes: "South coast industrial connector.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
];

export function buildSaintVincentAndTheGrenadinesTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, SAINT_VINCENT_AND_THE_GRENADINES_TI_DELTA_RECORDS);
}
