/**
 * Saint Kitts and Nevis countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Saint Kitts and Nevis";
const MARKET = "Saint Kitts and Nevis Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const SAINT_KITTS_AND_NEVIS_TI_DELTA_RECORDS = [
  ti({ name: "Robert L. Bradshaw International Airport Access", pointType: "Highway Access", city: "Basseterre", submarket: "Basseterre", latitude: 17.3112, longitude: -62.7189, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Airport Access", notes: "Primary international gateway for St. Kitts." }),
  ti({ name: "Port Zante Cruise Terminal Access", pointType: "Port / Maritime", city: "Basseterre", submarket: "Basseterre", latitude: 17.2967, longitude: -62.7234, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Cruise Terminal", notes: "Main cruise terminal in Basseterre harbour.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Frigate Bay Resort Corridor Access", pointType: "Highway Access", city: "Frigate Bay", submarket: "Frigate Bay", latitude: 17.2789, longitude: -62.6789, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Resort Corridor", notes: "Primary resort beach strip road access." }),
  ti({ name: "Christophe Harbour Marina Access", pointType: "Port / Maritime", city: "Basseterre", submarket: "Frigate Bay", latitude: 17.2567, longitude: -62.6234, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Marina", notes: "Luxury marina on south-east peninsula.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "St. Kitts Nevis Ferry Terminal Access", pointType: "Ferry Terminal", city: "Basseterre", submarket: "Basseterre", latitude: 17.2978, longitude: -62.7245, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Inter-Island Ferry", notes: "Ferry link between St. Kitts and Nevis." }),
  ti({ name: "Vance W. Amory Airport Nevis Access", pointType: "Highway Access", city: "Newcastle", submarket: "Nevis", latitude: 17.2012, longitude: -62.5934, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Airport Access", notes: "Nevis island air gateway." }),
  ti({ name: "Charlestown Nevis Waterfront Access", pointType: "Highway Access", city: "Charlestown", submarket: "Nevis", latitude: 17.1456, longitude: -62.6234, sourceReference: "https://www.stkittstourism.kn/", pointSubtype: "Waterfront Corridor", notes: "Nevis capital waterfront and heritage access." }),
  ti({ name: "Basseterre Deep Water Port Access", pointType: "Port / Maritime", city: "Basseterre", submarket: "Basseterre", latitude: 17.2912, longitude: -62.7312, sourceReference: "https://www.gov.kn/", pointSubtype: "Commercial Port", notes: "Cargo port supporting business travel.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
];

export function buildSaintKittsAndNevisTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, SAINT_KITTS_AND_NEVIS_TI_DELTA_RECORDS);
}
