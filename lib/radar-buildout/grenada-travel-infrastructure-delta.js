/**
 * Grenada countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Grenada";
const MARKET = "Grenada Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const GRENADA_TI_DELTA_RECORDS = [
  ti({ name: "Maurice Bishop International Airport Access", pointType: "Highway Access", city: "St. George's", submarket: "St. George's", latitude: 12.0042, longitude: -61.7862, sourceReference: "https://www.mbiagrenada.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Grenada stayover demand." }),
  ti({ name: "St. George's Cruise Terminal Access", pointType: "Port / Maritime", city: "St. George's", submarket: "St. George's", latitude: 12.0523, longitude: -61.7512, sourceReference: "https://www.puregrenada.com/", pointSubtype: "Cruise Terminal", notes: "Cruise terminal in St. George's harbour.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Grand Anse Resort Corridor Access", pointType: "Highway Access", city: "Grand Anse", submarket: "Grand Anse", latitude: 12.0234, longitude: -61.7678, sourceReference: "https://www.puregrenada.com/", pointSubtype: "Resort Corridor", notes: "Main resort beach road access." }),
  ti({ name: "Port Louis Marina Access", pointType: "Port / Maritime", city: "St. George's", submarket: "St. George's", latitude: 12.0567, longitude: -61.7489, sourceReference: "https://www.puregrenada.com/", pointSubtype: "Marina", notes: "Marina supporting yacht and leisure demand." }),
  ti({ name: "Prickly Bay Marina Access", pointType: "Port / Maritime", city: "L'Anse aux Epines", submarket: "South Coast", latitude: 11.9989, longitude: -61.7567, sourceReference: "https://www.puregrenada.com/", pointSubtype: "Marina", notes: "South coast marina village access." }),
  ti({ name: "Carriacou Ferry Gateway Access", pointType: "Ferry Terminal", city: "Hillsborough", submarket: "Other", latitude: 12.4789, longitude: -61.4567, sourceReference: "https://www.puregrenada.com/", pointSubtype: "Inter-Island Ferry", notes: "Ferry link to Carriacou and Petite Martinique." }),
  ti({ name: "Carenage Waterfront Access", pointType: "Highway Access", city: "St. George's", submarket: "St. George's", latitude: 12.0512, longitude: -61.7534, sourceReference: "https://www.puregrenada.com/", pointSubtype: "Waterfront Corridor", notes: "Historic waterfront access in capital harbour." }),
  ti({ name: "Grenada Industrial Zone Access", pointType: "Highway Access", city: "Perseverance", submarket: "St. George's", latitude: 12.0123, longitude: -61.7789, sourceReference: "https://www.gov.gd/", pointSubtype: "Industrial Corridor", notes: "Industrial connector for business travel.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
];

export function buildGrenadaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, GRENADA_TI_DELTA_RECORDS);
}
