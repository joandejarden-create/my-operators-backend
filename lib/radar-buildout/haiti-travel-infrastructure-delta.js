/**
 * Haiti Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Haiti";
const MARKET = "Haiti Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const HAITI_TI_DELTA_RECORDS = [
  ti({ name: "Toussaint Louverture International Airport Access", pointType: "Highway Access", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5801, longitude: -72.2925, sourceReference: "https://www.ofnac.gouv.ht/", pointSubtype: "Airport Access", notes: "Primary international gateway for Port-au-Prince metro demand." }),
  ti({ name: "Port of Port-au-Prince Access", pointType: "Port / Maritime", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5412, longitude: -72.3389, sourceReference: "https://www.haiti.org/", pointSubtype: "Commercial Port", notes: "Main cargo and ferry port for capital logistics and maritime demand.", useCaseTags: ["Industrial / Logistics","Cruise / Port"] }),
  ti({ name: "Cap-Haïtien International Airport Access", pointType: "Highway Access", city: "Cap-Haïtien", submarket: "Cap-Haïtien", latitude: 19.7581, longitude: -72.1944, sourceReference: "https://www.haiti.org/", pointSubtype: "Airport Access", notes: "North Haiti air gateway for heritage and Labadie cruise demand." }),
  ti({ name: "Labadee Cruise Destination Access", pointType: "Port / Maritime", city: "Labadee", submarket: "Cap-Haïtien", latitude: 19.7867, longitude: -72.2456, sourceReference: "https://www.royalcaribbean.com/", pointSubtype: "Cruise Terminal", notes: "Private cruise destination supporting north-coast day-call demand.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Citadelle Laferrière Scenic Highway Access", pointType: "Highway Access", city: "Milot", submarket: "Cap-Haïtien", latitude: 19.5734, longitude: -72.2445, sourceReference: "https://whc.unesco.org/en/list/180", pointSubtype: "Scenic Corridor", notes: "Mountain road access to Citadelle UNESCO heritage tourism." }),
  ti({ name: "Jacmel Coastal Highway Access", pointType: "Highway Access", city: "Jacmel", submarket: "Jacmel", latitude: 18.2345, longitude: -72.5356, sourceReference: "https://www.haiti.org/", pointSubtype: "Coastal Highway", notes: "South coast road connector for Jacmel arts and beach tourism." }),
  ti({ name: "Route Nationale 1 Cap-Haïtien Corridor Access", pointType: "Highway Access", city: "Cap-Haïtien", submarket: "Cap-Haïtien", latitude: 19.7612, longitude: -72.2089, sourceReference: "https://www.haiti.org/", pointSubtype: "National Highway", notes: "Primary north highway linking Cap-Haïtien to Port-au-Prince corridor." }),
  ti({ name: "Ouanaminthe Border Crossing Access", pointType: "Highway Access", city: "Ouanaminthe", submarket: "Other", latitude: 19.5489, longitude: -71.7234, sourceReference: "https://www.haiti.org/", pointSubtype: "Border Crossing", notes: "Dominican border commerce connector for cross-border transient demand.", useCaseTags: ["Industrial / Logistics"] }),
];

export function buildHaitiTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, HAITI_TI_DELTA_RECORDS);
}
