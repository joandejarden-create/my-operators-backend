/**
 * Cuba Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Cuba";
const MARKET = "Cuba Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const CUBA_TI_DELTA_RECORDS = [
  ti({ name: "José Martí International Airport Access", pointType: "Highway Access", city: "Havana", submarket: "Havana", latitude: 22.9892, longitude: -82.4091, sourceReference: "https://www.havana-airport.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Havana metro and west Cuba demand." }),
  ti({ name: "Port of Havana Cruise Terminal Access", pointType: "Port / Maritime", city: "Havana", submarket: "Havana", latitude: 23.1389, longitude: -82.3472, sourceReference: "https://www.cubatravel.cu/en", pointSubtype: "Cruise Terminal", notes: "Main cruise berth supporting Havana heritage and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Juan Gualberto Gómez Airport Access", pointType: "Highway Access", city: "Varadero", submarket: "Varadero", latitude: 23.0344, longitude: -81.4353, sourceReference: "https://www.varaderointernationalairport.com/", pointSubtype: "Airport Access", notes: "Resort-corridor air gateway for Matanzas–Varadero beach inventory." }),
  ti({ name: "Varadero Resort Highway Corridor Access", pointType: "Highway Access", city: "Varadero", submarket: "Varadero", latitude: 23.1397, longitude: -81.2861, sourceReference: "https://www.cubatravel.cu/en", pointSubtype: "Resort Corridor", notes: "Primary road access serving Varadero all-inclusive resort strip." }),
  ti({ name: "Trinidad Heritage Town Access", pointType: "Highway Access", city: "Trinidad", submarket: "Trinidad", latitude: 21.8022, longitude: -79.9831, sourceReference: "https://www.cubatravel.cu/en", pointSubtype: "Scenic Corridor", notes: "Central highway node linking Trinidad UNESCO core and Ancón beach." }),
  ti({ name: "Antonio Maceo Airport Access", pointType: "Highway Access", city: "Santiago de Cuba", submarket: "Santiago de Cuba", latitude: 19.9698, longitude: -75.8354, sourceReference: "https://www.cubatravel.cu/en", pointSubtype: "Airport Access", notes: "East Cuba air gateway for Santiago and Oriente resort demand." }),
  ti({ name: "Jardines del Rey Airport Access", pointType: "Highway Access", city: "Cayo Coco", submarket: "Other", latitude: 22.4612, longitude: -78.3289, sourceReference: "https://www.cubatravel.cu/en", pointSubtype: "Airport Access", notes: "Northern keys air access for Cayo Coco/Cayo Guillermo resort nodes." }),
  ti({ name: "Mariel Port and ZEDM Logistics Access", pointType: "Highway Access", city: "Mariel", submarket: "Other", latitude: 22.9878, longitude: -82.7512, sourceReference: "https://www.zedmariel.cu/", pointSubtype: "Industrial Corridor", notes: "West Havana industrial port connector for logistics and business travel.", useCaseTags: ["Industrial / Logistics"] }),
];

export function buildCubaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, CUBA_TI_DELTA_RECORDS);
}
