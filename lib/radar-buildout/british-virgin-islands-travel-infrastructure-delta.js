/**
 * British Virgin Islands countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "British Virgin Islands";
const MARKET = "British Virgin Islands Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const BRITISH_VIRGIN_ISLANDS_TI_DELTA_RECORDS = [
  ti({ name: "Terrance B. Lettsome Airport Access", pointType: "Highway Access", city: "Beef Island", submarket: "Tortola", latitude: 18.4448, longitude: -64.543, sourceReference: "https://www.bviaa.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for BVI on Beef Island." }),
  ti({ name: "Road Town Ferry Terminal Access", pointType: "Ferry Terminal", city: "Road Town", submarket: "Tortola", latitude: 18.4245, longitude: -64.6189, sourceReference: "https://www.bvitourism.com/", pointSubtype: "Inter-Island Ferry", notes: "Main ferry hub linking Tortola to USVI and outer islands." }),
  ti({ name: "Soper's Hole Marina Access", pointType: "Port / Maritime", city: "West End", submarket: "Tortola", latitude: 18.4123, longitude: -64.6678, sourceReference: "https://www.bvitourism.com/", pointSubtype: "Marina", notes: "West End yacht marina and village access.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Nanny Cay Marina Access", pointType: "Port / Maritime", city: "Nanny Cay", submarket: "Tortola", latitude: 18.3789, longitude: -64.6234, sourceReference: "https://www.bvitourism.com/", pointSubtype: "Marina", notes: "South Tortola marina resort access." }),
  ti({ name: "Virgin Gorda Yacht Harbour Access", pointType: "Port / Maritime", city: "Spanish Town", submarket: "Virgin Gorda", latitude: 18.4512, longitude: -64.4345, sourceReference: "https://www.bvitourism.com/", pointSubtype: "Marina", notes: "Virgin Gorda marina and resort gateway." }),
  ti({ name: "Virgin Gorda Airport Access", pointType: "Highway Access", city: "Spanish Town", submarket: "Virgin Gorda", latitude: 18.4464, longitude: -64.4275, sourceReference: "https://www.bviaa.com/", pointSubtype: "Airport Access", notes: "Virgin Gorda air link for inter-island demand." }),
  ti({ name: "Jost Van Dyke Ferry Access", pointType: "Ferry Terminal", city: "Great Harbour", submarket: "Other Islands", latitude: 18.4456, longitude: -64.7512, sourceReference: "https://www.bvitourism.com/", pointSubtype: "Inter-Island Ferry", notes: "Ferry access to Jost Van Dyke leisure district." }),
  ti({ name: "BVI Port Authority Road Town Access", pointType: "Port / Maritime", city: "Road Town", submarket: "Tortola", latitude: 18.4223, longitude: -64.6145, sourceReference: "https://www.bviports.org/", pointSubtype: "Commercial Port", notes: "Cargo and cruise port logistics access.", useCaseTags: ["Industrial / Logistics","Cruise / Port"] }),
];

export function buildBritishVirginIslandsTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, BRITISH_VIRGIN_ISLANDS_TI_DELTA_RECORDS);
}
