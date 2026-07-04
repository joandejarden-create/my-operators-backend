/**
 * U.S. Virgin Islands Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "U.S. Virgin Islands";
const MARKET = "U.S. Virgin Islands Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const US_VIRGIN_ISLANDS_TI_DELTA_RECORDS = [
  ti({ name: "Cyril E. King International Airport Access", pointType: "Highway Access", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3373, longitude: -64.9734, sourceReference: "https://www.viport.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for St. Thomas cruise and resort demand." }),
  ti({ name: "Charlotte Amalie Cruise Port Access", pointType: "Port / Maritime", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3312, longitude: -64.9289, sourceReference: "https://www.visitusvi.com/", pointSubtype: "Cruise Terminal", notes: "Havensight cruise terminal supporting day-call and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Red Hook Ferry Terminal Access", pointType: "Ferry Terminal", city: "Red Hook", submarket: "St. Thomas", latitude: 18.3189, longitude: -64.8512, sourceReference: "https://www.visitusvi.com/", pointSubtype: "Inter-Island Ferry", notes: "Ferry hub linking St. Thomas to St. John and BVI." }),
  ti({ name: "Henry E. Rohlsen International Airport Access", pointType: "Highway Access", city: "St. Croix", submarket: "St. Croix", latitude: 17.7019, longitude: -64.7986, sourceReference: "https://www.viport.com/", pointSubtype: "Airport Access", notes: "St. Croix international air gateway for south USVI demand." }),
  ti({ name: "Frederiksted Cruise Pier Access", pointType: "Port / Maritime", city: "Frederiksted", submarket: "St. Croix", latitude: 17.7123, longitude: -64.8834, sourceReference: "https://www.visitusvi.com/", pointSubtype: "Cruise Terminal", notes: "West St. Croix cruise berth for heritage and beach tourism.", useCaseTags: ["Cruise / Port"] }),
  ti({ name: "Cruz Bay Ferry Terminal Access", pointType: "Ferry Terminal", city: "Cruz Bay", submarket: "St. John", latitude: 18.3312, longitude: -64.7934, sourceReference: "https://www.visitusvi.com/", pointSubtype: "Inter-Island Ferry", notes: "St. John ferry landing for national park and villa demand." }),
  ti({ name: "Christiansted Waterfront Highway Access", pointType: "Highway Access", city: "Christiansted", submarket: "St. Croix", latitude: 17.7467, longitude: -64.7034, sourceReference: "https://www.visitusvi.com/", pointSubtype: "Historic Corridor", notes: "Historic waterfront road access for east St. Croix tourism." }),
  ti({ name: "Magens Bay Scenic Road Access", pointType: "Highway Access", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3689, longitude: -64.9234, sourceReference: "https://www.visitusvi.com/", pointSubtype: "Scenic Corridor", notes: "North shore scenic access to Magens Bay beach demand." }),
];

export function buildUsVirginIslandsTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, US_VIRGIN_ISLANDS_TI_DELTA_RECORDS);
}
