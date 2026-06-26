/**
 * Bonaire Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Bonaire";
const MARKET = "Bonaire Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const BONAIRE_TI_DELTA_RECORDS = [
  ti({ name: "Flamingo International Airport Access", pointType: "Highway Access", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.131, longitude: -68.2685, sourceReference: "https://bonaireinternationalairport.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Bonaire dive and leisure demand." }),
  ti({ name: "Kralendijk Cruise Port Access", pointType: "Port / Maritime", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1512, longitude: -68.2789, sourceReference: "https://www.tourismbonaire.com/", pointSubtype: "Cruise Terminal", notes: "Main cruise pier supporting day-call and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Klein Bonaire Ferry Access", pointType: "Ferry Terminal", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1589, longitude: -68.3012, sourceReference: "https://www.tourismbonaire.com/", pointSubtype: "Water Taxi", notes: "Water taxi access to Klein Bonaire snorkel and beach demand." }),
  ti({ name: "Lac Bay Coastal Highway Access", pointType: "Highway Access", city: "Sorobon", submarket: "Other", latitude: 12.1089, longitude: -68.2312, sourceReference: "https://www.tourismbonaire.com/", pointSubtype: "Scenic Corridor", notes: "East coast road access to Lac Bay windsurf and mangrove tourism." }),
  ti({ name: "Rincon Village Highway Access", pointType: "Highway Access", city: "Rincon", submarket: "Rincon", latitude: 12.2312, longitude: -68.3312, sourceReference: "https://www.tourismbonaire.com/", pointSubtype: "Heritage Corridor", notes: "North road access to Rincon heritage and agricultural tourism." }),
  ti({ name: "Washington Slagbaai Park Road Access", pointType: "Highway Access", city: "Rincon", submarket: "Washington Slagbaai", latitude: 12.2512, longitude: -68.3512, sourceReference: "https://www.stinapa.org/", pointSubtype: "National Park Access", notes: "Park entrance road for northwest nature and dive tourism." }),
  ti({ name: "Kralendijk South Dive Coast Road Access", pointType: "Highway Access", city: "Kralendijk", submarket: "Other", latitude: 12.1712, longitude: -68.3012, sourceReference: "https://www.tourismbonaire.com/", pointSubtype: "Coastal Highway", notes: "South coast dive site road connector along Bonaire National Marine Park." }),
  ti({ name: "Bonaire Free Zone Port Access", pointType: "Port / Maritime", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1389, longitude: -68.2589, sourceReference: "https://www.bonairegov.com/", pointSubtype: "Commercial Port", notes: "Industrial port and free-zone logistics access.", useCaseTags: ["Industrial / Logistics"] }),
];

export function buildBonaireTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, BONAIRE_TI_DELTA_RECORDS);
}
