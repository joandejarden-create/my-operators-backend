/**
 * Dominica countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Dominica";
const MARKET = "Dominica Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const DOMINICA_TI_DELTA_RECORDS = [
  ti({ name: "Douglas-Charles Airport Access", pointType: "Highway Access", city: "Marigot", submarket: "Portsmouth", latitude: 15.547, longitude: -61.3, sourceReference: "https://www.dominica.gov.dm/", pointSubtype: "Airport Access", notes: "Primary international airport in north-east." }),
  ti({ name: "Canefield Airport Access", pointType: "Highway Access", city: "Canefield", submarket: "Roseau", latitude: 15.3367, longitude: -61.3922, sourceReference: "https://www.dominica.gov.dm/", pointSubtype: "Airport Access", notes: "Regional airport near Roseau capital." }),
  ti({ name: "Roseau Cruise Berth Access", pointType: "Port / Maritime", city: "Roseau", submarket: "Roseau", latitude: 15.2978, longitude: -61.3871, sourceReference: "https://www.discoverdominica.com/", pointSubtype: "Cruise Terminal", notes: "Capital cruise berth access.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Portsmouth Bay Marina Access", pointType: "Port / Maritime", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5789, longitude: -61.4567, sourceReference: "https://www.discoverdominica.com/", pointSubtype: "Marina", notes: "North coast marina and yacht access." }),
  ti({ name: "Cabrits National Park Access", pointType: "Highway Access", city: "Portsmouth", submarket: "Portsmouth", latitude: 15.5845, longitude: -61.4678, sourceReference: "https://www.discoverdominica.com/", pointSubtype: "Scenic Corridor", notes: "Historic fort and north gateway scenic access." }),
  ti({ name: "Scotts Head Marine Reserve Access", pointType: "Highway Access", city: "Scotts Head", submarket: "South Coast", latitude: 15.2123, longitude: -61.3789, sourceReference: "https://www.discoverdominica.com/", pointSubtype: "Coastal Corridor", notes: "South-west coastal dive and snorkel access." }),
  ti({ name: "Calibishie East Coast Access", pointType: "Highway Access", city: "Calibishie", submarket: "East Coast", latitude: 15.5923, longitude: -61.3456, sourceReference: "https://www.discoverdominica.com/", pointSubtype: "Coastal Corridor", notes: "Atlantic coast village and beach access." }),
  ti({ name: "Dominica Industrial Estate Access", pointType: "Highway Access", city: "Fond Cole", submarket: "Roseau", latitude: 15.2912, longitude: -61.4012, sourceReference: "https://www.dominica.gov.dm/", pointSubtype: "Industrial Corridor", notes: "Industrial zone connector near capital.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
];

export function buildDominicaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, DOMINICA_TI_DELTA_RECORDS);
}
