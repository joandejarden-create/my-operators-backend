/**
 * Belize Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Belize";
const MARKET = "Belize Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const BELIZE_TI_DELTA_RECORDS = [
  ti({ name: "Philip S. W. Goldson International Airport (BZE) Access", pointType: "Highway Access", city: "Ladyville", submarket: "Belize City", latitude: 17.5392, longitude: -88.3082, sourceReference: "https://www.pgiaservices.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Belize City metro and mainland resort corridors." }),
  ti({ name: "Belize Tourism Village Cruise Terminal Access", pointType: "Port / Maritime", city: "Belize City", submarket: "Belize City", latitude: 17.4925, longitude: -88.1856, sourceReference: "https://www.travelbelize.org/", pointSubtype: "Cruise Terminal", notes: "Main cruise berth supporting Belize City pre/post-stay and shore-excursion lodging.", useCaseTags: ["Cruise / Port", "Resort / Leisure"] }),
  ti({ name: "San Pedro Ambergris Water Taxi Terminal Access", pointType: "Port / Maritime", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9169, longitude: -87.9619, sourceReference: "https://www.belizeexpress.com/", pointSubtype: "Water Taxi", notes: "Island water-taxi hub linking Ambergris Caye resort inventory to mainland arrivals.", useCaseTags: ["Cruise / Port", "Resort / Leisure"] }),
  ti({ name: "Placencia Peninsula Highway Access", pointType: "Highway Access", city: "Placencia", submarket: "Placencia", latitude: 16.5234, longitude: -88.3612, sourceReference: "https://www.travelbelize.org/", pointSubtype: "Peninsula Corridor", notes: "Southern Highway turnoff serving Placencia peninsula beach and eco-resort demand." }),
  ti({ name: "Southern Highway PGIA Access Corridor", pointType: "Highway Access", city: "Ladyville", submarket: "Belize City", latitude: 17.5345, longitude: -88.3123, sourceReference: "https://www.mowt.gov.bz/", pointSubtype: "Airport Connector", notes: "Southern Highway connector linking Philip Goldson International Airport to national road network." }),
  ti({ name: "Belize City Fort Street Water Taxi Pier Access", pointType: "Port / Maritime", city: "Belize City", submarket: "Belize City", latitude: 17.4942, longitude: -88.1867, sourceReference: "https://www.belizeexpress.com/", pointSubtype: "Water Taxi", notes: "Mainland departure pier for San Pedro and Caye Caulker island resort traffic.", useCaseTags: ["Cruise / Port", "Airport / Transit"] }),
  ti({ name: "Western Highway San Ignacio Corridor Access", pointType: "Highway Access", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1567, longitude: -89.0789, sourceReference: "https://www.travelbelize.org/", pointSubtype: "National Highway", notes: "Primary westbound highway node for Cayo District eco-tourism and Maya site lodging." }),
  ti({ name: "Hummingbird Highway Coastal Connector Access", pointType: "Highway Access", city: "Dangriga", submarket: "Other", latitude: 16.9697, longitude: -88.2319, sourceReference: "https://www.mowt.gov.bz/", pointSubtype: "Scenic Corridor", notes: "Central highway link between Belize City and southern coast resort markets." }),
];

export function buildBelizeTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, BELIZE_TI_DELTA_RECORDS);
}
