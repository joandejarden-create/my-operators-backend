/**
 * Puerto Vallarta / Riviera Nayarit Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Puerto Vallarta / Riviera Nayarit";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const PUERTO_VALLARTA_RIVIERA_NAYARIT_TI_DELTA_RECORDS = [
  ti({ name: "Puerto Vallarta International Airport Access", pointType: "Highway Access", city: "Puerto Vallarta", submarket: "Airport Corridor", latitude: 20.6801, longitude: -105.2544, sourceReference: "https://www.aeropuertosgap.com.mx/", pointSubtype: "Airport Access", notes: "Primary international gateway for Puerto Vallarta and Riviera Nayarit resort demand." }),
  ti({ name: "Malecón Waterfront Access", pointType: "Highway Access", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6113, longitude: -105.2303, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta", pointSubtype: "Resort Corridor", notes: "Main waterfront and hotel zone road access along Banderas Bay." }),
  ti({ name: "Puerto Vallarta Cruise Terminal Access", pointType: "Port / Maritime", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6145, longitude: -105.2456, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta", pointSubtype: "Cruise Terminal", notes: "Maritime terminal supporting cruise day-call and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Marina Vallarta Harbor Access", pointType: "Port / Maritime", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6653, longitude: -105.2419, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta", pointSubtype: "Marina", notes: "Yacht marina and north hotel zone harbor access." }),
  ti({ name: "Nuevo Vallarta Resort Corridor Access", pointType: "Highway Access", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.6989, longitude: -105.2717, sourceReference: "https://www.rivieranayarit.com/", pointSubtype: "Resort Corridor", notes: "Highway 200 access to master-planned Nuevo Vallarta resort inventory." }),
  ti({ name: "Punta de Mita Luxury Peninsula Access", pointType: "Highway Access", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7736, longitude: -105.5286, sourceReference: "https://www.rivieranayarit.com/", pointSubtype: "Resort Corridor", notes: "Coastal road access to Punta de Mita ultra-luxury resort demand." }),
  ti({ name: "La Cruz de Huanacaxtle Marina Access", pointType: "Port / Maritime", city: "La Cruz de Huanacaxtle", submarket: "Riviera Nayarit North Coast", latitude: 20.7534, longitude: -105.3789, sourceReference: "https://www.rivieranayarit.com/", pointSubtype: "Marina", notes: "North bay marina access for yacht and Riviera Nayarit leisure demand." }),
  ti({ name: "Riviera Nayarit North Coast Highway Access", pointType: "Highway Access", city: "Bucerías", submarket: "Riviera Nayarit North Coast", latitude: 20.7512, longitude: -105.3345, sourceReference: "https://www.rivieranayarit.com/", pointSubtype: "Scenic Corridor", notes: "North coast scenic highway linking Bucerías to Sayulita and San Blas." }),
];

export function buildPuertoVallartaRivieraNayaritTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, PUERTO_VALLARTA_RIVIERA_NAYARIT_TI_DELTA_RECORDS);
}
