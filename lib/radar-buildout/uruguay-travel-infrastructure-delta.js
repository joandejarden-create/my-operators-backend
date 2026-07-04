/**
 * Uruguay Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Uruguay";
const MARKET = "Uruguay Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const URUGUAY_TI_DELTA_RECORDS = [
  ti({ name: "Carrasco International Airport Access", pointType: "Highway Access", city: "Montevideo", submarket: "Montevideo", latitude: -34.8383, longitude: -56.0306, sourceReference: "https://www.aeropuertodecarrasco.com.uy/", pointSubtype: "Airport Access", notes: "MVD Carrasco airport highway access for Montevideo metro and east-coast resort flows." }),
  ti({ name: "Port of Montevideo Maritime Terminal Access", pointType: "Port / Maritime", city: "Montevideo", submarket: "Montevideo", latitude: -34.9061, longitude: -56.2136, sourceReference: "https://www.anp.com.uy/", pointSubtype: "Container Port", notes: "Primary capital container and cruise port on the Rio de la Plata.", useCaseTags: ["Industrial / Logistics", "Cruise / Port"] }),
  ti({ name: "Montevideo Rambla Coastal Corridor Access", pointType: "Highway Access", city: "Montevideo", submarket: "Montevideo", latitude: -34.9128, longitude: -56.1503, sourceReference: "https://www.gub.uy/ministerio-vivienda-ordenamiento-territorial", pointSubtype: "Urban Corridor", notes: "Rambla waterfront vehicular corridor serving Montevideo coastal hotel and event demand.", useCaseTags: ["Urban / Corporate", "Resort / Leisure"] }),
  ti({ name: "Punta del Este Marina Access", pointType: "Port / Maritime", city: "Punta del Este", submarket: "Punta del Este", latitude: -34.9703, longitude: -54.9519, sourceReference: "https://www.laposta.com.uy/", pointSubtype: "Marina", notes: "Yacht and marina hub for Punta del Este seasonal luxury resort lodging.", useCaseTags: ["Resort / Leisure", "Cruise / Port"] }),
  ti({ name: "Colonia del Sacramento Ferry Gateway Access", pointType: "Port / Maritime", city: "Colonia del Sacramento", submarket: "Colonia", latitude: -34.4714, longitude: -57.8436, sourceReference: "https://www.buquebus.com/", pointSubtype: "Ferry Terminal", notes: "Buquebus ferry terminal linking Buenos Aires day-trip and overnight heritage tourism.", useCaseTags: ["Cruise / Port", "Heritage / Cultural Tourism"] }),
  ti({ name: "Ruta 9 Interbalnearia Punta del Este Corridor Access", pointType: "Highway Access", city: "Atlántida", submarket: "Punta del Este", latitude: -34.775, longitude: -55.758, sourceReference: "https://www.gub.uy/ministerio-transporte-obras-publicas", pointSubtype: "Coastal Corridor", notes: "Interbalnearia highway node connecting Montevideo to Punta del Este resort markets." }),
  ti({ name: "Route 1 Colonia Western Gateway Highway Access", pointType: "Highway Access", city: "Colonia del Sacramento", submarket: "Colonia", latitude: -34.471, longitude: -57.844, sourceReference: "https://www.gub.uy/ministerio-transporte-obras-publicas", pointSubtype: "Border Gateway", notes: "RN-1 western corridor vehicular access for Colonia heritage and cross-Rio ferry flows." }),
  ti({ name: "Piriápolis Coastal Highway Corridor Access", pointType: "Highway Access", city: "Piriápolis", submarket: "Other", latitude: -34.868, longitude: -55.274, sourceReference: "https://www.gub.uy/ministerio-transporte-obras-publicas", pointSubtype: "Coastal Corridor", notes: "RN-10 coastal connector between Montevideo and Punta del Este secondary resort markets." }),
];

export function buildUruguayTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, URUGUAY_TI_DELTA_RECORDS);
}
