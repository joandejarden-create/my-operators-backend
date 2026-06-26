/**
 * Los Cabos Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Los Cabos";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const LOS_CABOS_TI_DELTA_RECORDS = [
  ti({ name: "Los Cabos International Airport Access", pointType: "Highway Access", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.1518, longitude: -109.7211, sourceReference: "https://www.asur.com.mx/", pointSubtype: "Airport Access", notes: "Primary international gateway for Los Cabos luxury resort demand." }),
  ti({ name: "Cabo San Lucas Marina Access", pointType: "Port / Maritime", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8797, longitude: -109.9083, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", pointSubtype: "Marina", notes: "Marina and sportfishing harbor access for Cabo San Lucas leisure demand.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Cabo San Lucas Cruise Terminal Access", pointType: "Port / Maritime", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8756, longitude: -109.9112, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", pointSubtype: "Cruise Terminal", notes: "Cruise ship terminal supporting day-call and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Tourist Corridor Transpeninsular Highway Access", pointType: "Highway Access", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9789, longitude: -109.8234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", pointSubtype: "Resort Corridor", notes: "Transpeninsular highway access linking SJD to corridor resort inventory." }),
  ti({ name: "Puerto Los Cabos Marina Access", pointType: "Port / Maritime", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0634, longitude: -109.6845, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", pointSubtype: "Marina", notes: "Marina village access north of San José historic core." }),
  ti({ name: "East Cape Coastal Highway Access", pointType: "Highway Access", city: "Los Barriles", submarket: "East Cape", latitude: 23.6789, longitude: -109.7012, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", pointSubtype: "Scenic Corridor", notes: "East Cape road access for sport-fishing and eco-resort secondary demand." }),
  ti({ name: "San José del Cabo Historic District Access", pointType: "Highway Access", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0594, longitude: -109.6972, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", pointSubtype: "Urban Core", notes: "Downtown San José access for art district and boutique hotel demand." }),
  ti({ name: "Todos Santos Pacific Coast Highway Access", pointType: "Highway Access", city: "Todos Santos", submarket: "Other", latitude: 23.4467, longitude: -110.2234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/todos-santos", pointSubtype: "Tourism Corridor", notes: "Pacific coast day-trip access from Los Cabos resort base." }),
];

export function buildLosCabosTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, LOS_CABOS_TI_DELTA_RECORDS);
}
