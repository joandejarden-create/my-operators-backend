/**
 * Mérida / Yucatán Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Mérida / Yucatán";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const MERIDA_YUCATAN_TI_DELTA_RECORDS = [
  ti({ name: "Mérida International Airport Access", pointType: "Highway Access", city: "Mérida", submarket: "Airport Corridor", latitude: 20.937, longitude: -89.6577, sourceReference: "https://www.aeropuertosasa.mx/", pointSubtype: "Airport Access", notes: "Primary international gateway for Mérida urban and Yucatán tourism demand." }),
  ti({ name: "Paseo de Montejo Boulevard Access", pointType: "Highway Access", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9756, longitude: -89.6178, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida", pointSubtype: "Urban Artery", notes: "Main historic boulevard access for centro and boutique hotel demand." }),
  ti({ name: "Centro de Convenciones Siglo XXI Access", pointType: "Highway Access", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9939, longitude: -89.6142, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida", pointSubtype: "Convention Access", notes: "Convention center highway access for MICE and group compression." }),
  ti({ name: "Progreso Cruise Terminal Access", pointType: "Port / Maritime", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2812, longitude: -89.6689, sourceReference: "https://www.yucatan.travel/", pointSubtype: "Cruise Terminal", notes: "Gulf cruise terminal supporting day-call and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Progreso Malecón Beach Access", pointType: "Highway Access", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2853, longitude: -89.6644, sourceReference: "https://www.yucatan.travel/", pointSubtype: "Resort Corridor", notes: "Coastal highway access for Progreso weekend beach and cruise demand." }),
  ti({ name: "Chichén Itzá Tourism Highway Access", pointType: "Highway Access", city: "Tinúm", submarket: "Other", latitude: 20.6843, longitude: -88.5678, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/chichen-itza", pointSubtype: "Tourism Corridor", notes: "Primary heritage day-trip corridor from Mérida hotel base." }),
  ti({ name: "Uxmal Ruta Puuc Highway Access", pointType: "Highway Access", city: "Santa Elena", submarket: "Other", latitude: 20.3594, longitude: -89.7715, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/uxmal", pointSubtype: "Tourism Corridor", notes: "Southwest heritage corridor for Uxmal and Ruta Puuc visitation." }),
  ti({ name: "Periférico Mérida Ring Road Access", pointType: "Highway Access", city: "Mérida", submarket: "Industrial / Periférico", latitude: 20.9512, longitude: -89.6789, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida", pointSubtype: "Urban Artery", notes: "Periférico ring road linking airport, industrial parks, and north Mérida growth." }),
];

export function buildMeridaYucatanTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, MERIDA_YUCATAN_TI_DELTA_RECORDS);
}
