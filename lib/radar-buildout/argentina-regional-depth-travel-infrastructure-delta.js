/**
 * Argentina Regional Depth mature-pass Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Argentina";
const MARKET = "Argentina Regional Depth";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const ARGENTINA_REGIONAL_DEPTH_TI_DELTA_RECORDS = [
  ti({
    name: "Mendoza Governor Gabrielli Airport Cargo Highway",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Mendoza",
    submarket: "Mendoza",
    latitude: -32.8317,
    longitude: -68.7928,
    sourceReference: "https://www.aeropuertomendoza.gob.ar/",
    notes: "Mendoza airport highway connector for wine-country and convention hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Llao Llao Peninsula Resort Highway Spur",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "San Carlos de Bariloche",
    submarket: "Bariloche",
    latitude: -41.1389,
    longitude: -71.3923,
    sourceReference: "https://www.vialidad.gob.ar/",
    notes: "Patagonia lakes resort corridor highway linking Bariloche city to Llao Llao and Circuito Chico.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
  ti({
    name: "RN-12 Km 5 Iguazú Park Approach",
    pointType: "Highway Access",
    pointSubtype: "Scenic Corridor",
    city: "Puerto Iguazú",
    submarket: "Puerto Iguazú",
    latitude: -25.7012,
    longitude: -54.4689,
    sourceReference: "https://www.iguazuargentina.com/",
    notes: "Primary falls tourism highway from Puerto Iguazú to Iguazú National Park visitor zone.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Nature / Eco-Tourism"],
  }),
];

export function buildArgentinaRegionalDepthTiDeltaFixture() {
  const fixture = buildIslandTiDeltaFixture(COUNTRY, MARKET, ARGENTINA_REGIONAL_DEPTH_TI_DELTA_RECORDS);
  for (const p of fixture.points) {
    p.scopeLevel = "Market";
    p.projectRelevanceLogic = `Argentina Regional Depth mature pass — ${p.name}.`;
  }
  return fixture;
}
