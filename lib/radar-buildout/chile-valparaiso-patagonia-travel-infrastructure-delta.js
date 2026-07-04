/**
 * Chile — Valparaíso / Patagonia mature-pass Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Chile";
const MARKET = "Valparaíso / Patagonia";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const CHILE_VALPARAISO_PATAGONIA_TI_DELTA_RECORDS = [
  ti({
    name: "Valparaíso Ruta 68 Coastal Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Valparaíso",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -33.0472,
    longitude: -71.6127,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/valparaiso/",
    notes: "Ruta 68 and coastal connector linking Valparaíso port heritage and Viña del Mar resort demand.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
  ti({
    name: "Puerto Montt El Tepual Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Puerto Montt",
    submarket: "Patagonia Lakes",
    latitude: -41.4389,
    longitude: -73.0939,
    sourceReference: "https://www.aeropuertoeltepual.cl/",
    notes: "Southern lakes district air gateway for Puerto Varas, Frutillar, and cruise embarkation demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Punta Arenas Carlos Ibáñez Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Punta Arenas",
    submarket: "Puerto Natales",
    latitude: -53.0026,
    longitude: -70.8544,
    sourceReference: "https://www.aeropuertopuntaarenas.cl/",
    notes: "Southern Patagonia air access for Strait of Magellan and Torres del Paine extension stays.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Nature / Eco-Tourism"],
  }),
  ti({
    name: "Puerto Natales Torres del Paine Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Scenic Corridor",
    city: "Puerto Natales",
    submarket: "Puerto Natales",
    latitude: -51.7236,
    longitude: -72.4875,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/torres-del-paine/",
    notes: "Primary highway corridor from Puerto Natales to Torres del Paine national park visitation.",
    scopeLevel: "Market",
    useCaseTags: ["Nature / Eco-Tourism", "Resort / Leisure"],
  }),
];

export function buildChileValparaisoPatagoniaTiDeltaFixture() {
  const fixture = buildIslandTiDeltaFixture(COUNTRY, MARKET, CHILE_VALPARAISO_PATAGONIA_TI_DELTA_RECORDS);
  for (const p of fixture.points) {
    p.scopeLevel = "Market";
    p.projectRelevanceLogic = `Chile Valparaíso / Patagonia mature pass — ${p.name}.`;
  }
  return fixture;
}
