/**
 * Peru — Arequipa / Paracas mature-pass Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Peru";
const MARKET = "Arequipa / Paracas";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const PERU_AREQUIPA_PARACAS_TI_DELTA_RECORDS = [
  ti({
    name: "Rodriguez Ballon Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.3411,
    longitude: -71.5831,
    sourceReference: "https://www.gob.pe/aaqq",
    notes: "Primary air gateway for Arequipa heritage, Colca Canyon, and southern Peru hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Pan-American Arequipa Highway",
    pointType: "Highway Access",
    pointSubtype: "Urban Artery",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.409,
    longitude: -71.535,
    sourceReference: "https://www.peru.travel/en/destinations/arequipa",
    notes: "Panamericana Sur corridor linking Arequipa centro, convention, and Colca gateway demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Paracas Coastal Highway",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Paracas",
    submarket: "Paracas",
    latitude: -13.8512,
    longitude: -76.2712,
    sourceReference: "https://www.peru.travel/en/destinations/ica/paracas",
    notes: "Coastal Panamericana Sur access for Paracas reserve, marina, and resort lodging compression.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
  ti({
    name: "Pisco Port",
    pointType: "Port / Maritime",
    pointSubtype: "Commercial Port",
    city: "Pisco",
    submarket: "Paracas",
    latitude: -13.7098,
    longitude: -76.2034,
    sourceReference: "https://www.peru.travel/en/destinations/ica/pisco",
    notes: "Commercial port and logistics node supporting Paracas coastal and industrial hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Industrial / Logistics", "Cruise / Port"],
  }),
];

export function buildPeruArequipaParacasTiDeltaFixture() {
  const fixture = buildIslandTiDeltaFixture(COUNTRY, MARKET, PERU_AREQUIPA_PARACAS_TI_DELTA_RECORDS);
  for (const p of fixture.points) {
    p.scopeLevel = "Market";
    p.projectRelevanceLogic = `Peru Arequipa / Paracas mature pass — ${p.name}.`;
  }
  return fixture;
}
