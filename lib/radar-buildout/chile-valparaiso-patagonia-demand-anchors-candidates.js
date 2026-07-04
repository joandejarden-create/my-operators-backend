/**
 * Chile — Valparaíso / Patagonia mature-pass demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyChileValparaisoPatagoniaGovernanceDefaults,
  CHILE_VALPARAISO_PATAGONIA_SUBMARKETS,
} from "./chile-valparaiso-patagonia-demand-anchor-governance.js";

const COUNTRY = "Chile";
const REGION = "South America";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyChileValparaisoPatagoniaGovernanceDefaults);

export const CHILE_VALPARAISO_PATAGONIA_CANDIDATES = [
  // Valparaíso / Viña del Mar (4)
  pt({
    name: "Valparaíso Port UNESCO Historic Quarter",
    pointType: "Tourist Attraction",
    city: "Valparaíso",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -33.0472,
    longitude: -71.6127,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/valparaiso/",
    manuallyVerified: true,
  }),
  pt({
    name: "Viña del Mar Municipal Casino",
    pointType: "Entertainment District",
    city: "Viña del Mar",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -33.0256,
    longitude: -71.5517,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/vina-del-mar/",
  }),
  pt({
    name: "Reñaca Beach",
    pointType: "Beach / Waterfront",
    city: "Viña del Mar",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -32.9547,
    longitude: -71.5123,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/vina-del-mar/renaca-beach",
  }),
  pt({
    name: "Concón Coast",
    pointType: "Beach / Waterfront",
    city: "Concón",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -32.9234,
    longitude: -71.5234,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/vina-del-mar/",
  }),

  // Patagonia (4)
  pt({
    name: "Puerto Natales Torres del Paine Gateway",
    pointType: "Future Growth Node",
    city: "Puerto Natales",
    submarket: "Puerto Natales",
    latitude: -51.7236,
    longitude: -72.4875,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/torres-del-paine/",
    manuallyVerified: true,
  }),
  pt({
    name: "Punta Arenas Strait of Magellan",
    pointType: "Tourist Attraction",
    city: "Punta Arenas",
    submarket: "Puerto Natales",
    latitude: -53.1638,
    longitude: -70.9171,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/punta-arenas/",
  }),
  pt({
    name: "Puerto Varas Lakes District",
    pointType: "Tourist Attraction",
    city: "Puerto Varas",
    submarket: "Patagonia Lakes",
    latitude: -41.3195,
    longitude: -72.9856,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/puerto-varas/",
  }),
  pt({
    name: "Frutillar German Heritage Town",
    pointType: "Tourist Attraction",
    city: "Frutillar",
    submarket: "Patagonia Lakes",
    latitude: -41.1167,
    longitude: -73.0167,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/frutillar/",
  }),

  // Additional split (2 Valparaíso / Viña, 2 Patagonia)
  pt({
    name: "Cerro Alegre Funicular Historic District",
    pointType: "Tourist Attraction",
    city: "Valparaíso",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -33.0412,
    longitude: -71.6289,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/valparaiso/",
  }),
  pt({
    name: "Quinta Vergara Viña del Mar",
    pointType: "Entertainment District",
    city: "Viña del Mar",
    submarket: "Valparaíso / Viña del Mar",
    latitude: -33.0312,
    longitude: -71.5412,
    sourceReference: "https://www.chile.travel/en/where-to-go/valleys-and-coasts/vina-del-mar/",
  }),
  pt({
    name: "Puerto Montt Cruise Gateway",
    pointType: "Mixed-Use Development",
    city: "Puerto Montt",
    submarket: "Patagonia Lakes",
    latitude: -41.4712,
    longitude: -72.9367,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/puerto-montt/",
  }),
  pt({
    name: "Coyhaique Aysén Patagonia Hub",
    pointType: "Government / Civic Center",
    city: "Coyhaique",
    submarket: "Puerto Natales",
    latitude: -45.5752,
    longitude: -72.0662,
    sourceReference: "https://www.chile.travel/en/where-to-go/the-south-and-antarctica/patagonia/coyhaique/",
  }),
];

export function getChileValparaisoPatagoniaCandidates() {
  return CHILE_VALPARAISO_PATAGONIA_CANDIDATES;
}

export { CHILE_VALPARAISO_PATAGONIA_SUBMARKETS };
