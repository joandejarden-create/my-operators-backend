/**
 * Argentina Regional Depth mature-pass demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyArgentinaRegionalDepthGovernanceDefaults,
  ARGENTINA_REGIONAL_DEPTH_SUBMARKETS,
} from "./argentina-regional-depth-demand-anchor-governance.js";

const COUNTRY = "Argentina";
const REGION = "South America";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyArgentinaRegionalDepthGovernanceDefaults);

export const ARGENTINA_REGIONAL_DEPTH_CANDIDATES = [
  // Mendoza (+4)
  pt({
    name: "Uco Valley Wine Route",
    pointType: "Tourist Attraction",
    city: "Tunuyán",
    submarket: "Mendoza",
    latitude: -33.5833,
    longitude: -69.0167,
    sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza/uco-valley",
  }),
  pt({
    name: "Potrerillos Dam Recreation Area",
    pointType: "Tourist Attraction",
    city: "Potrerillos",
    submarket: "Mendoza",
    latitude: -32.9583,
    longitude: -68.5833,
    sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza",
  }),
  pt({
    name: "Mendoza Convention and Expo Center",
    pointType: "Convention Center",
    city: "Mendoza",
    submarket: "Mendoza",
    latitude: -32.9012,
    longitude: -68.7812,
    sourceReference: "https://www.mendoza.gov.ar/",
  }),
  pt({
    name: "Luján de Cuyo Wine Corridor",
    pointType: "Entertainment District",
    city: "Luján de Cuyo",
    submarket: "Mendoza",
    latitude: -33.0333,
    longitude: -68.8833,
    sourceReference: "https://www.argentina.travel/en/destinations/cuyo/mendoza/lujan-de-cuyo",
  }),

  // Bariloche (+4)
  pt({
    name: "Cerro Catedral Ski Resort",
    pointType: "Sports Venue",
    city: "San Carlos de Bariloche",
    submarket: "Bariloche",
    latitude: -41.1667,
    longitude: -71.45,
    sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche/cerro-catedral",
  }),
  pt({
    name: "Llao Llao Resort District",
    pointType: "Mixed-Use Development",
    city: "San Carlos de Bariloche",
    submarket: "Bariloche",
    latitude: -41.1412,
    longitude: -71.4012,
    sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche",
  }),
  pt({
    name: "Circuito Chico Scenic Route",
    pointType: "Tourist Attraction",
    city: "San Carlos de Bariloche",
    submarket: "Bariloche",
    latitude: -41.12,
    longitude: -71.35,
    sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche/circuito-chico",
  }),
  pt({
    name: "Villa La Angostura Gateway",
    pointType: "Future Growth Node",
    city: "Villa La Angostura",
    submarket: "Bariloche",
    latitude: -40.7612,
    longitude: -71.6312,
    sourceReference: "https://www.argentina.travel/en/destinations/patagonia/bariloche",
  }),

  // Puerto Iguazú (+3)
  pt({
    name: "Hito Tres Fronteras",
    pointType: "Tourist Attraction",
    city: "Puerto Iguazú",
    submarket: "Puerto Iguazú",
    latitude: -25.5978,
    longitude: -54.5906,
    sourceReference: "https://www.argentina.travel/en/destinations/litoral/iguazu-falls",
  }),
  pt({
    name: "Yriapú Nature Reserve",
    pointType: "Tourist Attraction",
    city: "Puerto Iguazú",
    submarket: "Puerto Iguazú",
    latitude: -25.6212,
    longitude: -54.5712,
    sourceReference: "https://www.iguazuargentina.com/",
  }),
  pt({
    name: "Puerto Iguazú Duty-Free Corridor",
    pointType: "Entertainment District",
    city: "Puerto Iguazú",
    submarket: "Puerto Iguazú",
    latitude: -25.6012,
    longitude: -54.5712,
    sourceReference: "https://www.argentina.travel/en/destinations/litoral/iguazu-falls",
  }),
];

export function getArgentinaRegionalDepthCandidates() {
  return ARGENTINA_REGIONAL_DEPTH_CANDIDATES;
}

export { ARGENTINA_REGIONAL_DEPTH_SUBMARKETS };
