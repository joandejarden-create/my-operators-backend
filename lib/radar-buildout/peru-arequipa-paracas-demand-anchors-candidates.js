/**
 * Peru — Arequipa / Paracas mature-pass demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyPeruArequipaParacasGovernanceDefaults,
  PERU_AREQUIPA_PARACAS_SUBMARKETS,
} from "./peru-arequipa-paracas-demand-anchor-governance.js";

const COUNTRY = "Peru";
const REGION = "South America";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyPeruArequipaParacasGovernanceDefaults);

export const PERU_AREQUIPA_PARACAS_CANDIDATES = [
  // Arequipa (6)
  pt({
    name: "Plaza de Armas UNESCO World Heritage Site",
    pointType: "Tourist Attraction",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.3989,
    longitude: -71.5369,
    sourceReference: "https://www.peru.travel/en/destinations/arequipa/plaza-de-armas",
    manuallyVerified: true,
  }),
  pt({
    name: "Yanahuara Viewpoint",
    pointType: "Tourist Attraction",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.397,
    longitude: -71.5455,
    sourceReference: "https://www.peru.travel/en/destinations/arequipa/yanahuara",
  }),
  pt({
    name: "Colca Canyon Gateway",
    pointType: "Tourist Attraction",
    city: "Chivay",
    submarket: "Arequipa",
    latitude: -15.6389,
    longitude: -71.6039,
    sourceReference: "https://www.peru.travel/en/destinations/arequipa/colca-canyon",
  }),
  pt({
    name: "Arequipa Convention Center",
    pointType: "Convention Center",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.4156,
    longitude: -71.5389,
    sourceReference: "https://www.peru.travel/en/destinations/arequipa",
  }),
  pt({
    name: "Mercado San Camilo",
    pointType: "Entertainment District",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.3934,
    longitude: -71.5345,
    sourceReference: "https://www.peru.travel/en/destinations/arequipa",
  }),
  pt({
    name: "Rodriguez Ballon Airport Corridor",
    pointType: "Future Growth Node",
    city: "Arequipa",
    submarket: "Arequipa",
    latitude: -16.3411,
    longitude: -71.5831,
    sourceReference: "https://www.gob.pe/aaqq",
    manuallyVerified: true,
  }),

  // Paracas (6)
  pt({
    name: "Paracas National Reserve",
    pointType: "Tourist Attraction",
    city: "Paracas",
    submarket: "Paracas",
    latitude: -14.3389,
    longitude: -76.2139,
    sourceReference: "https://www.peru.travel/en/destinations/ica/paracas",
    manuallyVerified: true,
  }),
  pt({
    name: "Ballestas Islands Marina",
    pointType: "Beach / Waterfront",
    city: "Paracas",
    submarket: "Paracas",
    latitude: -13.8345,
    longitude: -76.2545,
    sourceReference: "https://www.peru.travel/en/destinations/ica/paracas/ballestas-islands",
  }),
  pt({
    name: "Paracas Luxury Resort Corridor",
    pointType: "Mixed-Use Development",
    city: "Paracas",
    submarket: "Paracas",
    latitude: -13.8512,
    longitude: -76.2712,
    sourceReference: "https://www.peru.travel/en/destinations/ica/paracas",
  }),
  pt({
    name: "Pisco Airport Access Node",
    pointType: "Future Growth Node",
    city: "Pisco",
    submarket: "Paracas",
    latitude: -13.7448,
    longitude: -76.2203,
    sourceReference: "https://www.gob.pe/mtc",
  }),
  pt({
    name: "Tambo Colorado Ruins",
    pointType: "Tourist Attraction",
    city: "Pisco",
    submarket: "Paracas",
    latitude: -13.6656,
    longitude: -76.2323,
    sourceReference: "https://www.peru.travel/en/destinations/ica/pisco",
  }),
  pt({
    name: "Paracas Future Growth Waterfront",
    pointType: "Future Growth Node",
    city: "Paracas",
    submarket: "Paracas",
    latitude: -13.8423,
    longitude: -76.2489,
    sourceReference: "https://www.peru.travel/en/destinations/ica/paracas",
  }),
];

export function getPeruArequipaParacasCandidates() {
  return PERU_AREQUIPA_PARACAS_CANDIDATES;
}

export { PERU_AREQUIPA_PARACAS_SUBMARKETS };
