/**
 * DR Secondary Coasts Mature Pass demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyDominicanRepublicSecondaryCoastsGovernanceDefaults,
  DOMINICAN_REPUBLIC_SECONDARY_COASTS_SUBMARKETS,
} from "./dominican-republic-secondary-coasts-demand-anchor-governance.js";

const COUNTRY = "Dominican Republic";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(
  COUNTRY,
  REGION,
  applyDominicanRepublicSecondaryCoastsGovernanceDefaults
);

export const DOMINICAN_REPUBLIC_SECONDARY_COASTS_CANDIDATES = [
  // Miches / Costa Esmeralda (4)
  pt({
    name: "Zemi Miches All Inclusive Resort Corridor",
    pointType: "Mixed-Use Development",
    city: "Miches",
    submarket: "Miches / Costa Esmeralda",
    latitude: 18.9834,
    longitude: -69.0389,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Flagship east-coast resort corridor anchoring emerging Costa Esmeralda lodging demand.",
  }),
  pt({
    name: "Playa Esmeralda Beach Coast",
    pointType: "Beach / Waterfront",
    city: "Miches",
    submarket: "Miches / Costa Esmeralda",
    latitude: 18.9712,
    longitude: -69.0212,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Primary beachfront leisure strip for new Miches resort and day-trip lodging.",
  }),
  pt({
    name: "Costa Esmeralda Highway Tourism Node",
    pointType: "Future Growth Node",
    city: "Miches",
    submarket: "Miches / Costa Esmeralda",
    latitude: 18.9789,
    longitude: -69.0489,
    sourceReference: "https://www.mop.gob.do/",
    hotelDemandNote: "Highway connector node linking Higüey to the Costa Esmeralda resort growth zone.",
    manuallyVerified: true,
  }),
  pt({
    name: "Miches Fishing Village Tourism",
    pointType: "Entertainment District",
    city: "Miches",
    submarket: "Miches / Costa Esmeralda",
    latitude: 18.9833,
    longitude: -69.05,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Authentic fishing-village dining and excursion base for eco-resort guests.",
  }),

  // Barahona / Pedernales (4)
  pt({
    name: "Bahía de las Águilas Beach",
    pointType: "Beach / Waterfront",
    city: "Pedernales",
    submarket: "Barahona / Pedernales",
    latitude: 17.8606,
    longitude: -71.6447,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Remote southwest flagship beach driving eco-lodge and adventure lodging demand.",
  }),
  pt({
    name: "Larimar Mines Tourism Attraction",
    pointType: "Tourist Attraction",
    city: "Barahona",
    submarket: "Barahona / Pedernales",
    latitude: 18.2512,
    longitude: -71.1212,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Unique blue-stone mining tours supporting southwest heritage tourism stays.",
  }),
  pt({
    name: "Lago Enriquillo Eco-Tourism Zone",
    pointType: "Tourist Attraction",
    city: "La Descubierta",
    submarket: "Barahona / Pedernales",
    latitude: 18.5012,
    longitude: -71.7512,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Hypersaline lake and wildlife eco-tourism anchor for southwest hotel base nights.",
    governance: { useCaseTags: ["Nature / Eco-Tourism", "Resort / Leisure"] },
  }),
  pt({
    name: "María Montez International Airport Corridor",
    pointType: "Future Growth Node",
    city: "Barahona",
    submarket: "Barahona / Pedernales",
    latitude: 18.2515,
    longitude: -71.1203,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Regional air gateway corridor for southwest eco-tourism and Pedernales beach demand.",
    manuallyVerified: true,
  }),

  // Jarabacoa / Constanza (4)
  pt({
    name: "Reserva Científica Ebano Verde",
    pointType: "Tourist Attraction",
    city: "Jarabacoa",
    submarket: "Jarabacoa / Constanza",
    latitude: 19.0834,
    longitude: -70.6178,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Cloud-forest scientific reserve driving highland eco-lodge and adventure stays.",
    governance: { useCaseTags: ["Nature / Eco-Tourism", "Resort / Leisure"] },
  }),
  pt({
    name: "Baiguate Adventure Park",
    pointType: "Tourist Attraction",
    city: "Jarabacoa",
    submarket: "Jarabacoa / Constanza",
    latitude: 19.1178,
    longitude: -70.6378,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Adventure park and canopy tourism hub for Jarabacoa weekend hotel compression.",
  }),
  pt({
    name: "Constanza Cold-Climate Tourism Valley",
    pointType: "Tourist Attraction",
    city: "Constanza",
    submarket: "Jarabacoa / Constanza",
    latitude: 18.9089,
    longitude: -70.7412,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "Highland valley agritourism and cold-climate leisure stays distinct from coastal resorts.",
    governance: { useCaseTags: ["Nature / Eco-Tourism", "Resort / Leisure"] },
  }),
  pt({
    name: "Jarabacoa Eco-Lodge Adventure Corridor",
    pointType: "Mixed-Use Development",
    city: "Jarabacoa",
    submarket: "Jarabacoa / Constanza",
    latitude: 19.1212,
    longitude: -70.6412,
    sourceReference: "https://www.godominicanrepublic.com/",
    hotelDemandNote: "River-valley eco-lodge and adventure lodge cluster for highland hotel demand.",
  }),
];

export function getDominicanRepublicSecondaryCoastsCandidates() {
  return DOMINICAN_REPUBLIC_SECONDARY_COASTS_CANDIDATES;
}

export { DOMINICAN_REPUBLIC_SECONDARY_COASTS_SUBMARKETS };
