/**
 * Mexico Secondary Markets demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyMexicoSecondaryMarketsGovernanceDefaults,
  MEXICO_SECONDARY_MARKETS_SUBMARKETS,
} from "./mexico-secondary-markets-demand-anchor-governance.js";
import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyMexicoSecondaryMarketsGovernanceDefaults);

export const MEXICO_SECONDARY_MARKETS_CANDIDATES = [
  // Oaxaca (6)
  pt({
    name: "Centro de Convenciones Oaxaca",
    pointType: "Convention Center",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 17.0612,
    longitude: -96.7212,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    governance: {
      projectRelevanceLogic:
        "Oaxaca Secondary Markets build — convention center anchor for group and MICE hotel demand.",
    },
  }),
  pt({
    name: "Historic Centre of Oaxaca UNESCO Zone",
    pointType: "Tourist Attraction",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 17.0657,
    longitude: -96.7256,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    governance: {
      projectRelevanceLogic:
        "Oaxaca Secondary Markets build — UNESCO heritage core anchor for cultural tourism hotel demand.",
    },
  }),
  pt({
    name: "Monte Albán Archaeological Zone",
    pointType: "Tourist Attraction",
    city: "Santa Cruz Xoxocotlán",
    submarket: "Oaxaca",
    latitude: 17.0433,
    longitude: -96.7672,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    governance: {
      projectRelevanceLogic:
        "Oaxaca Secondary Markets build — flagship pre-Hispanic attraction driving day-trip and extended-stay lodging.",
    },
  }),
  pt({
    name: "Reforma Business and Corporate Corridor",
    pointType: "Business District",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 17.0689,
    longitude: -96.7189,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    governance: {
      projectRelevanceLogic:
        "Oaxaca Secondary Markets build — corporate corridor anchor for weekday business hotel demand.",
    },
  }),
  pt({
    name: "Santo Domingo Cultural Complex",
    pointType: "Tourist Attraction",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 17.0712,
    longitude: -96.7267,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    governance: {
      projectRelevanceLogic:
        "Oaxaca Secondary Markets build — museum and church complex anchor for heritage tourism stays.",
    },
  }),
  pt({
    name: "Hierve el Agua Mineral Springs",
    pointType: "Tourist Attraction",
    city: "San Lorenzo Albarradas",
    submarket: "Oaxaca",
    latitude: 16.8667,
    longitude: -96.2833,
    sourceReference: "https://www.oaxaca.travel/en/",
    governance: {
      projectRelevanceLogic:
        "Oaxaca Secondary Markets build — iconic natural attraction supporting valley eco-lodge and tour-base demand.",
      useCaseTags: ["Nature / Eco-Tourism", "Resort / Leisure"],
    },
  }),

  // Querétaro (6)
  pt({
    name: "Querétaro Centro de Congresos",
    pointType: "Convention Center",
    city: "Santiago de Querétaro",
    submarket: "Querétaro",
    latitude: 20.5889,
    longitude: -100.3912,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    governance: {
      projectRelevanceLogic:
        "Querétaro Secondary Markets build — convention center anchor for Bajío MICE and corporate event demand.",
    },
  }),
  pt({
    name: "Historic Monuments Zone of Querétaro",
    pointType: "Tourist Attraction",
    city: "Santiago de Querétaro",
    submarket: "Querétaro",
    latitude: 20.5933,
    longitude: -100.3917,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    governance: {
      projectRelevanceLogic:
        "Querétaro Secondary Markets build — UNESCO historic center anchor for heritage tourism hotel demand.",
    },
  }),
  pt({
    name: "Zona Norte Corporate Corridor",
    pointType: "Business District",
    city: "Santiago de Querétaro",
    submarket: "Querétaro",
    latitude: 20.6212,
    longitude: -100.4512,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    governance: {
      projectRelevanceLogic:
        "Querétaro Secondary Markets build — north-side corporate and industrial corridor for weekday lodging.",
    },
  }),
  pt({
    name: "Aqueduct of Querétaro Landmark",
    pointType: "Tourist Attraction",
    city: "Santiago de Querétaro",
    submarket: "Querétaro",
    latitude: 20.6012,
    longitude: -100.3789,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    governance: {
      projectRelevanceLogic:
        "Querétaro Secondary Markets build — signature aqueduct landmark supporting centro hotel walkability.",
    },
  }),
  pt({
    name: "Peña de Bernal Monolith",
    pointType: "Tourist Attraction",
    city: "Bernal",
    submarket: "Querétaro",
    latitude: 20.5012,
    longitude: -99.8189,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro/bernal",
    governance: {
      projectRelevanceLogic:
        "Querétaro Secondary Markets build — Pueblo Mágico monolith day-trip anchor for regional leisure stays.",
      useCaseTags: ["Nature / Eco-Tourism", "Resort / Leisure"],
    },
  }),
  pt({
    name: "Teatro de la República Historic Venue",
    pointType: "Government / Civic Center",
    city: "Santiago de Querétaro",
    submarket: "Querétaro",
    latitude: 20.5945,
    longitude: -100.3889,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    governance: {
      projectRelevanceLogic:
        "Querétaro Secondary Markets build — civic heritage venue anchor for cultural event and tour-group demand.",
    },
  }),

  // Guanajuato (6)
  pt({
    name: "Forum Guanajuato Convention Center",
    pointType: "Convention Center",
    city: "León",
    submarket: "Guanajuato",
    latitude: 21.1212,
    longitude: -101.6512,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    governance: {
      projectRelevanceLogic:
        "Guanajuato Secondary Markets build — Bajío convention anchor for group and trade-show hotel compression.",
    },
  }),
  pt({
    name: "Historic Town of Guanajuato UNESCO Core",
    pointType: "Tourist Attraction",
    city: "Guanajuato",
    submarket: "Guanajuato",
    latitude: 21.0178,
    longitude: -101.2567,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    governance: {
      projectRelevanceLogic:
        "Guanajuato Secondary Markets build — UNESCO colonial core anchor for heritage tourism hotel demand.",
    },
  }),
  pt({
    name: "Silao-León Industrial Business Corridor",
    pointType: "Business District",
    city: "Silao",
    submarket: "Guanajuato",
    latitude: 20.9512,
    longitude: -101.4812,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    governance: {
      projectRelevanceLogic:
        "Guanajuato Secondary Markets build — industrial and logistics corridor anchor for corporate weekday lodging.",
    },
  }),
  pt({
    name: "Museo de las Momias de Guanajuato",
    pointType: "Tourist Attraction",
    city: "Guanajuato",
    submarket: "Guanajuato",
    latitude: 21.0189,
    longitude: -101.2612,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    governance: {
      projectRelevanceLogic:
        "Guanajuato Secondary Markets build — signature attraction driving centro walkable hotel demand.",
    },
  }),
  pt({
    name: "Monumento al Pípila Scenic Overlook",
    pointType: "Tourist Attraction",
    city: "Guanajuato",
    submarket: "Guanajuato",
    latitude: 21.0167,
    longitude: -101.2534,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    governance: {
      projectRelevanceLogic:
        "Guanajuato Secondary Markets build — hillside landmark anchor for panoramic tourism and event stays.",
    },
  }),
  pt({
    name: "Valenciana Mine and Templo San Cayetano",
    pointType: "Tourist Attraction",
    city: "Guanajuato",
    submarket: "Guanajuato",
    latitude: 21.0312,
    longitude: -101.2712,
    sourceReference: "https://www.guanajuato.travel/",
    governance: {
      projectRelevanceLogic:
        "Guanajuato Secondary Markets build — silver-mining heritage site supporting cultural tourism lodging.",
    },
  }),

  // Mazatlán (6)
  pt({
    name: "Mazatlán International Convention Center",
    pointType: "Convention Center",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.2212,
    longitude: -106.4212,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    governance: {
      projectRelevanceLogic:
        "Mazatlán Secondary Markets build — convention center anchor for Pacific coast MICE hotel demand.",
    },
  }),
  pt({
    name: "Historic Centro and Plazuela Machado",
    pointType: "Tourist Attraction",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.1989,
    longitude: -106.4212,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    governance: {
      projectRelevanceLogic:
        "Mazatlán Secondary Markets build — restored historic core anchor for cultural tourism and dining demand.",
    },
  }),
  pt({
    name: "Zona Dorada Beach Resort Strip",
    pointType: "Beach / Waterfront",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.2312,
    longitude: -106.4512,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    governance: {
      projectRelevanceLogic:
        "Mazatlán Secondary Markets build — primary resort beachfront anchor for leisure hotel demand.",
    },
  }),
  pt({
    name: "Mazatlán Malecón Waterfront Promenade",
    pointType: "Entertainment District",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.2189,
    longitude: -106.4312,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    governance: {
      projectRelevanceLogic:
        "Mazatlán Secondary Markets build — oceanfront promenade anchor for entertainment and event lodging.",
    },
  }),
  pt({
    name: "Port and Industrial Logistics Corridor",
    pointType: "Industrial / Logistics Zone",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.2012,
    longitude: -106.4189,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    governance: {
      projectRelevanceLogic:
        "Mazatlán Secondary Markets build — port-adjacent logistics corridor for maritime and corporate transit demand.",
      useCaseTags: ["Industrial / Logistics", "Cruise / Port"],
    },
  }),
  pt({
    name: "El Faro Lighthouse and Cerro Crestón",
    pointType: "Tourist Attraction",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.1789,
    longitude: -106.4112,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    governance: {
      projectRelevanceLogic:
        "Mazatlán Secondary Markets build — iconic lighthouse attraction supporting south-end resort and tour demand.",
    },
  }),
];

export function getMexicoSecondaryMarketsCandidates() {
  return MEXICO_SECONDARY_MARKETS_CANDIDATES;
}

export { MEXICO_SECONDARY_MARKETS_SUBMARKETS };
