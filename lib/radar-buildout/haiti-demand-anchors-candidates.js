/**
 * Haiti Countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyHaitiGovernanceDefaults,
  HAITI_SUBMARKETS,
} from "./haiti-demand-anchor-governance.js";

const COUNTRY = "Haiti";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyHaitiGovernanceDefaults);

export const HAITI_CANDIDATES = [
  pt({ name: "Toussaint Louverture International Airport Corridor", pointType: "Future Growth Node", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5801, longitude: -72.2925, sourceReference: "https://www.ofnac.gouv.ht/", manuallyVerified: true }),
  pt({ name: "Port of Port-au-Prince", pointType: "Mixed-Use Development", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5412, longitude: -72.3389, sourceReference: "https://www.haiti.org/", manuallyVerified: true }),
  pt({ name: "Champ de Mars Civic Plaza", pointType: "Government / Civic Center", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5434, longitude: -72.3378, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Iron Market Marché en Fer", pointType: "Entertainment District", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5445, longitude: -72.3356, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Pétion-Ville Business and Dining Corridor", pointType: "Business District", city: "Pétion-Ville", submarket: "Port-au-Prince", latitude: 18.5123, longitude: -72.2845, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Karibe Convention Center", pointType: "Convention Center", city: "Pétion-Ville", submarket: "Port-au-Prince", latitude: 18.5089, longitude: -72.2812, sourceReference: "https://www.karibehotel.com/" }),
  pt({ name: "Université Quisqueya Campus", pointType: "University / College", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5234, longitude: -72.2912, sourceReference: "https://www.uniq.edu.ht/" }),
  pt({ name: "Université d'État d'Haïti", pointType: "University / College", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5389, longitude: -72.3345, sourceReference: "https://www.ueh.edu.ht/" }),
  pt({ name: "Hôpital Universitaire de la Paix", pointType: "Medical Campus", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5312, longitude: -72.3012, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Stade Sylvio Cator", pointType: "Sports Venue", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5456, longitude: -72.3289, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Musée du Panthéon National Haïtien", pointType: "Tourist Attraction", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5467, longitude: -72.3367, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Tabarre Industrial and Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Tabarre", submarket: "Port-au-Prince", latitude: 18.5712, longitude: -72.2678, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Kenscoff Mountain Retreat Corridor", pointType: "Tourist Attraction", city: "Kenscoff", submarket: "Port-au-Prince", latitude: 18.4512, longitude: -72.1989, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Cap-Haïtien International Airport Corridor", pointType: "Future Growth Node", city: "Cap-Haïtien", submarket: "Cap-Haïtien", latitude: 19.7581, longitude: -72.1944, sourceReference: "https://www.haiti.org/", manuallyVerified: true }),
  pt({ name: "Citadelle Laferrière UNESCO Site", pointType: "Tourist Attraction", city: "Milot", submarket: "Cap-Haïtien", latitude: 19.5734, longitude: -72.2445, sourceReference: "https://whc.unesco.org/en/list/180", manuallyVerified: true }),
  pt({ name: "Sans-Souci Palace Ruins", pointType: "Tourist Attraction", city: "Milot", submarket: "Cap-Haïtien", latitude: 19.6012, longitude: -72.2189, sourceReference: "https://whc.unesco.org/en/list/180" }),
  pt({ name: "Cap-Haïtien Historic Waterfront", pointType: "Beach / Waterfront", city: "Cap-Haïtien", submarket: "Cap-Haïtien", latitude: 19.7589, longitude: -72.2012, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Labadee Royal Caribbean Destination", pointType: "Beach / Waterfront", city: "Labadee", submarket: "Cap-Haïtien", latitude: 19.7867, longitude: -72.2456, sourceReference: "https://www.royalcaribbean.com/", manuallyVerified: true }),
  pt({ name: "Bassin Bleu Waterfall Attraction", pointType: "Tourist Attraction", city: "Jacmel", submarket: "Cap-Haïtien", latitude: 18.3234, longitude: -72.4123, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Cap-Haïtien Cathedral and Central Square", pointType: "Government / Civic Center", city: "Cap-Haïtien", submarket: "Cap-Haïtien", latitude: 19.7598, longitude: -72.2034, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Fort Picolet Historic Site", pointType: "Tourist Attraction", city: "Cap-Haïtien", submarket: "Cap-Haïtien", latitude: 19.7712, longitude: -72.1912, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Jacmel Historic Colonial District", pointType: "Tourist Attraction", city: "Jacmel", submarket: "Jacmel", latitude: 18.2345, longitude: -72.5356, sourceReference: "https://www.haiti.org/", manuallyVerified: true }),
  pt({ name: "Jacmel Carnival Arts Corridor", pointType: "Entertainment District", city: "Jacmel", submarket: "Jacmel", latitude: 18.2367, longitude: -72.5334, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Jacmel Beach Waterfront", pointType: "Beach / Waterfront", city: "Jacmel", submarket: "Jacmel", latitude: 18.2289, longitude: -72.5412, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Bassin Bleu Jacmel Eco-Attraction", pointType: "Tourist Attraction", city: "Jacmel", submarket: "Jacmel", latitude: 18.2512, longitude: -72.5234, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Cyvadier Beach Resort Zone", pointType: "Beach / Waterfront", city: "Jacmel", submarket: "Jacmel", latitude: 18.2189, longitude: -72.5489, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Port-de-Paix North Coast Gateway", pointType: "Future Growth Node", city: "Port-de-Paix", submarket: "Other", latitude: 19.9389, longitude: -72.8312, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Les Cayes South Coast Hub", pointType: "Business District", city: "Les Cayes", submarket: "Other", latitude: 18.1934, longitude: -73.7456, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Port-Salut Beach Tourism Corridor", pointType: "Beach / Waterfront", city: "Port-Salut", submarket: "Other", latitude: 18.0789, longitude: -73.9234, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Île-à-Vache Island Resort Destination", pointType: "Mixed-Use Development", city: "Île-à-Vache", submarket: "Other", latitude: 18.0712, longitude: -73.6912, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Gonaïves Independence Heritage City", pointType: "Tourist Attraction", city: "Gonaïves", submarket: "Other", latitude: 19.4512, longitude: -72.6889, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Jérémie South Peninsula Gateway", pointType: "Future Growth Node", city: "Jérémie", submarket: "Other", latitude: 18.6512, longitude: -74.1234, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Côte des Arcadins Resort Coast", pointType: "Beach / Waterfront", city: "Montrouis", submarket: "Other", latitude: 19.1012, longitude: -72.7012, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Wahoo Bay Beach Resort Area", pointType: "Beach / Waterfront", city: "Côtes-de-Fer", submarket: "Other", latitude: 18.1912, longitude: -72.5234, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Saut-d'Eau Pilgrimage and Tourism Site", pointType: "Tourist Attraction", city: "Saut-d'Eau", submarket: "Other", latitude: 18.8234, longitude: -72.2189, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Fort Jacques Mountain Heritage Site", pointType: "Tourist Attraction", city: "Kenscoff", submarket: "Other", latitude: 18.4389, longitude: -72.2123, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Ouanaminthe Border Commerce Zone", pointType: "Industrial / Logistics Zone", city: "Ouanaminthe", submarket: "Other", latitude: 19.5489, longitude: -71.7234, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Hinche Central Plateau Hub", pointType: "Business District", city: "Hinche", submarket: "Other", latitude: 19.1512, longitude: -72.0167, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Port-au-Prince Convention and NGO District", pointType: "Convention Center", city: "Port-au-Prince", submarket: "Port-au-Prince", latitude: 18.5189, longitude: -72.2889, sourceReference: "https://www.haiti.org/" }),
  pt({ name: "Tortuga Island Heritage Access", pointType: "Tourist Attraction", city: "Île de la Tortue", submarket: "Other", latitude: 20.0289, longitude: -72.7912, sourceReference: "https://www.haiti.org/" }),
];

export function getHaitiCandidates() {
  return HAITI_CANDIDATES;
}

export { HAITI_SUBMARKETS };
