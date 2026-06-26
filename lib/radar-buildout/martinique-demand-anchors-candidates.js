/**
 * Martinique Countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyMartiniqueGovernanceDefaults,
  MARTINIQUE_SUBMARKETS,
} from "./martinique-demand-anchor-governance.js";

const COUNTRY = "Martinique";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyMartiniqueGovernanceDefaults);

export const MARTINIQUE_CANDIDATES = [
  pt({ name: "Martinique Aimé Césaire International Airport Corridor", pointType: "Future Growth Node", city: "Le Lamentin", submarket: "Fort-de-France", latitude: 14.591, longitude: -61.0032, sourceReference: "https://www.martinique.aeroport.fr/", manuallyVerified: true }),
  pt({ name: "Fort-de-France Cruise Port — Pointe Simon", pointType: "Mixed-Use Development", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6012, longitude: -61.0712, sourceReference: "https://www.martinique.org/en", manuallyVerified: true }),
  pt({ name: "La Savane Park and Schoelcher Library", pointType: "Tourist Attraction", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6034, longitude: -61.0734, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Fort Saint-Louis Historic Waterfront", pointType: "Tourist Attraction", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.5989, longitude: -61.0689, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Fort-de-France Central Business District", pointType: "Business District", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6045, longitude: -61.0756, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Centre Hospitalier Universitaire de Martinique", pointType: "Medical Campus", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6123, longitude: -61.0812, sourceReference: "https://www.chu-martinique.fr/" }),
  pt({ name: "Université des Antilles — Martinique Campus", pointType: "University / College", city: "Schoelcher", submarket: "Fort-de-France", latitude: 14.6189, longitude: -61.0912, sourceReference: "https://www.univ-antilles.fr/" }),
  pt({ name: "Stade Pierre-Aliker", pointType: "Sports Venue", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6234, longitude: -61.0534, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Palais Régional Government Precinct", pointType: "Government / Civic Center", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6067, longitude: -61.0789, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Balata Botanical Garden", pointType: "Tourist Attraction", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6712, longitude: -61.0234, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Les Trois-Îlets Resort Coast", pointType: "Beach / Waterfront", city: "Les Trois-Îlets", submarket: "South Coast", latitude: 14.5389, longitude: -61.0389, sourceReference: "https://www.martinique.org/en", manuallyVerified: true }),
  pt({ name: "La Pagerie — Empress Joséphine Museum", pointType: "Tourist Attraction", city: "Les Trois-Îlets", submarket: "South Coast", latitude: 14.5412, longitude: -61.0412, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Pointe du Bout Marina and Beach", pointType: "Beach / Waterfront", city: "Les Trois-Îlets", submarket: "South Coast", latitude: 14.5312, longitude: -61.0512, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Anse Mitan Beach Resort Strip", pointType: "Beach / Waterfront", city: "Les Trois-Îlets", submarket: "South Coast", latitude: 14.5289, longitude: -61.0534, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Anse à l'Âne Beach", pointType: "Beach / Waterfront", city: "Les Trois-Îlets", submarket: "South Coast", latitude: 14.5234, longitude: -61.0489, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le Diamant Beach and Rocher du Diamant", pointType: "Tourist Attraction", city: "Le Diamant", submarket: "South Coast", latitude: 14.4789, longitude: -61.0289, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Les Anses-d'Arlet Fishing Village", pointType: "Tourist Attraction", city: "Les Anses-d'Arlet", submarket: "South Coast", latitude: 14.4912, longitude: -61.0812, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Saint-Anne Beach — Grande Anse des Salines", pointType: "Beach / Waterfront", city: "Saint-Anne", submarket: "South Coast", latitude: 14.4389, longitude: -60.8812, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Club Med Les Boucaniers", pointType: "Mixed-Use Development", city: "Sainte-Anne", submarket: "South Coast", latitude: 14.4312, longitude: -60.8934, sourceReference: "https://www.clubmed.com/" }),
  pt({ name: "Cap 110 Memorial Sculpture Park", pointType: "Tourist Attraction", city: "Le Diamant", submarket: "South Coast", latitude: 14.4612, longitude: -61.0389, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Saint-Pierre Volcano Heritage Town", pointType: "Tourist Attraction", city: "Saint-Pierre", submarket: "North Atlantic", latitude: 14.7412, longitude: -61.1756, sourceReference: "https://www.martinique.org/en", manuallyVerified: true }),
  pt({ name: "Montagne Pelée Volcano Trailhead", pointType: "Tourist Attraction", city: "Saint-Pierre", submarket: "North Atlantic", latitude: 14.8089, longitude: -61.1667, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Depaz Distillery Heritage Site", pointType: "Tourist Attraction", city: "Saint-Pierre", submarket: "North Atlantic", latitude: 14.7389, longitude: -61.1512, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le Carbet Beach and Yoles Village", pointType: "Beach / Waterfront", city: "Le Carbet", submarket: "North Atlantic", latitude: 14.7112, longitude: -61.1812, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Bellefontaine Surf Coast", pointType: "Beach / Waterfront", city: "Bellefontaine", submarket: "North Atlantic", latitude: 14.6734, longitude: -61.1689, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le Lorrain Atlantic Coast", pointType: "Beach / Waterfront", city: "Le Lorrain", submarket: "North Atlantic", latitude: 14.8312, longitude: -61.0534, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le François Bay and Islets", pointType: "Tourist Attraction", city: "Le François", submarket: "North Atlantic", latitude: 14.6189, longitude: -60.9012, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Habitation Clément Rum Distillery", pointType: "Tourist Attraction", city: "Le François", submarket: "North Atlantic", latitude: 14.6512, longitude: -60.9234, sourceReference: "https://www.habitation-clement.fr/" }),
  pt({ name: "Robert Atlantic Coast", pointType: "Beach / Waterfront", city: "Le Robert", submarket: "North Atlantic", latitude: 14.6789, longitude: -60.9412, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Presqu'île de la Caravelle Nature Reserve", pointType: "Tourist Attraction", city: "La Trinité", submarket: "North Atlantic", latitude: 14.7612, longitude: -60.9012, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le Marin Marina and Yacht Hub", pointType: "Mixed-Use Development", city: "Le Marin", submarket: "Other", latitude: 14.4712, longitude: -60.8689, sourceReference: "https://www.martinique.org/en", manuallyVerified: true }),
  pt({ name: "Rivière-Pilote East Coast Gateway", pointType: "Future Growth Node", city: "Rivière-Pilote", submarket: "Other", latitude: 14.4789, longitude: -60.9012, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le Vauclin Kitesurf Lagoon", pointType: "Beach / Waterfront", city: "Le Vauclin", submarket: "Other", latitude: 14.5489, longitude: -60.8412, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Sainte-Marie Banana Museum Corridor", pointType: "Tourist Attraction", city: "Sainte-Marie", submarket: "Other", latitude: 14.7812, longitude: -60.9234, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Le Morne-Rouge Highland Gateway", pointType: "Tourist Attraction", city: "Le Morne-Rouge", submarket: "Other", latitude: 14.7089, longitude: -61.1312, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Fort-de-France Convention and Events Zone", pointType: "Convention Center", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6089, longitude: -61.0634, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "ZAC Étang Z'Abricots Industrial Zone", pointType: "Industrial / Logistics Zone", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.5912, longitude: -61.0589, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Case-Pilote West Coast Village", pointType: "Tourist Attraction", city: "Case-Pilote", submarket: "Other", latitude: 14.6412, longitude: -61.1389, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Ducos Commercial and Retail Hub", pointType: "Business District", city: "Ducos", submarket: "Fort-de-France", latitude: 14.5789, longitude: -60.9712, sourceReference: "https://www.martinique.org/en" }),
  pt({ name: "Martinique South Cape Growth Corridor", pointType: "Future Growth Node", city: "Sainte-Anne", submarket: "South Coast", latitude: 14.4289, longitude: -60.8789, sourceReference: "https://www.martinique.org/en" }),
];

export function getMartiniqueCandidates() {
  return MARTINIQUE_CANDIDATES;
}

export { MARTINIQUE_SUBMARKETS };
