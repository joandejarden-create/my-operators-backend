/**
 * Guadeloupe Countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyGuadeloupeGovernanceDefaults,
  GUADELOUPE_SUBMARKETS,
} from "./guadeloupe-demand-anchor-governance.js";

const COUNTRY = "Guadeloupe";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyGuadeloupeGovernanceDefaults);

export const GUADELOUPE_CANDIDATES = [
  pt({ name: "Pointe-à-Pitre International Airport Corridor", pointType: "Future Growth Node", city: "Les Abymes", submarket: "Pointe-à-Pitre", latitude: 16.2653, longitude: -61.5318, sourceReference: "https://www.guadeloupe.aeroport.fr/", manuallyVerified: true }),
  pt({ name: "Port of Pointe-à-Pitre Cruise Terminal", pointType: "Mixed-Use Development", city: "Pointe-à-Pitre", submarket: "Pointe-à-Pitre", latitude: 16.2412, longitude: -61.5334, sourceReference: "https://www.guadeloupe-islands.com/en", manuallyVerified: true }),
  pt({ name: "Pointe-à-Pitre Central Market and Downtown", pointType: "Entertainment District", city: "Pointe-à-Pitre", submarket: "Pointe-à-Pitre", latitude: 16.2412, longitude: -61.5312, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Memorial ACTe Museum", pointType: "Tourist Attraction", city: "Pointe-à-Pitre", submarket: "Pointe-à-Pitre", latitude: 16.2289, longitude: -61.5389, sourceReference: "https://memorial-acte.fr/" }),
  pt({ name: "Centre Hospitalier Universitaire de Pointe-à-Pitre", pointType: "Medical Campus", city: "Pointe-à-Pitre", submarket: "Pointe-à-Pitre", latitude: 16.2512, longitude: -61.5289, sourceReference: "https://www.chu-guadeloupe.fr/" }),
  pt({ name: "Université des Antilles — Guadeloupe Campus", pointType: "University / College", city: "Pointe-à-Pitre", submarket: "Pointe-à-Pitre", latitude: 16.2534, longitude: -61.5412, sourceReference: "https://www.univ-antilles.fr/" }),
  pt({ name: "Destreland Shopping Center", pointType: "Business District", city: "Baie-Mahault", submarket: "Pointe-à-Pitre", latitude: 16.2689, longitude: -61.5712, sourceReference: "https://www.destreland.com/" }),
  pt({ name: "Jarry Industrial and Port Zone", pointType: "Industrial / Logistics Zone", city: "Baie-Mahault", submarket: "Pointe-à-Pitre", latitude: 16.2412, longitude: -61.5589, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Stade René Serge Nabajoth", pointType: "Sports Venue", city: "Les Abymes", submarket: "Pointe-à-Pitre", latitude: 16.2712, longitude: -61.5089, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Place de la Victoire Civic Precinct", pointType: "Government / Civic Center", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0012, longitude: -61.7312, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Sainte-Anne Beach Resort Coast", pointType: "Beach / Waterfront", city: "Sainte-Anne", submarket: "Grande-Terre", latitude: 16.2289, longitude: -61.3812, sourceReference: "https://www.guadeloupe-islands.com/en", manuallyVerified: true }),
  pt({ name: "Saint-François Marina and Golf Resort", pointType: "Mixed-Use Development", city: "Saint-François", submarket: "Grande-Terre", latitude: 16.2512, longitude: -61.2712, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Pointe des Châteaux Scenic Headland", pointType: "Tourist Attraction", city: "Saint-François", submarket: "Grande-Terre", latitude: 16.2489, longitude: -61.1789, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Le Gosier Beach and Fort Fleur d'Épée", pointType: "Beach / Waterfront", city: "Le Gosier", submarket: "Grande-Terre", latitude: 16.2012, longitude: -61.4912, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Datcha Beach Le Gosier", pointType: "Beach / Waterfront", city: "Le Gosier", submarket: "Grande-Terre", latitude: 16.1989, longitude: -61.5012, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Porte d'Enfer Coastal Arch", pointType: "Tourist Attraction", city: "Anse-Bertrand", submarket: "Grande-Terre", latitude: 16.4712, longitude: -61.4512, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Moule East Coast Fishing Village", pointType: "Tourist Attraction", city: "Le Moule", submarket: "Grande-Terre", latitude: 16.3312, longitude: -61.3412, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "La Pointe de la Grande Vigie", pointType: "Tourist Attraction", city: "Anse-Bertrand", submarket: "Grande-Terre", latitude: 16.5189, longitude: -61.4512, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Morne-à-l'Eau Heritage Town", pointType: "Tourist Attraction", city: "Morne-à-l'Eau", submarket: "Grande-Terre", latitude: 16.3289, longitude: -61.4712, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "La Soufrière Volcano Summit Trail", pointType: "Tourist Attraction", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0445, longitude: -61.6645, sourceReference: "https://www.guadeloupe-islands.com/en", manuallyVerified: true }),
  pt({ name: "Parc National de la Guadeloupe", pointType: "Tourist Attraction", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0189, longitude: -61.6912, sourceReference: "https://www.guadeloupe-parcnational.fr/" }),
  pt({ name: "Cascade aux Écrevisses Waterfall", pointType: "Tourist Attraction", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0289, longitude: -61.6789, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Deshaies Botanical Garden", pointType: "Tourist Attraction", city: "Deshaies", submarket: "Basse-Terre", latitude: 16.3112, longitude: -61.7912, sourceReference: "https://www.jardin-botanique.com/" }),
  pt({ name: "Plage de Grande Anse Deshaies", pointType: "Beach / Waterfront", city: "Deshaies", submarket: "Basse-Terre", latitude: 16.3189, longitude: -61.8012, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Bouillante Hot Springs and Cousteau Reserve", pointType: "Tourist Attraction", city: "Bouillante", submarket: "Basse-Terre", latitude: 16.1289, longitude: -61.7689, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Plage de Malendure Snorkel Coast", pointType: "Beach / Waterfront", city: "Bouillante", submarket: "Basse-Terre", latitude: 16.1212, longitude: -61.7712, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Fort Delgrès Historic Site", pointType: "Tourist Attraction", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0012, longitude: -61.7289, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Trois-Rivières South Basse-Terre Coast", pointType: "Beach / Waterfront", city: "Trois-Rivières", submarket: "Basse-Terre", latitude: 15.9789, longitude: -61.6412, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Terre-de-Haut Les Saintes Harbour", pointType: "Tourist Attraction", city: "Terre-de-Haut", submarket: "Other", latitude: 15.8689, longitude: -61.5812, sourceReference: "https://www.guadeloupe-islands.com/en", manuallyVerified: true }),
  pt({ name: "Fort Napoléon des Saintes", pointType: "Tourist Attraction", city: "Terre-de-Haut", submarket: "Other", latitude: 15.8712, longitude: -61.5834, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Marie-Galante Distillery Heritage", pointType: "Tourist Attraction", city: "Grand-Bourg", submarket: "Other", latitude: 15.8812, longitude: -61.3189, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Plage de la Feuillère Marie-Galante", pointType: "Beach / Waterfront", city: "Grand-Bourg", submarket: "Other", latitude: 15.8689, longitude: -61.2512, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "La Désirade Island Nature Reserve", pointType: "Tourist Attraction", city: "Beauséjour", submarket: "Other", latitude: 16.3189, longitude: -61.0512, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Carbet Falls — Première Chute", pointType: "Tourist Attraction", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0389, longitude: -61.7012, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Rivière-Sens Convention and Events Zone", pointType: "Convention Center", city: "Les Abymes", submarket: "Pointe-à-Pitre", latitude: 16.2612, longitude: -61.5189, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Petit-Bourg Rainforest Gateway", pointType: "Future Growth Node", city: "Petit-Bourg", submarket: "Basse-Terre", latitude: 16.0312, longitude: -61.6712, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Lamentin Commercial Corridor", pointType: "Business District", city: "Lamentin", submarket: "Pointe-à-Pitre", latitude: 16.2712, longitude: -61.5512, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Capesterre-Belle-Eau East Basse-Terre", pointType: "Tourist Attraction", city: "Capesterre-Belle-Eau", submarket: "Basse-Terre", latitude: 16.0512, longitude: -61.5712, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Vieux-Habitants Coffee Heritage Coast", pointType: "Tourist Attraction", city: "Vieux-Habitants", submarket: "Basse-Terre", latitude: 16.0589, longitude: -61.7612, sourceReference: "https://www.guadeloupe-islands.com/en" }),
  pt({ name: "Guadeloupe Grande-Terre Resort Growth Corridor", pointType: "Future Growth Node", city: "Saint-François", submarket: "Grande-Terre", latitude: 16.2489, longitude: -61.2812, sourceReference: "https://www.guadeloupe-islands.com/en" }),
];

export function getGuadeloupeCandidates() {
  return GUADELOUPE_CANDIDATES;
}

export { GUADELOUPE_SUBMARKETS };
