/**
 * Guadeloupe Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Guadeloupe";
const MARKET = "Guadeloupe Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const GUADELOUPE_TI_DELTA_RECORDS = [
  ti({ name: "Pointe-à-Pitre International Airport Access", pointType: "Highway Access", city: "Les Abymes", submarket: "Pointe-à-Pitre", latitude: 16.2653, longitude: -61.5318, sourceReference: "https://www.guadeloupe.aeroport.fr/", pointSubtype: "Airport Access", notes: "Pôle Caraïbes international gateway for Grande-Terre demand." }),
  ti({ name: "Port of Pointe-à-Pitre Cruise Terminal Access", pointType: "Port / Maritime", city: "Pointe-à-Pitre", submarket: "Pointe-à-Pitre", latitude: 16.2412, longitude: -61.5334, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Cruise Terminal", notes: "Main cruise berth for Guadeloupe archipelago demand.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Jarry Port and Industrial Zone Access", pointType: "Highway Access", city: "Baie-Mahault", submarket: "Pointe-à-Pitre", latitude: 16.2412, longitude: -61.5589, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Industrial Corridor", notes: "Cargo port and free-zone connector for business travel.", useCaseTags: ["Industrial / Logistics"] }),
  ti({ name: "Saint-François Marina Access", pointType: "Port / Maritime", city: "Saint-François", submarket: "Grande-Terre", latitude: 16.2512, longitude: -61.2712, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Marina", notes: "East Grande-Terre marina and resort gateway.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Sainte-Anne Resort Coast Highway Access", pointType: "Highway Access", city: "Sainte-Anne", submarket: "Grande-Terre", latitude: 16.2289, longitude: -61.3812, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Resort Corridor", notes: "Primary beach resort road access on south Grande-Terre." }),
  ti({ name: "Basse-Terre La Soufrière Scenic Access", pointType: "Highway Access", city: "Basse-Terre", submarket: "Basse-Terre", latitude: 16.0445, longitude: -61.6645, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Scenic Corridor", notes: "Volcano national park road access for eco-tourism demand." }),
  ti({ name: "Terre-de-Haut Ferry Terminal Access", pointType: "Ferry Terminal", city: "Terre-de-Haut", submarket: "Other", latitude: 15.8689, longitude: -61.5812, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Inter-Island Ferry", notes: "Les Saintes ferry access from Basse-Terre coast." }),
  ti({ name: "Marie-Galante Ferry Terminal Access", pointType: "Ferry Terminal", city: "Grand-Bourg", submarket: "Other", latitude: 15.8812, longitude: -61.3189, sourceReference: "https://www.guadeloupe-islands.com/en", pointSubtype: "Inter-Island Ferry", notes: "Marie-Galante island ferry link from Grande-Terre." }),
];

export function buildGuadeloupeTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, GUADELOUPE_TI_DELTA_RECORDS);
}
