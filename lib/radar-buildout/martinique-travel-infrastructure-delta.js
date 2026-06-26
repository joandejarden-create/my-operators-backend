/**
 * Martinique Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Martinique";
const MARKET = "Martinique Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const MARTINIQUE_TI_DELTA_RECORDS = [
  ti({ name: "Martinique Aimé Césaire Airport Access", pointType: "Highway Access", city: "Le Lamentin", submarket: "Fort-de-France", latitude: 14.591, longitude: -61.0032, sourceReference: "https://www.martinique.aeroport.fr/", pointSubtype: "Airport Access", notes: "Primary international gateway for Fort-de-France metro demand." }),
  ti({ name: "Fort-de-France Cruise Port Access", pointType: "Port / Maritime", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.6012, longitude: -61.0712, sourceReference: "https://www.martinique.org/en", pointSubtype: "Cruise Terminal", notes: "Pointe Simon cruise terminal for capital and south-coast demand.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Les Trois-Îlets Ferry and Marina Access", pointType: "Ferry Terminal", city: "Les Trois-Îlets", submarket: "South Coast", latitude: 14.5312, longitude: -61.0512, sourceReference: "https://www.martinique.org/en", pointSubtype: "Marina", notes: "South coast ferry link from Fort-de-France to resort beaches." }),
  ti({ name: "Le Marin Marina Access", pointType: "Port / Maritime", city: "Le Marin", submarket: "Other", latitude: 14.4712, longitude: -60.8689, sourceReference: "https://www.martinique.org/en", pointSubtype: "Marina", notes: "Major yacht marina hub for south Martinique sailing demand.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Autoroute A1 Fort-de-France Access", pointType: "Highway Access", city: "Fort-de-France", submarket: "Fort-de-France", latitude: 14.5989, longitude: -61.0512, sourceReference: "https://www.martinique.org/en", pointSubtype: "National Highway", notes: "Primary highway connector from airport to capital." }),
  ti({ name: "Saint-Pierre North Coast Scenic Access", pointType: "Highway Access", city: "Saint-Pierre", submarket: "North Atlantic", latitude: 14.7412, longitude: -61.1756, sourceReference: "https://www.martinique.org/en", pointSubtype: "Scenic Corridor", notes: "North Atlantic scenic road to volcano heritage tourism." }),
  ti({ name: "Grande Anse des Salines Beach Access", pointType: "Highway Access", city: "Saint-Anne", submarket: "South Coast", latitude: 14.4389, longitude: -60.8812, sourceReference: "https://www.martinique.org/en", pointSubtype: "Resort Corridor", notes: "South cape beach road access for resort and day-trip demand." }),
  ti({ name: "Le François Bay Maritime Access", pointType: "Port / Maritime", city: "Le François", submarket: "North Atlantic", latitude: 14.6189, longitude: -60.9012, sourceReference: "https://www.martinique.org/en", pointSubtype: "Harbour Access", notes: "Atlantic bay harbour access for islet excursions and leisure boating." }),
];

export function buildMartiniqueTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, MARTINIQUE_TI_DELTA_RECORDS);
}
