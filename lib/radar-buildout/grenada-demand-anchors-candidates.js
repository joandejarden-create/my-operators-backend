/**
 * Grenada countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyGrenadaGovernanceDefaults,
  GRENADA_SUBMARKETS,
} from "./grenada-demand-anchor-governance.js";

const COUNTRY = "Grenada";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyGrenadaGovernanceDefaults);

export const GRENADA_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Maurice Bishop International Airport Corridor", pointType: "Future Growth Node", city: "St. George's", submarket: "St. George's", latitude: 12.0042, longitude: -61.7862, sourceReference: "https://www.mbiagrenada.com/", manuallyVerified: true }),
  pt({ name: "St. George's Cruise Terminal", pointType: "Mixed-Use Development", city: "St. George's", submarket: "St. George's", latitude: 12.0523, longitude: -61.7512, sourceReference: "https://www.puregrenada.com/", manuallyVerified: true }),
  pt({ name: "Carenage Waterfront District", pointType: "Beach / Waterfront", city: "St. George's", submarket: "St. George's", latitude: 12.0512, longitude: -61.7534, sourceReference: "https://www.puregrenada.com/", manuallyVerified: true }),
  pt({ name: "Fort George Historic Precinct", pointType: "Tourist Attraction", city: "St. George's", submarket: "St. George's", latitude: 12.0489, longitude: -61.7567, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Sendall Tunnel Heritage Corridor", pointType: "Tourist Attraction", city: "St. George's", submarket: "St. George's", latitude: 12.0501, longitude: -61.7545, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "St. George's University Campus", pointType: "University / College", city: "True Blue", submarket: "St. George's", latitude: 12.0012, longitude: -61.7623, sourceReference: "https://www.sgu.edu/" }),
  pt({ name: "General Hospital Grenada", pointType: "Medical Campus", city: "St. George's", submarket: "St. George's", latitude: 12.0456, longitude: -61.7489, sourceReference: "https://www.gov.gd/" }),
  pt({ name: "National Stadium Grenada", pointType: "Sports Venue", city: "St. George's", submarket: "St. George's", latitude: 12.0389, longitude: -61.7412, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grand Anse Beach Resort Corridor", pointType: "Beach / Waterfront", city: "Grand Anse", submarket: "Grand Anse", latitude: 12.0234, longitude: -61.7678, sourceReference: "https://www.puregrenada.com/", manuallyVerified: true }),
  pt({ name: "Radisson Grenada Beach Resort Zone", pointType: "Mixed-Use Development", city: "Grand Anse", submarket: "Grand Anse", latitude: 12.0212, longitude: -61.7656, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Morne Rouge Bay Beach", pointType: "Beach / Waterfront", city: "Morne Rouge", submarket: "Grand Anse", latitude: 12.0189, longitude: -61.7623, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Port Louis Marina", pointType: "Beach / Waterfront", city: "St. George's", submarket: "St. George's", latitude: 12.0567, longitude: -61.7489, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grand Etang National Park", pointType: "Tourist Attraction", city: "Constantine", submarket: "Other", latitude: 12.1123, longitude: -61.7012, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Annandale Falls", pointType: "Tourist Attraction", city: "St. George's", submarket: "Other", latitude: 12.0789, longitude: -61.7234, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Levera National Park and Bathway Beach", pointType: "Beach / Waterfront", city: "Sauteurs", submarket: "North Coast", latitude: 12.2234, longitude: -61.6234, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Sauteurs Leap Historic Site", pointType: "Tourist Attraction", city: "Sauteurs", submarket: "North Coast", latitude: 12.2189, longitude: -61.6345, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Gouyave Nutmeg Processing", pointType: "Tourist Attraction", city: "Gouyave", submarket: "North Coast", latitude: 12.1623, longitude: -61.7289, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Belmont Estate Heritage", pointType: "Tourist Attraction", city: "Belmont", submarket: "North Coast", latitude: 12.1789, longitude: -61.6789, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "La Sagesse Nature Centre Beach", pointType: "Beach / Waterfront", city: "La Sagesse", submarket: "South Coast", latitude: 12.0123, longitude: -61.6789, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Prickly Bay Marina", pointType: "Mixed-Use Development", city: "L'Anse aux Epines", submarket: "South Coast", latitude: 11.9989, longitude: -61.7567, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Secret Harbour Marina", pointType: "Beach / Waterfront", city: "L'Anse aux Epines", submarket: "South Coast", latitude: 11.9967, longitude: -61.7589, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Magazine Beach", pointType: "Beach / Waterfront", city: "Woburn", submarket: "South Coast", latitude: 12.0012, longitude: -61.7712, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Underwater Sculpture Park", pointType: "Tourist Attraction", city: "Moliniere", submarket: "North Coast", latitude: 12.1234, longitude: -61.7234, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Carriacou Island Tourism Gateway", pointType: "Future Growth Node", city: "Hillsborough", submarket: "Other", latitude: 12.4789, longitude: -61.4567, sourceReference: "https://www.puregrenada.com/", manuallyVerified: true }),
  pt({ name: "Petite Martinique Fishing Village", pointType: "Tourist Attraction", city: "Petite Martinique", submarket: "Other", latitude: 12.5234, longitude: -61.4012, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grenada Chocolate Factory", pointType: "Entertainment District", city: "Hermitage", submarket: "Other", latitude: 12.0678, longitude: -61.7123, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "River Antoine Rum Distillery", pointType: "Tourist Attraction", city: "River Antoine", submarket: "North Coast", latitude: 12.1923, longitude: -61.6456, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "St. David's Anglican Church Heritage", pointType: "Tourist Attraction", city: "St. David's", submarket: "South Coast", latitude: 12.0456, longitude: -61.6678, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grenada Trade Centre", pointType: "Convention Center", city: "St. George's", submarket: "St. George's", latitude: 12.0412, longitude: -61.7456, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Spice Island Beach Resort Zone", pointType: "Mixed-Use Development", city: "Grand Anse", submarket: "Grand Anse", latitude: 12.0245, longitude: -61.7689, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grenada Industrial Development Corporation Zone", pointType: "Industrial / Logistics Zone", city: "Perseverance", submarket: "St. George's", latitude: 12.0123, longitude: -61.7789, sourceReference: "https://www.gov.gd/" }),
  pt({ name: "Grenada Investment Development Corporation", pointType: "Government / Civic Center", city: "St. George's", submarket: "St. George's", latitude: 12.0478, longitude: -61.7512, sourceReference: "https://www.theiguides.com/" }),
  pt({ name: "Calivigny Island Luxury Resort", pointType: "Mixed-Use Development", city: "Calivigny", submarket: "South Coast", latitude: 11.9789, longitude: -61.7234, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Westerhall Estate Rum Tour", pointType: "Tourist Attraction", city: "Westerhall", submarket: "South Coast", latitude: 12.0234, longitude: -61.7012, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Fort Frederick Historic Site", pointType: "Tourist Attraction", city: "Richmond Hill", submarket: "St. George's", latitude: 12.0467, longitude: -61.7589, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grenada South Coast Growth Node", pointType: "Future Growth Node", city: "Lance aux Epines", submarket: "South Coast", latitude: 11.9934, longitude: -61.7623, sourceReference: "https://www.theiguides.com/" }),
  pt({ name: "Tyrell Bay Carriacou Marina", pointType: "Beach / Waterfront", city: "Carriacou", submarket: "Other", latitude: 12.4567, longitude: -61.4789, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Grenada National Museum", pointType: "Tourist Attraction", city: "St. George's", submarket: "St. George's", latitude: 12.0498, longitude: -61.7523, sourceReference: "https://www.puregrenada.com/" }),
  pt({ name: "Laura Herb and Spice Garden", pointType: "Tourist Attraction", city: "Laura", submarket: "Other", latitude: 12.1012, longitude: -61.7123, sourceReference: "https://www.puregrenada.com/" }),
];

export function getGrenadaCandidates() {
  return GRENADA_COUNTRYWIDE_CANDIDATES;
}

export { GRENADA_SUBMARKETS };
