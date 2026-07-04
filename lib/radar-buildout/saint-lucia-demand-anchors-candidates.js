/**
 * Saint Lucia countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applySaintLuciaGovernanceDefaults,
  SAINT_LUCIA_SUBMARKETS,
} from "./saint-lucia-demand-anchor-governance.js";

const COUNTRY = "Saint Lucia";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applySaintLuciaGovernanceDefaults);

export const SAINT_LUCIA_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Hewanorra International Airport Corridor", pointType: "Future Growth Node", city: "Vieux Fort", submarket: "Vieux Fort", latitude: 13.7332, longitude: -60.9526, sourceReference: "https://www.slulimited.com/", manuallyVerified: true }),
  pt({ name: "George F. L. Charles Airport", pointType: "Future Growth Node", city: "Castries", submarket: "Castries", latitude: 14.0202, longitude: -60.9929, sourceReference: "https://www.slulimited.com/", manuallyVerified: true }),
  pt({ name: "Port of Castries Cruise Terminal", pointType: "Mixed-Use Development", city: "Castries", submarket: "Castries", latitude: 14.0107, longitude: -60.9915, sourceReference: "https://www.stlucia.org/", manuallyVerified: true }),
  pt({ name: "Castries Central Business District", pointType: "Business District", city: "Castries", submarket: "Castries", latitude: 14.0101, longitude: -60.9895, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Duty Free Pointe Seraphine", pointType: "Entertainment District", city: "Castries", submarket: "Castries", latitude: 14.0139, longitude: -60.9878, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "OECS Commission Headquarters Precinct", pointType: "Government / Civic Center", city: "Castries", submarket: "Castries", latitude: 14.0078, longitude: -60.9934, sourceReference: "https://www.oecs.org/" }),
  pt({ name: "Owen King EU Hospital", pointType: "Medical Campus", city: "Castries", submarket: "Castries", latitude: 14.0056, longitude: -60.9789, sourceReference: "https://www.stlucia.gov.lc/" }),
  pt({ name: "Sir Arthur Lewis Community College", pointType: "University / College", city: "Castries", submarket: "Castries", latitude: 14.0189, longitude: -60.9812, sourceReference: "https://www.salcc.edu.lc/" }),
  pt({ name: "Daren Sammy Cricket Ground", pointType: "Sports Venue", city: "Gros Islet", submarket: "Rodney Bay / Gros Islet", latitude: 14.0689, longitude: -60.9534, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Rodney Bay Marina", pointType: "Beach / Waterfront", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0742, longitude: -60.9512, sourceReference: "https://www.stlucia.org/", manuallyVerified: true }),
  pt({ name: "Reduit Beach Resort Corridor", pointType: "Beach / Waterfront", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0789, longitude: -60.9498, sourceReference: "https://www.stlucia.org/", manuallyVerified: true }),
  pt({ name: "Pigeon Island National Landmark", pointType: "Tourist Attraction", city: "Gros Islet", submarket: "Rodney Bay / Gros Islet", latitude: 14.0923, longitude: -60.9612, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Gros Islet Friday Night Street Party", pointType: "Entertainment District", city: "Gros Islet", submarket: "Rodney Bay / Gros Islet", latitude: 14.0798, longitude: -60.9456, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Cap Estate Golf and Resort Zone", pointType: "Mixed-Use Development", city: "Cap Estate", submarket: "Rodney Bay / Gros Islet", latitude: 14.1012, longitude: -60.9478, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Atlantic Rally for Cruisers Finish", pointType: "Future Growth Node", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0756, longitude: -60.9523, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Marigot Bay Heritage Harbour", pointType: "Tourist Attraction", city: "Marigot Bay", submarket: "Other", latitude: 13.9478, longitude: -61.0267, sourceReference: "https://www.stlucia.org/", manuallyVerified: true }),
  pt({ name: "Pitons Management Area UNESCO", pointType: "Tourist Attraction", city: "Soufrière", submarket: "Soufrière", latitude: 13.8089, longitude: -61.0678, sourceReference: "https://www.stlucia.org/", manuallyVerified: true }),
  pt({ name: "Soufrière Town Waterfront", pointType: "Beach / Waterfront", city: "Soufrière", submarket: "Soufrière", latitude: 13.8567, longitude: -61.0567, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Sulphur Springs Drive-In Volcano", pointType: "Tourist Attraction", city: "Soufrière", submarket: "Soufrière", latitude: 13.8412, longitude: -61.0456, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Diamond Falls Botanical Gardens", pointType: "Tourist Attraction", city: "Soufrière", submarket: "Soufrière", latitude: 13.8534, longitude: -61.0512, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Anse Chastanet Resort Beach", pointType: "Beach / Waterfront", city: "Soufrière", submarket: "Soufrière", latitude: 13.8623, longitude: -61.0789, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Sugar Beach Pitons View Corridor", pointType: "Beach / Waterfront", city: "Soufrière", submarket: "Soufrière", latitude: 13.8645, longitude: -61.0756, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Fond Doux Plantation Resort", pointType: "Tourist Attraction", city: "Soufrière", submarket: "Soufrière", latitude: 13.8489, longitude: -61.0389, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Vieux Fort Industrial Free Zone", pointType: "Industrial / Logistics Zone", city: "Vieux Fort", submarket: "Vieux Fort", latitude: 13.7289, longitude: -60.9612, sourceReference: "https://www.stlucia.gov.lc/" }),
  pt({ name: "Micoud Bay Fishing Village", pointType: "Beach / Waterfront", city: "Micoud", submarket: "Other", latitude: 13.8123, longitude: -60.9012, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Dennery Fishing Village East Coast", pointType: "Beach / Waterfront", city: "Dennery", submarket: "Other", latitude: 13.9012, longitude: -60.8934, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Choiseul Heritage Coast", pointType: "Tourist Attraction", city: "Choiseul", submarket: "Other", latitude: 13.7723, longitude: -61.0456, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Laborie Bay South Coast", pointType: "Beach / Waterfront", city: "Laborie", submarket: "Other", latitude: 13.7512, longitude: -60.9934, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Canaries Fishing Village", pointType: "Tourist Attraction", city: "Canaries", submarket: "Other", latitude: 13.9012, longitude: -61.0623, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Saint Lucia Conference Centre", pointType: "Convention Center", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0723, longitude: -60.9534, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Baywalk Shopping Plaza Rodney Bay", pointType: "Entertainment District", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0767, longitude: -60.9501, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Windward Islands Research and Education Foundation", pointType: "University / College", city: "Soufrière", submarket: "Soufrière", latitude: 13.8578, longitude: -61.0545, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Saint Lucia South Coast Growth Node", pointType: "Future Growth Node", city: "Vieux Fort", submarket: "Vieux Fort", latitude: 13.7267, longitude: -60.9489, sourceReference: "https://www.investstlucia.com/", manuallyVerified: true }),
  pt({ name: "Rodney Bay Resort Expansion Corridor", pointType: "Future Growth Node", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0812, longitude: -60.9478, sourceReference: "https://www.investstlucia.com/" }),
  pt({ name: "Castries Waterfront Redevelopment", pointType: "Mixed-Use Development", city: "Castries", submarket: "Castries", latitude: 14.0098, longitude: -60.9889, sourceReference: "https://www.investstlucia.com/" }),
  pt({ name: "Anse La Raye Fish Fry", pointType: "Entertainment District", city: "Anse La Raye", submarket: "Other", latitude: 13.9456, longitude: -61.0389, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Babonneau Community Tourism Corridor", pointType: "Tourist Attraction", city: "Babonneau", submarket: "Other", latitude: 14.0123, longitude: -60.9234, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Ti Rocher Rainforest Adventure Zone", pointType: "Tourist Attraction", city: "Castries", submarket: "Other", latitude: 13.9789, longitude: -60.9678, sourceReference: "https://www.stlucia.org/" }),
  pt({ name: "Saint Lucia National Stadium Precinct", pointType: "Sports Venue", city: "Bisee", submarket: "Castries", latitude: 14.0234, longitude: -60.9712, sourceReference: "https://www.stlucia.gov.lc/" }),
];

export function getSaintLuciaCandidates() {
  return SAINT_LUCIA_COUNTRYWIDE_CANDIDATES;
}

export { SAINT_LUCIA_SUBMARKETS };
