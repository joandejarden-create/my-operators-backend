/**
 * Antigua and Barbuda countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyAntiguaAndBarbudaGovernanceDefaults,
  ANTIGUA_AND_BARBUDA_SUBMARKETS,
} from "./antigua-and-barbuda-demand-anchor-governance.js";

const COUNTRY = "Antigua and Barbuda";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyAntiguaAndBarbudaGovernanceDefaults);

export const ANTIGUA_AND_BARBUDA_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "V.C. Bird International Airport Corridor", pointType: "Future Growth Node", city: "St. John's", submarket: "St. John's", latitude: 17.1367, longitude: -61.7928, sourceReference: "https://www.antigua-barbuda.com/", manuallyVerified: true }),
  pt({ name: "St. John's Cruise Port Heritage Quay", pointType: "Mixed-Use Development", city: "St. John's", submarket: "St. John's", latitude: 17.1234, longitude: -61.8456, sourceReference: "https://www.visitantiguabarbuda.com/", manuallyVerified: true }),
  pt({ name: "Redcliffe Quay Shopping District", pointType: "Entertainment District", city: "St. John's", submarket: "St. John's", latitude: 17.1245, longitude: -61.8434, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "St. John's Central Business District", pointType: "Business District", city: "St. John's", submarket: "St. John's", latitude: 17.1278, longitude: -61.8412, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Fort James Beach and Historic Fort", pointType: "Tourist Attraction", city: "St. John's", submarket: "St. John's", latitude: 17.1456, longitude: -61.8567, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Sir Vivian Richards Stadium", pointType: "Sports Venue", city: "North Sound", submarket: "St. John's", latitude: 17.1123, longitude: -61.8234, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Mount St. John's Medical Centre", pointType: "Medical Campus", city: "St. John's", submarket: "St. John's", latitude: 17.1189, longitude: -61.8345, sourceReference: "https://www.gov.ab/" }),
  pt({ name: "Antigua State College", pointType: "University / College", city: "St. John's", submarket: "St. John's", latitude: 17.1156, longitude: -61.8289, sourceReference: "https://www.gov.ab/" }),
  pt({ name: "Nelson's Dockyard UNESCO Site", pointType: "Tourist Attraction", city: "English Harbour", submarket: "English Harbour", latitude: 17.0323, longitude: -61.7634, sourceReference: "https://www.visitantiguabarbuda.com/", manuallyVerified: true }),
  pt({ name: "English Harbour Marina", pointType: "Beach / Waterfront", city: "English Harbour", submarket: "English Harbour", latitude: 17.0289, longitude: -61.7612, sourceReference: "https://www.visitantiguabarbuda.com/", manuallyVerified: true }),
  pt({ name: "Shirley Heights Lookout", pointType: "Tourist Attraction", city: "English Harbour", submarket: "English Harbour", latitude: 17.0234, longitude: -61.7678, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Falmouth Harbour Superyacht Marina", pointType: "Mixed-Use Development", city: "Falmouth", submarket: "English Harbour", latitude: 17.0189, longitude: -61.7789, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Antigua Yacht Club", pointType: "Beach / Waterfront", city: "English Harbour", submarket: "English Harbour", latitude: 17.0267, longitude: -61.7623, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Dickenson Bay Resort Beach", pointType: "Beach / Waterfront", city: "Dickenson Bay", submarket: "Dickenson Bay", latitude: 17.1678, longitude: -61.8456, sourceReference: "https://www.visitantiguabarbuda.com/", manuallyVerified: true }),
  pt({ name: "Runway Beach Resort Strip", pointType: "Beach / Waterfront", city: "St. John's", submarket: "Dickenson Bay", latitude: 17.1612, longitude: -61.8389, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Jolly Beach Resort Corridor", pointType: "Beach / Waterfront", city: "Jolly Harbour", submarket: "Dickenson Bay", latitude: 17.0589, longitude: -61.8912, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Jolly Harbour Marina Village", pointType: "Mixed-Use Development", city: "Jolly Harbour", submarket: "Dickenson Bay", latitude: 17.0567, longitude: -61.8934, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Devil's Bridge Natural Arch", pointType: "Tourist Attraction", city: "Willikies", submarket: "Other", latitude: 17.0934, longitude: -61.6789, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Betty's Hope Sugar Mill Heritage", pointType: "Tourist Attraction", city: "Pares", submarket: "Other", latitude: 17.0789, longitude: -61.7234, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Fig Tree Drive Rainforest Corridor", pointType: "Tourist Attraction", city: "Swetes", submarket: "Other", latitude: 17.0456, longitude: -61.8012, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Half Moon Bay Beach", pointType: "Beach / Waterfront", city: "Half Moon Bay", submarket: "Other", latitude: 17.0289, longitude: -61.6789, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Darkwood Beach", pointType: "Beach / Waterfront", city: "Darkwood", submarket: "Other", latitude: 17.0512, longitude: -61.8678, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Carlisle Bay Beach", pointType: "Beach / Waterfront", city: "Old Road", submarket: "Other", latitude: 17.0234, longitude: -61.8567, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Barbuda Frigate Bird Sanctuary", pointType: "Tourist Attraction", city: "Codrington", submarket: "Barbuda", latitude: 17.6234, longitude: -61.8012, sourceReference: "https://www.visitantiguabarbuda.com/", manuallyVerified: true }),
  pt({ name: "Codrington Lagoon National Park", pointType: "Tourist Attraction", city: "Codrington", submarket: "Barbuda", latitude: 17.6345, longitude: -61.8123, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Princess Diana Beach Barbuda", pointType: "Beach / Waterfront", city: "Codrington", submarket: "Barbuda", latitude: 17.5789, longitude: -61.8234, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Barbuda Belle Luxury Resort Zone", pointType: "Mixed-Use Development", city: "Codrington", submarket: "Barbuda", latitude: 17.5812, longitude: -61.8189, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Codrington Airport Corridor", pointType: "Future Growth Node", city: "Codrington", submarket: "Barbuda", latitude: 17.6356, longitude: -61.8289, sourceReference: "https://www.antigua-barbuda.com/", manuallyVerified: true }),
  pt({ name: "Deep Bay Beach", pointType: "Beach / Waterfront", city: "St. John's", submarket: "St. John's", latitude: 17.1389, longitude: -61.8623, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Stingray City Antigua", pointType: "Tourist Attraction", city: "Seatons", submarket: "Other", latitude: 17.0789, longitude: -61.7123, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Antigua Recreation Ground", pointType: "Sports Venue", city: "St. John's", submarket: "St. John's", latitude: 17.1212, longitude: -61.8389, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Antigua and Barbuda Investment Authority Precinct", pointType: "Government / Civic Center", city: "St. John's", submarket: "St. John's", latitude: 17.1267, longitude: -61.8445, sourceReference: "https://www.theiguides.com/" }),
  pt({ name: "Five Islands Peninsula Resort Growth", pointType: "Future Growth Node", city: "Five Islands", submarket: "St. John's", latitude: 17.1123, longitude: -61.8789, sourceReference: "https://www.theiguides.com/" }),
  pt({ name: "Crab Hill Industrial Zone", pointType: "Industrial / Logistics Zone", city: "Crab Hill", submarket: "Other", latitude: 17.0678, longitude: -61.9012, sourceReference: "https://www.gov.ab/" }),
  pt({ name: "Liberta Village Heritage", pointType: "Tourist Attraction", city: "Liberta", submarket: "English Harbour", latitude: 17.0412, longitude: -61.7923, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Long Bay Beach Antigua", pointType: "Beach / Waterfront", city: "Long Bay", submarket: "Other", latitude: 17.0678, longitude: -61.6934, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Valley Church Beach", pointType: "Beach / Waterfront", city: "Jolly Harbour", submarket: "Dickenson Bay", latitude: 17.0534, longitude: -61.8867, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Antigua Convention Bureau District", pointType: "Convention Center", city: "St. John's", submarket: "St. John's", latitude: 17.1256, longitude: -61.8423, sourceReference: "https://www.visitantiguabarbuda.com/" }),
  pt({ name: "Harmony Hall Art Gallery", pointType: "Entertainment District", city: "Brown's Bay", submarket: "Other", latitude: 17.1012, longitude: -61.7234, sourceReference: "https://www.visitantiguabarbuda.com/" }),
];

export function getAntiguaAndBarbudaCandidates() {
  return ANTIGUA_AND_BARBUDA_COUNTRYWIDE_CANDIDATES;
}

export { ANTIGUA_AND_BARBUDA_SUBMARKETS };
