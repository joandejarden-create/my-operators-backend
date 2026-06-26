/**
 * Belize countrywide demand anchor candidates (source-backed).
 */
import {
  createCentralAmericaCandidateBuilder,
  createCentralAmericaGovernance,
} from "./central-america-country-shared.js";

const COUNTRY = "Belize";
const applyBelizeGovernanceDefaults = createCentralAmericaGovernance("Belize");
const pt = createCentralAmericaCandidateBuilder(COUNTRY, applyBelizeGovernanceDefaults);

export const BELIZE_SUBMARKETS = [
  "Belize City",
  "Ambergris Caye",
  "Placencia",
  "San Ignacio",
  "Caye Caulker",
  "Other",
];

export const BELIZE_CANDIDATES = [
  pt({ name: "Philip S. W. Goldson International Airport Corridor", pointType: "Future Growth Node", city: "Belize City", submarket: "Belize City", latitude: 17.5392, longitude: -88.3082, sourceReference: "https://www.pgiaservices.com/", manuallyVerified: true }),
  pt({ name: "Belize Tourism Village Cruise Port", pointType: "Mixed-Use Development", city: "Belize City", submarket: "Belize City", latitude: 17.4925, longitude: -88.1856, sourceReference: "https://www.travelbelize.org/", manuallyVerified: true }),
  pt({ name: "Karl Heusner Memorial Hospital", pointType: "Medical Campus", city: "Belize City", submarket: "Belize City", latitude: 17.4986, longitude: -88.1897, sourceReference: "https://www.khmh.bz/" }),
  pt({ name: "University of Belize Belize City Campus", pointType: "University / College", city: "Belize City", submarket: "Belize City", latitude: 17.4983, longitude: -88.1972, sourceReference: "https://www.ub.edu.bz/" }),
  pt({ name: "Belize City Albert Street Business District", pointType: "Business District", city: "Belize City", submarket: "Belize City", latitude: 17.4993, longitude: -88.1886, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "St. John's Cathedral Historic District", pointType: "Tourist Attraction", city: "Belize City", submarket: "Belize City", latitude: 17.4936, longitude: -88.1889, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "Museum of Belize and Central Bank Precinct", pointType: "Tourist Attraction", city: "Belize City", submarket: "Belize City", latitude: 17.4998, longitude: -88.1912, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "Fort George Marina and Waterfront", pointType: "Beach / Waterfront", city: "Belize City", submarket: "Belize City", latitude: 17.4928, longitude: -88.1856, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Belize City House of Culture and Government House", pointType: "Government / Civic Center", city: "Belize City", submarket: "Belize City", latitude: 17.4931, longitude: -88.1878, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "Belize City Swing Bridge and Commercial Waterfront", pointType: "Entertainment District", city: "Belize City", submarket: "Belize City", latitude: 17.4945, longitude: -88.1867, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "San Pedro Airport Corridor", pointType: "Future Growth Node", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9139, longitude: -87.9711, sourceReference: "https://www.tropicair.com/", manuallyVerified: true }),
  pt({ name: "Hol Chan Marine Reserve", pointType: "Tourist Attraction", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9167, longitude: -87.9500, sourceReference: "https://belizeaudubon.org/protected-areas/hol-chan-marine-reserve/", manuallyVerified: true }),
  pt({ name: "Shark Ray Alley Snorkel Site", pointType: "Tourist Attraction", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9189, longitude: -87.9456, sourceReference: "https://belizeaudubon.org/protected-areas/hol-chan-marine-reserve/" }),
  pt({ name: "San Pedro Barrier Reef Drive Beach Strip", pointType: "Beach / Waterfront", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9214, longitude: -87.9611, sourceReference: "https://www.travelbelize.org/", manuallyVerified: true }),
  pt({ name: "Secret Beach North Ambergris Coast", pointType: "Beach / Waterfront", city: "San Pedro", submarket: "Ambergris Caye", latitude: 18.0012, longitude: -87.9234, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Bacalar Chico Marine Reserve", pointType: "Tourist Attraction", city: "San Pedro", submarket: "Ambergris Caye", latitude: 18.0512, longitude: -87.8512, sourceReference: "https://belizeaudubon.org/protected-areas/bacalar-chico-marine-reserve/" }),
  pt({ name: "San Pedro Town Tourism and Dining Core", pointType: "Entertainment District", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9201, longitude: -87.9623, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Marco Gonzalez Maya Site Ambergris", pointType: "Tourist Attraction", city: "San Pedro", submarket: "Ambergris Caye", latitude: 17.9012, longitude: -87.9789, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "Placencia Peninsula Beach Resort Coast", pointType: "Beach / Waterfront", city: "Placencia", submarket: "Placencia", latitude: 16.5122, longitude: -88.3661, sourceReference: "https://www.travelbelize.org/", manuallyVerified: true }),
  pt({ name: "Placencia Airport Corridor", pointType: "Future Growth Node", city: "Placencia", submarket: "Placencia", latitude: 16.5369, longitude: -88.3614, sourceReference: "https://www.tropicair.com/", manuallyVerified: true }),
  pt({ name: "Placencia Village Tourism Core", pointType: "Entertainment District", city: "Placencia", submarket: "Placencia", latitude: 16.5145, longitude: -88.3645, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Seine Bight Garifuna Village", pointType: "Tourist Attraction", city: "Placencia", submarket: "Placencia", latitude: 16.5289, longitude: -88.3712, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Laughing Bird Caye National Park", pointType: "Tourist Attraction", city: "Placencia", submarket: "Placencia", latitude: 16.4434, longitude: -88.2012, sourceReference: "https://belizeaudubon.org/protected-areas/laughing-bird-caye-national-park/" }),
  pt({ name: "Maya Beach Resort Strip", pointType: "Mixed-Use Development", city: "Placencia", submarket: "Placencia", latitude: 16.5512, longitude: -88.3789, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Xunantunich Mayan Archaeological Site", pointType: "Tourist Attraction", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.0833, longitude: -89.1333, sourceReference: "https://nichbelize.org/", manuallyVerified: true }),
  pt({ name: "Cahal Pech Mayan Archaeological Site", pointType: "Tourist Attraction", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1517, longitude: -89.0767, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "San Ignacio Town Tourism District", pointType: "Entertainment District", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1588, longitude: -89.0696, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Green Iguana Conservation Project", pointType: "Tourist Attraction", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1612, longitude: -89.0712, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Actun Tunichil Muknal Cave Tourism Gateway", pointType: "Tourist Attraction", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.0712, longitude: -88.7234, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "San Ignacio Resort Hotel Ridge Corridor", pointType: "Mixed-Use Development", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1545, longitude: -89.0634, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Western Highway San Ignacio Business Corridor", pointType: "Business District", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1567, longitude: -89.0789, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Belize Botanic Gardens", pointType: "Tourist Attraction", city: "San Ignacio", submarket: "San Ignacio", latitude: 17.1234, longitude: -89.0512, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Caye Caulker Marine Reserve and The Split", pointType: "Beach / Waterfront", city: "Caye Caulker", submarket: "Caye Caulker", latitude: 17.7417, longitude: -88.0267, sourceReference: "https://belizeaudubon.org/protected-areas/caye-caulker-marine-reserve/", manuallyVerified: true }),
  pt({ name: "Caye Caulker Airport Corridor", pointType: "Future Growth Node", city: "Caye Caulker", submarket: "Caye Caulker", latitude: 17.7347, longitude: -88.0328, sourceReference: "https://www.tropicair.com/", manuallyVerified: true }),
  pt({ name: "Caye Caulker Village Tourism Core", pointType: "Entertainment District", city: "Caye Caulker", submarket: "Caye Caulker", latitude: 17.7356, longitude: -88.0278, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Caye Caulker North Beach Snorkel Coast", pointType: "Beach / Waterfront", city: "Caye Caulker", submarket: "Caye Caulker", latitude: 17.7489, longitude: -88.0234, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Caye Caulker Ferry and Water Taxi Terminal", pointType: "Mixed-Use Development", city: "Caye Caulker", submarket: "Caye Caulker", latitude: 17.7367, longitude: -88.0289, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Great Blue Hole Lighthouse Reef", pointType: "Tourist Attraction", city: "Lighthouse Reef", submarket: "Other", latitude: 17.3158, longitude: -87.5347, sourceReference: "https://whc.unesco.org/en/list/764/", manuallyVerified: true }),
  pt({ name: "Caracol Archaeological Reserve", pointType: "Tourist Attraction", city: "Caracol", submarket: "Other", latitude: 16.7633, longitude: -89.1178, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "Lamanai Archaeological Reserve", pointType: "Tourist Attraction", city: "Orange Walk", submarket: "Other", latitude: 17.7594, longitude: -88.6547, sourceReference: "https://nichbelize.org/" }),
  pt({ name: "Belmopan Government and Civic Center", pointType: "Government / Civic Center", city: "Belmopan", submarket: "Other", latitude: 17.2514, longitude: -88.7669, sourceReference: "https://www.travelbelize.org/" }),
  pt({ name: "Half Moon Caye Natural Monument", pointType: "Tourist Attraction", city: "Lighthouse Reef", submarket: "Other", latitude: 17.2012, longitude: -87.5512, sourceReference: "https://belizeaudubon.org/protected-areas/half-moon-caye-natural-monument/" }),
  pt({ name: "Hopkins Garifuna Beach Village", pointType: "Beach / Waterfront", city: "Hopkins", submarket: "Other", latitude: 16.8567, longitude: -88.2912, sourceReference: "https://www.travelbelize.org/" }),
];

export function getBelizeCandidates() {
  return BELIZE_CANDIDATES;
}
