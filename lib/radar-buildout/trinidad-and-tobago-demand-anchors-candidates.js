/**
 * Trinidad and Tobago countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyTrinidadAndTobagoGovernanceDefaults,
  TRINIDAD_AND_TOBAGO_SUBMARKETS,
} from "./trinidad-and-tobago-demand-anchor-governance.js";

const COUNTRY = "Trinidad and Tobago";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyTrinidadAndTobagoGovernanceDefaults);

export const TRINIDAD_AND_TOBAGO_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Piarco International Airport Corridor", pointType: "Future Growth Node", city: "Piarco", submarket: "East-West Corridor", latitude: 10.5954, longitude: -61.3372, sourceReference: "https://www.ttairport.com/", manuallyVerified: true }),
  pt({ name: "Port of Spain Central Business District", pointType: "Business District", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6549, longitude: -61.5089, sourceReference: "https://www.visittrinidad.tt/", manuallyVerified: true }),
  pt({ name: "Port of Spain Waterfront District", pointType: "Beach / Waterfront", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6512, longitude: -61.5123, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Queen's Park Savannah", pointType: "Tourist Attraction", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6678, longitude: -61.5189, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "National Academy for the Performing Arts", pointType: "Entertainment District", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6534, longitude: -61.5156, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Trinidad Carnival Grand Stand District", pointType: "Entertainment District", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6689, longitude: -61.5234, sourceReference: "https://www.visittrinidad.tt/", manuallyVerified: true }),
  pt({ name: "Hyatt Regency Port of Spain Precinct", pointType: "Mixed-Use Development", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6523, longitude: -61.5101, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Eric Williams Medical Sciences Complex", pointType: "Medical Campus", city: "Mount Hope", submarket: "East-West Corridor", latitude: 10.6412, longitude: -61.4012, sourceReference: "https://www.gov.tt/" }),
  pt({ name: "University of the West Indies St. Augustine", pointType: "University / College", city: "St. Augustine", submarket: "East-West Corridor", latitude: 10.6412, longitude: -61.3989, sourceReference: "https://www.uwi.edu/", manuallyVerified: true }),
  pt({ name: "University of Trinidad and Tobago", pointType: "University / College", city: "Point Lisas", submarket: "South Trinidad", latitude: 10.4123, longitude: -61.4567, sourceReference: "https://www.utt.edu.tt/" }),
  pt({ name: "Hasely Crawford Stadium", pointType: "Sports Venue", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6612, longitude: -61.5312, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "International Financial Centre", pointType: "Business District", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6567, longitude: -61.5067, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Maracas Bay Beach", pointType: "Beach / Waterfront", city: "Maracas", submarket: "Other", latitude: 10.7623, longitude: -61.4234, sourceReference: "https://www.visittrinidad.tt/", manuallyVerified: true }),
  pt({ name: "Las Cuevas Beach", pointType: "Beach / Waterfront", city: "Las Cuevas", submarket: "Other", latitude: 10.7789, longitude: -61.4123, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Chaguaramas Boardwalk and Marina", pointType: "Beach / Waterfront", city: "Chaguaramas", submarket: "Other", latitude: 10.6789, longitude: -61.6234, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Point Lisas Industrial Estate", pointType: "Industrial / Logistics Zone", city: "Point Lisas", submarket: "South Trinidad", latitude: 10.4012, longitude: -61.4678, sourceReference: "https://www.gov.tt/" }),
  pt({ name: "San Fernando City Centre", pointType: "Business District", city: "San Fernando", submarket: "South Trinidad", latitude: 10.2789, longitude: -61.4567, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Pitch Lake La Brea", pointType: "Tourist Attraction", city: "La Brea", submarket: "South Trinidad", latitude: 10.2345, longitude: -61.6234, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Caroni Bird Sanctuary", pointType: "Tourist Attraction", city: "Caroni", submarket: "East-West Corridor", latitude: 10.6012, longitude: -61.4567, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Asa Wright Nature Centre", pointType: "Tourist Attraction", city: "Arima", submarket: "East-West Corridor", latitude: 10.7234, longitude: -61.3123, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Mount St. Benedict Monastery", pointType: "Tourist Attraction", city: "Tunapuna", submarket: "East-West Corridor", latitude: 10.6567, longitude: -61.3789, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "ANR Robinson International Airport Tobago", pointType: "Future Growth Node", city: "Crown Point", submarket: "Tobago", latitude: 11.1497, longitude: -60.8322, sourceReference: "https://www.tobagoairport.com/", manuallyVerified: true }),
  pt({ name: "Scarborough Tobago Civic Centre", pointType: "Government / Civic Center", city: "Scarborough", submarket: "Tobago", latitude: 11.1812, longitude: -60.7345, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Pigeon Point Beach Tobago", pointType: "Beach / Waterfront", city: "Crown Point", submarket: "Tobago", latitude: 11.1678, longitude: -60.8345, sourceReference: "https://www.visittobago.gov.tt/", manuallyVerified: true }),
  pt({ name: "Store Bay Tobago", pointType: "Beach / Waterfront", city: "Crown Point", submarket: "Tobago", latitude: 11.1512, longitude: -60.8389, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Buccoo Reef and Nylon Pool", pointType: "Tourist Attraction", city: "Buccoo", submarket: "Tobago", latitude: 11.1789, longitude: -60.8234, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Tobago Plantations Beach Resort Zone", pointType: "Mixed-Use Development", city: "Lowlands", submarket: "Tobago", latitude: 11.1923, longitude: -60.7789, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Main Ridge Forest Reserve Tobago", pointType: "Tourist Attraction", city: "Roxborough", submarket: "Tobago", latitude: 11.2678, longitude: -60.5789, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Argyle Waterfall Tobago", pointType: "Tourist Attraction", city: "Roxborough", submarket: "Tobago", latitude: 11.2567, longitude: -60.6012, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Fort King George Tobago", pointType: "Tourist Attraction", city: "Scarborough", submarket: "Tobago", latitude: 11.1834, longitude: -60.7312, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Port of Spain Convention Centre", pointType: "Convention Center", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6556, longitude: -61.5078, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Trinidad Hilton Conference Precinct", pointType: "Convention Center", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6623, longitude: -61.5167, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Gulf City Shopping and Entertainment", pointType: "Entertainment District", city: "San Fernando", submarket: "South Trinidad", latitude: 10.2812, longitude: -61.4512, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Trincity Industrial Estate", pointType: "Industrial / Logistics Zone", city: "Trincity", submarket: "East-West Corridor", latitude: 10.6234, longitude: -61.3456, sourceReference: "https://www.gov.tt/" }),
  pt({ name: "InvesTT Business Precinct", pointType: "Government / Civic Center", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6539, longitude: -61.5098, sourceReference: "https://www.investt.co.tt/" }),
  pt({ name: "Tobago Ferry Terminal Scarborough", pointType: "Future Growth Node", city: "Scarborough", submarket: "Tobago", latitude: 11.1789, longitude: -60.7289, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Port of Spain Cruise Ship Complex", pointType: "Mixed-Use Development", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6498, longitude: -61.5134, sourceReference: "https://www.visittrinidad.tt/" }),
  pt({ name: "Moka Trinidad Resort Growth Node", pointType: "Future Growth Node", city: "Maraval", submarket: "Port of Spain", latitude: 10.7012, longitude: -61.5234, sourceReference: "https://www.investt.co.tt/" }),
  pt({ name: "Tobago Eco Resort Growth Corridor", pointType: "Future Growth Node", city: "Crown Point", submarket: "Tobago", latitude: 11.1612, longitude: -60.8312, sourceReference: "https://www.visittobago.gov.tt/" }),
  pt({ name: "Lady Young Road Entertainment Corridor", pointType: "Entertainment District", city: "Port of Spain", submarket: "Port of Spain", latitude: 10.6589, longitude: -61.5212, sourceReference: "https://www.visittrinidad.tt/" }),
];

export function getTrinidadAndTobagoCandidates() {
  return TRINIDAD_AND_TOBAGO_COUNTRYWIDE_CANDIDATES;
}

export { TRINIDAD_AND_TOBAGO_SUBMARKETS };
