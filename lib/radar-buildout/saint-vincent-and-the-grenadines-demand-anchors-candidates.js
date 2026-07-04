/**
 * Saint Vincent and the Grenadines countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applySaintVincentAndTheGrenadinesGovernanceDefaults,
  SAINT_VINCENT_AND_THE_GRENADINES_SUBMARKETS,
} from "./saint-vincent-and-the-grenadines-demand-anchor-governance.js";

const COUNTRY = "Saint Vincent and the Grenadines";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applySaintVincentAndTheGrenadinesGovernanceDefaults);

export const SAINT_VINCENT_AND_THE_GRENADINES_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Argyle International Airport Corridor", pointType: "Future Growth Node", city: "Argyle", submarket: "Other", latitude: 13.1567, longitude: -61.1512, sourceReference: "https://www.svgairport.com/", manuallyVerified: true }),
  pt({ name: "Kingstown Cruise Ship Berth", pointType: "Mixed-Use Development", city: "Kingstown", submarket: "Kingstown", latitude: 13.1567, longitude: -61.2234, sourceReference: "https://www.discoversvg.com/", manuallyVerified: true }),
  pt({ name: "Kingstown Central Business District", pointType: "Business District", city: "Kingstown", submarket: "Kingstown", latitude: 13.1589, longitude: -61.2267, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Botanic Gardens St. Vincent", pointType: "Tourist Attraction", city: "Kingstown", submarket: "Kingstown", latitude: 13.1612, longitude: -61.2289, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Fort Charlotte Historic Site", pointType: "Tourist Attraction", city: "Kingstown", submarket: "Kingstown", latitude: 13.1623, longitude: -61.2312, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Milton Cato Memorial Hospital", pointType: "Medical Campus", city: "Kingstown", submarket: "Kingstown", latitude: 13.1545, longitude: -61.2189, sourceReference: "https://www.gov.vc/" }),
  pt({ name: "St. Vincent and the Grenadines Community College", pointType: "University / College", city: "Kingstown", submarket: "Kingstown", latitude: 13.1523, longitude: -61.2212, sourceReference: "https://www.gov.vc/" }),
  pt({ name: "Arnos Vale Sports Complex", pointType: "Sports Venue", city: "Kingstown", submarket: "Kingstown", latitude: 13.1489, longitude: -61.2156, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Villa Beach Resort Strip", pointType: "Beach / Waterfront", city: "Villa", submarket: "Kingstown", latitude: 13.1456, longitude: -61.2012, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Indian Bay Beach", pointType: "Beach / Waterfront", city: "Kingstown", submarket: "Kingstown", latitude: 13.1423, longitude: -61.1989, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Young Island Resort", pointType: "Mixed-Use Development", city: "Kingstown", submarket: "Kingstown", latitude: 13.1389, longitude: -61.2123, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "La Soufrière Volcano National Park", pointType: "Tourist Attraction", city: "Richmond", submarket: "North Coast", latitude: 13.3345, longitude: -61.1789, sourceReference: "https://www.discoversvg.com/", manuallyVerified: true }),
  pt({ name: "Owia Salt Pond", pointType: "Tourist Attraction", city: "Owia", submarket: "North Coast", latitude: 13.3678, longitude: -61.1234, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Dark View Falls", pointType: "Tourist Attraction", city: "Richmond", submarket: "North Coast", latitude: 13.3123, longitude: -61.1567, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Falls of Baleine", pointType: "Tourist Attraction", city: "Richmond", submarket: "North Coast", latitude: 13.3789, longitude: -61.1456, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Bequia Tourism Gateway", pointType: "Future Growth Node", city: "Port Elizabeth", submarket: "Grenadines", latitude: 13.0123, longitude: -61.2456, sourceReference: "https://www.discoversvg.com/", manuallyVerified: true }),
  pt({ name: "Bequia Admiralty Bay Marina", pointType: "Beach / Waterfront", city: "Port Elizabeth", submarket: "Grenadines", latitude: 13.0089, longitude: -61.2412, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Princess Margaret Beach Bequia", pointType: "Beach / Waterfront", city: "Port Elizabeth", submarket: "Grenadines", latitude: 13.0067, longitude: -61.2389, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Mustique Island Resort Zone", pointType: "Mixed-Use Development", city: "Mustique", submarket: "Grenadines", latitude: 12.8789, longitude: -61.1789, sourceReference: "https://www.discoversvg.com/", manuallyVerified: true }),
  pt({ name: "Canouan Island Resort Marina", pointType: "Beach / Waterfront", city: "Canouan", submarket: "Grenadines", latitude: 12.7012, longitude: -61.3234, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Union Island Clifton Harbour", pointType: "Beach / Waterfront", city: "Clifton", submarket: "Grenadines", latitude: 12.5989, longitude: -61.4234, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Palm Island Resort", pointType: "Mixed-Use Development", city: "Palm Island", submarket: "Grenadines", latitude: 12.5789, longitude: -61.3912, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Mayreau Salt Whistle Bay", pointType: "Beach / Waterfront", city: "Mayreau", submarket: "Grenadines", latitude: 12.6456, longitude: -61.3912, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Tobago Cays Marine Park", pointType: "Tourist Attraction", city: "Tobago Cays", submarket: "Grenadines", latitude: 12.6234, longitude: -61.3567, sourceReference: "https://www.discoversvg.com/", manuallyVerified: true }),
  pt({ name: "Petit St. Vincent Private Island Resort", pointType: "Mixed-Use Development", city: "Petit St. Vincent", submarket: "Grenadines", latitude: 12.5234, longitude: -61.3789, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Chateaubelair Fishing Village", pointType: "Tourist Attraction", city: "Chateaubelair", submarket: "North Coast", latitude: 13.2912, longitude: -61.2456, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Barrouallie Fishing Village", pointType: "Beach / Waterfront", city: "Barrouallie", submarket: "Other", latitude: 13.2234, longitude: -61.2678, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Layou Petroglyph Park", pointType: "Tourist Attraction", city: "Layou", submarket: "Other", latitude: 13.2123, longitude: -61.2567, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Buccament Bay Resort Corridor", pointType: "Beach / Waterfront", city: "Buccament", submarket: "Other", latitude: 13.1789, longitude: -61.2678, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Georgetown Market District", pointType: "Entertainment District", city: "Georgetown", submarket: "Other", latitude: 13.2789, longitude: -61.1234, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Mesopotamia Valley Agriculture Tourism", pointType: "Tourist Attraction", city: "Mesopotamia", submarket: "Other", latitude: 13.2012, longitude: -61.2123, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Invest SVG Business Precinct", pointType: "Government / Civic Center", city: "Kingstown", submarket: "Kingstown", latitude: 13.1578, longitude: -61.2245, sourceReference: "https://www.investsvg.com/" }),
  pt({ name: "Kingstown Market Vendors District", pointType: "Entertainment District", city: "Kingstown", submarket: "Kingstown", latitude: 13.1598, longitude: -61.2278, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Blue Lagoon Marina St. Vincent", pointType: "Beach / Waterfront", city: "Kingstown", submarket: "Kingstown", latitude: 13.1345, longitude: -61.1934, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Grenadines Yacht Charter Hub", pointType: "Future Growth Node", city: "Port Elizabeth", submarket: "Grenadines", latitude: 13.0112, longitude: -61.2434, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "St. Vincent Conference Centre", pointType: "Convention Center", city: "Kingstown", submarket: "Kingstown", latitude: 13.1556, longitude: -61.2201, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Calliaqua Industrial Zone", pointType: "Industrial / Logistics Zone", city: "Calliaqua", submarket: "Kingstown", latitude: 13.1289, longitude: -61.1912, sourceReference: "https://www.gov.vc/" }),
  pt({ name: "Wallilabou Bay Pirates of the Caribbean Site", pointType: "Tourist Attraction", city: "Wallilabou", submarket: "North Coast", latitude: 13.2567, longitude: -61.2567, sourceReference: "https://www.discoversvg.com/" }),
  pt({ name: "Argyle South Coast Growth Node", pointType: "Future Growth Node", city: "Argyle", submarket: "Other", latitude: 13.1512, longitude: -61.1489, sourceReference: "https://www.investsvg.com/" }),
];

export function getSaintVincentAndTheGrenadinesCandidates() {
  return SAINT_VINCENT_AND_THE_GRENADINES_COUNTRYWIDE_CANDIDATES;
}

export { SAINT_VINCENT_AND_THE_GRENADINES_SUBMARKETS };
