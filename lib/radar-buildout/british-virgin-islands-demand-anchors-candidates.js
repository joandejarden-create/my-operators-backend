/**
 * British Virgin Islands countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyBritishVirginIslandsGovernanceDefaults,
  BRITISH_VIRGIN_ISLANDS_SUBMARKETS,
} from "./british-virgin-islands-demand-anchor-governance.js";

const COUNTRY = "British Virgin Islands";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyBritishVirginIslandsGovernanceDefaults);

export const BRITISH_VIRGIN_ISLANDS_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Terrance B. Lettsome International Airport Corridor", pointType: "Future Growth Node", city: "Beef Island", submarket: "Tortola", latitude: 18.4448, longitude: -64.543, sourceReference: "https://www.bviaa.com/", manuallyVerified: true }),
  pt({ name: "Road Town Central Business District", pointType: "Business District", city: "Road Town", submarket: "Tortola", latitude: 18.4261, longitude: -64.6205, sourceReference: "https://www.bvitourism.com/", manuallyVerified: true }),
  pt({ name: "Road Town Ferry Terminal", pointType: "Mixed-Use Development", city: "Road Town", submarket: "Tortola", latitude: 18.4245, longitude: -64.6189, sourceReference: "https://www.bvitourism.com/", manuallyVerified: true }),
  pt({ name: "Wickham's Cay Marina District", pointType: "Beach / Waterfront", city: "Road Town", submarket: "Tortola", latitude: 18.4212, longitude: -64.6156, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Soper's Hole Marina West End", pointType: "Beach / Waterfront", city: "West End", submarket: "Tortola", latitude: 18.4123, longitude: -64.6678, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Nanny Cay Marina and Resort", pointType: "Mixed-Use Development", city: "Nanny Cay", submarket: "Tortola", latitude: 18.3789, longitude: -64.6234, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Cane Garden Bay Beach", pointType: "Beach / Waterfront", city: "Cane Garden Bay", submarket: "Tortola", latitude: 18.4345, longitude: -64.6456, sourceReference: "https://www.bvitourism.com/", manuallyVerified: true }),
  pt({ name: "Smuggler's Cove Beach", pointType: "Beach / Waterfront", city: "West End", submarket: "Tortola", latitude: 18.4567, longitude: -64.6789, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Long Bay Beach Tortola", pointType: "Beach / Waterfront", city: "East End", submarket: "Tortola", latitude: 18.4456, longitude: -64.5678, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "J.R. O'Neal Botanic Gardens", pointType: "Tourist Attraction", city: "Road Town", submarket: "Tortola", latitude: 18.4289, longitude: -64.6123, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Peebles Hospital", pointType: "Medical Campus", city: "Road Town", submarket: "Tortola", latitude: 18.4256, longitude: -64.6234, sourceReference: "https://www.bvi.gov.vg/" }),
  pt({ name: "H. Lavity Stoutt Community College", pointType: "University / College", city: "Road Town", submarket: "Tortola", latitude: 18.4312, longitude: -64.6089, sourceReference: "https://www.bvi.gov.vg/" }),
  pt({ name: "A.O. Shirley Recreation Ground", pointType: "Sports Venue", city: "Road Town", submarket: "Tortola", latitude: 18.4234, longitude: -64.6267, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Virgin Gorda Yacht Harbour", pointType: "Beach / Waterfront", city: "Spanish Town", submarket: "Virgin Gorda", latitude: 18.4512, longitude: -64.4345, sourceReference: "https://www.bvitourism.com/", manuallyVerified: true }),
  pt({ name: "The Baths National Park", pointType: "Tourist Attraction", city: "Spanish Town", submarket: "Virgin Gorda", latitude: 18.4312, longitude: -64.4456, sourceReference: "https://www.bvitourism.com/", manuallyVerified: true }),
  pt({ name: "Oil Nut Bay Resort", pointType: "Mixed-Use Development", city: "Virgin Gorda", submarket: "Virgin Gorda", latitude: 18.4789, longitude: -64.4012, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Leverick Bay Resort Marina", pointType: "Beach / Waterfront", city: "North Sound", submarket: "Virgin Gorda", latitude: 18.5012, longitude: -64.3789, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Bitter End Yacht Club", pointType: "Mixed-Use Development", city: "North Sound", submarket: "Virgin Gorda", latitude: 18.5123, longitude: -64.3678, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Virgin Gorda Airport Corridor", pointType: "Future Growth Node", city: "Spanish Town", submarket: "Virgin Gorda", latitude: 18.4464, longitude: -64.4275, sourceReference: "https://www.bviaa.com/", manuallyVerified: true }),
  pt({ name: "Jost Van Dyke Great Harbour", pointType: "Beach / Waterfront", city: "Great Harbour", submarket: "Other Islands", latitude: 18.4456, longitude: -64.7512, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "White Bay Beach Jost Van Dyke", pointType: "Beach / Waterfront", city: "White Bay", submarket: "Other Islands", latitude: 18.4389, longitude: -64.7567, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Foxy's Tamarind Bar District", pointType: "Entertainment District", city: "Great Harbour", submarket: "Other Islands", latitude: 18.4467, longitude: -64.7523, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Anegada Reef Hotel Zone", pointType: "Mixed-Use Development", city: "The Settlement", submarket: "Other Islands", latitude: 18.7234, longitude: -64.3234, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Anegada Flamingo Pond", pointType: "Tourist Attraction", city: "The Settlement", submarket: "Other Islands", latitude: 18.7123, longitude: -64.3345, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Loblolly Bay Anegada", pointType: "Beach / Waterfront", city: "The Settlement", submarket: "Other Islands", latitude: 18.7456, longitude: -64.3123, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Anegada Airport Corridor", pointType: "Future Growth Node", city: "The Settlement", submarket: "Other Islands", latitude: 18.7272, longitude: -64.3296, sourceReference: "https://www.bviaa.com/" }),
  pt({ name: "Peter Island Resort Zone", pointType: "Mixed-Use Development", city: "Peter Island", submarket: "Other Islands", latitude: 18.3512, longitude: -64.6234, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Cooper Island Beach Club", pointType: "Beach / Waterfront", city: "Cooper Island", submarket: "Other Islands", latitude: 18.3789, longitude: -64.5234, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Norman Island Pirate Bight", pointType: "Tourist Attraction", city: "Norman Island", submarket: "Other Islands", latitude: 18.3234, longitude: -64.6123, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Saba Rock Resort", pointType: "Mixed-Use Development", city: "Virgin Gorda", submarket: "Virgin Gorda", latitude: 18.5012, longitude: -64.3567, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "BVI Finance Business Precinct", pointType: "Government / Civic Center", city: "Road Town", submarket: "Tortola", latitude: 18.4278, longitude: -64.6212, sourceReference: "https://www.bvi.gov.vg/" }),
  pt({ name: "Tortola Yacht Charter Hub", pointType: "Future Growth Node", city: "Road Town", submarket: "Tortola", latitude: 18.4251, longitude: -64.6178, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "BVI Convention Centre Road Town", pointType: "Convention Center", city: "Road Town", submarket: "Tortola", latitude: 18.4267, longitude: -64.6198, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Tortola Cruise Pier Expansion Node", pointType: "Future Growth Node", city: "Road Town", submarket: "Tortola", latitude: 18.4239, longitude: -64.6167, sourceReference: "https://www.bvi.gov.vg/" }),
  pt({ name: "East End Tortola Growth Corridor", pointType: "Future Growth Node", city: "East End", submarket: "Tortola", latitude: 18.4478, longitude: -64.5612, sourceReference: "https://www.bvi.gov.vg/" }),
  pt({ name: "BVI Port Authority Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Road Town", submarket: "Tortola", latitude: 18.4223, longitude: -64.6145, sourceReference: "https://www.bviports.org/" }),
  pt({ name: "Rhone Marine Park Dive Site", pointType: "Tourist Attraction", city: "Salt Island", submarket: "Other Islands", latitude: 18.3678, longitude: -64.5345, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Sage Mountain National Park", pointType: "Tourist Attraction", city: "Road Town", submarket: "Tortola", latitude: 18.4567, longitude: -64.6012, sourceReference: "https://www.bvitourism.com/" }),
  pt({ name: "Treasure Point Resort Growth", pointType: "Mixed-Use Development", city: "East End", submarket: "Tortola", latitude: 18.4412, longitude: -64.5789, sourceReference: "https://www.bvitourism.com/" }),
];

export function getBritishVirginIslandsCandidates() {
  return BRITISH_VIRGIN_ISLANDS_COUNTRYWIDE_CANDIDATES;
}

export { BRITISH_VIRGIN_ISLANDS_SUBMARKETS };
