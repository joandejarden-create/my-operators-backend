/**
 * Saint Kitts and Nevis countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applySaintKittsAndNevisGovernanceDefaults,
  SAINT_KITTS_AND_NEVIS_SUBMARKETS,
} from "./saint-kitts-and-nevis-demand-anchor-governance.js";

const COUNTRY = "Saint Kitts and Nevis";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applySaintKittsAndNevisGovernanceDefaults);

export const SAINT_KITTS_AND_NEVIS_COUNTRYWIDE_CANDIDATES = [
  pt({ name: "Robert L. Bradshaw International Airport Corridor", pointType: "Future Growth Node", city: "Basseterre", submarket: "Basseterre", latitude: 17.3112, longitude: -62.7189, sourceReference: "https://www.stkittstourism.kn/", manuallyVerified: true }),
  pt({ name: "Port Zante Cruise Terminal", pointType: "Mixed-Use Development", city: "Basseterre", submarket: "Basseterre", latitude: 17.2967, longitude: -62.7234, sourceReference: "https://www.stkittstourism.kn/", manuallyVerified: true }),
  pt({ name: "Basseterre Central Business District", pointType: "Business District", city: "Basseterre", submarket: "Basseterre", latitude: 17.2956, longitude: -62.7267, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Independence Square Basseterre", pointType: "Government / Civic Center", city: "Basseterre", submarket: "Basseterre", latitude: 17.2945, longitude: -62.7289, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "St. Kitts National Museum", pointType: "Tourist Attraction", city: "Basseterre", submarket: "Basseterre", latitude: 17.2934, longitude: -62.7278, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Joseph N. France General Hospital", pointType: "Medical Campus", city: "Basseterre", submarket: "Basseterre", latitude: 17.2912, longitude: -62.7312, sourceReference: "https://www.gov.kn/" }),
  pt({ name: "Clarence Fitzroy Bryant College", pointType: "University / College", city: "Basseterre", submarket: "Basseterre", latitude: 17.2889, longitude: -62.7234, sourceReference: "https://www.gov.kn/" }),
  pt({ name: "Warner Park Cricket Stadium", pointType: "Sports Venue", city: "Basseterre", submarket: "Basseterre", latitude: 17.3012, longitude: -62.7189, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Brimstone Hill Fortress UNESCO", pointType: "Tourist Attraction", city: "Sandy Point", submarket: "Other", latitude: 17.3478, longitude: -62.8345, sourceReference: "https://www.stkittstourism.kn/", manuallyVerified: true }),
  pt({ name: "Frigate Bay Resort Beach Strip", pointType: "Beach / Waterfront", city: "Frigate Bay", submarket: "Frigate Bay", latitude: 17.2789, longitude: -62.6789, sourceReference: "https://www.stkittstourism.kn/", manuallyVerified: true }),
  pt({ name: "South Frigate Bay Beach", pointType: "Beach / Waterfront", city: "Frigate Bay", submarket: "Frigate Bay", latitude: 17.2767, longitude: -62.6767, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "North Frigate Bay Beach", pointType: "Beach / Waterfront", city: "Frigate Bay", submarket: "Frigate Bay", latitude: 17.2812, longitude: -62.6812, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "St. Kitts Marriott Resort Zone", pointType: "Mixed-Use Development", city: "Frigate Bay", submarket: "Frigate Bay", latitude: 17.2801, longitude: -62.6798, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Christophe Harbour Marina", pointType: "Beach / Waterfront", city: "Basseterre", submarket: "Frigate Bay", latitude: 17.2567, longitude: -62.6234, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Sandy Point National Marine Park", pointType: "Tourist Attraction", city: "Sandy Point", submarket: "Other", latitude: 17.3567, longitude: -62.8512, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Black Rocks Formation", pointType: "Tourist Attraction", city: "Saddlers", submarket: "Other", latitude: 17.3678, longitude: -62.7789, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Romney Manor and Caribelle Batik", pointType: "Tourist Attraction", city: "Old Road", submarket: "Other", latitude: 17.3234, longitude: -62.8012, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "St. Kitts Scenic Railway", pointType: "Tourist Attraction", city: "Basseterre", submarket: "Basseterre", latitude: 17.2989, longitude: -62.7212, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Fairview Great House and Botanical Garden", pointType: "Tourist Attraction", city: "Basseterre", submarket: "Other", latitude: 17.3123, longitude: -62.7456, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Nevis Tourism Gateway Charlestown", pointType: "Future Growth Node", city: "Charlestown", submarket: "Nevis", latitude: 17.1456, longitude: -62.6234, sourceReference: "https://www.stkittstourism.kn/", manuallyVerified: true }),
  pt({ name: "Vance W. Amory International Airport Nevis", pointType: "Future Growth Node", city: "Newcastle", submarket: "Nevis", latitude: 17.2012, longitude: -62.5934, sourceReference: "https://www.stkittstourism.kn/", manuallyVerified: true }),
  pt({ name: "Pinney's Beach Nevis", pointType: "Beach / Waterfront", city: "Charlestown", submarket: "Nevis", latitude: 17.1512, longitude: -62.6189, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Four Seasons Resort Nevis", pointType: "Mixed-Use Development", city: "Charlestown", submarket: "Nevis", latitude: 17.1678, longitude: -62.6012, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Nevis Peak Trailhead", pointType: "Tourist Attraction", city: "Gingerland", submarket: "Nevis", latitude: 17.1567, longitude: -62.5789, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Bath Spring Historic Site Nevis", pointType: "Tourist Attraction", city: "Charlestown", submarket: "Nevis", latitude: 17.1423, longitude: -62.6267, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Alexander Hamilton Museum Nevis", pointType: "Tourist Attraction", city: "Charlestown", submarket: "Nevis", latitude: 17.1434, longitude: -62.6256, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Nevis Botanical Gardens", pointType: "Tourist Attraction", city: "Charlestown", submarket: "Nevis", latitude: 17.1612, longitude: -62.6123, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Oualie Beach Resort Nevis", pointType: "Beach / Waterfront", city: "Oualie", submarket: "Nevis", latitude: 17.1789, longitude: -62.5934, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "St. Kitts Ferry Terminal", pointType: "Future Growth Node", city: "Basseterre", submarket: "Basseterre", latitude: 17.2978, longitude: -62.7245, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Carambola Beach Club", pointType: "Beach / Waterfront", city: "St. Paul's", submarket: "Other", latitude: 17.3456, longitude: -62.8123, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Turtle Beach St. Kitts", pointType: "Beach / Waterfront", city: "Southeast Peninsula", submarket: "Frigate Bay", latitude: 17.2512, longitude: -62.6567, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "South East Peninsula Growth Corridor", pointType: "Future Growth Node", city: "Southeast Peninsula", submarket: "Frigate Bay", latitude: 17.2456, longitude: -62.6489, sourceReference: "https://www.investstkitts.kn/" }),
  pt({ name: "St. Kitts Economic Citizenship Investment Zone", pointType: "Government / Civic Center", city: "Basseterre", submarket: "Basseterre", latitude: 17.2923, longitude: -62.7256, sourceReference: "https://www.investstkitts.kn/" }),
  pt({ name: "Kennedy Avenue Entertainment District", pointType: "Entertainment District", city: "Basseterre", submarket: "Basseterre", latitude: 17.2941, longitude: -62.7298, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Dieppe Bay Fishing Village", pointType: "Tourist Attraction", city: "Dieppe Bay", submarket: "Other", latitude: 17.4123, longitude: -62.8234, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "St. Kitts Industrial Site", pointType: "Industrial / Logistics Zone", city: "Basseterre", submarket: "Basseterre", latitude: 17.2867, longitude: -62.7345, sourceReference: "https://www.gov.kn/" }),
  pt({ name: "St. Kitts Convention Centre", pointType: "Convention Center", city: "Frigate Bay", submarket: "Frigate Bay", latitude: 17.2798, longitude: -62.6778, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Mount Liamuiga Volcano Trail", pointType: "Tourist Attraction", city: "St. Paul's", submarket: "Other", latitude: 17.3789, longitude: -62.8012, sourceReference: "https://www.stkittstourism.kn/" }),
  pt({ name: "Nevis Heritage Trail Charlestown", pointType: "Tourist Attraction", city: "Charlestown", submarket: "Nevis", latitude: 17.1445, longitude: -62.6245, sourceReference: "https://www.stkittstourism.kn/" }),
];

export function getSaintKittsAndNevisCandidates() {
  return SAINT_KITTS_AND_NEVIS_COUNTRYWIDE_CANDIDATES;
}

export { SAINT_KITTS_AND_NEVIS_SUBMARKETS };
