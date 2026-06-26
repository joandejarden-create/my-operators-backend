/**
 * Bonaire Countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyBonaireGovernanceDefaults,
  BONAIRE_SUBMARKETS,
} from "./bonaire-demand-anchor-governance.js";

const COUNTRY = "Bonaire";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyBonaireGovernanceDefaults);

export const BONAIRE_CANDIDATES = [
  pt({ name: "Flamingo International Airport Corridor", pointType: "Future Growth Node", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.131, longitude: -68.2685, sourceReference: "https://bonaireinternationalairport.com/", manuallyVerified: true }),
  pt({ name: "Kralendijk Cruise Port — Harbour Village", pointType: "Mixed-Use Development", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1512, longitude: -68.2789, sourceReference: "https://www.tourismbonaire.com/", manuallyVerified: true }),
  pt({ name: "Kralendijk Waterfront and Wilhelmina Park", pointType: "Beach / Waterfront", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1534, longitude: -68.2767, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Kaya Grandi Shopping and Dining Strip", pointType: "Entertainment District", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1512, longitude: -68.2712, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Fort Oranje Historic Waterfront", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1523, longitude: -68.2734, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Bonaire National Marine Park Headquarters", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1489, longitude: -68.2812, sourceReference: "https://www.bonairenaturefoundation.org/" }),
  pt({ name: "Donkey Sanctuary Bonaire", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1712, longitude: -68.2512, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Hospital San Francisco", pointType: "Medical Campus", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1589, longitude: -68.2689, sourceReference: "https://www.bonairegov.com/" }),
  pt({ name: "Bonaire Convention and Events Center", pointType: "Convention Center", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1567, longitude: -68.2634, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Plaza Resort Bonaire", pointType: "Mixed-Use Development", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1612, longitude: -68.2912, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Delfins Beach Resort", pointType: "Beach / Waterfront", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1689, longitude: -68.3012, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Buddy Dive Resort and Pier", pointType: "Mixed-Use Development", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1634, longitude: -68.2889, sourceReference: "https://www.buddyresort.com/" }),
  pt({ name: "Klein Bonaire Uninhabited Island", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1589, longitude: -68.3012, sourceReference: "https://www.tourismbonaire.com/", manuallyVerified: true }),
  pt({ name: "Lac Bay Windsurf and Sorobon Beach", pointType: "Beach / Waterfront", city: "Sorobon", submarket: "Other", latitude: 12.1089, longitude: -68.2312, sourceReference: "https://www.tourismbonaire.com/", manuallyVerified: true }),
  pt({ name: "Cargill Salt Flats — Pink Salt Pans", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Other", latitude: 12.0789, longitude: -68.2512, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Willemstoren Lighthouse South Tip", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Other", latitude: 12.0289, longitude: -68.2512, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Rincon Heritage Village", pointType: "Tourist Attraction", city: "Rincon", submarket: "Rincon", latitude: 12.2312, longitude: -68.3312, sourceReference: "https://www.tourismbonaire.com/", manuallyVerified: true }),
  pt({ name: "Rincon Cadushy Distillery", pointType: "Tourist Attraction", city: "Rincon", submarket: "Rincon", latitude: 12.2334, longitude: -68.3289, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Rincon Valley Agricultural Heartland", pointType: "Tourist Attraction", city: "Rincon", submarket: "Rincon", latitude: 12.2412, longitude: -68.3189, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Gotomeer Flamingo Sanctuary", pointType: "Tourist Attraction", city: "Rincon", submarket: "Rincon", latitude: 12.2189, longitude: -68.3012, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Washington Slagbaai National Park Entrance", pointType: "Tourist Attraction", city: "Rincon", submarket: "Washington Slagbaai", latitude: 12.2512, longitude: -68.3512, sourceReference: "https://www.stinapa.org/", manuallyVerified: true }),
  pt({ name: "Slagbaai Bay Beach", pointType: "Beach / Waterfront", city: "Washington Slagbaai", submarket: "Washington Slagbaai", latitude: 12.2689, longitude: -68.3712, sourceReference: "https://www.stinapa.org/" }),
  pt({ name: "Boka Kokolishi Black Sand Beach", pointType: "Beach / Waterfront", city: "Washington Slagbaai", submarket: "Washington Slagbaai", latitude: 12.2812, longitude: -68.3612, sourceReference: "https://www.stinapa.org/" }),
  pt({ name: "Seru Largu Scenic Overlook", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Washington Slagbaai", latitude: 12.2012, longitude: -68.2912, sourceReference: "https://www.stinapa.org/" }),
  pt({ name: "1000 Steps Dive Site", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Other", latitude: 12.2189, longitude: -68.3112, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Karpata Dive and Snorkel Coast", pointType: "Beach / Waterfront", city: "Kralendijk", submarket: "Other", latitude: 12.2412, longitude: -68.3412, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Andrea I and II Dive Reef", pointType: "Tourist Attraction", city: "Kralendijk", submarket: "Other", latitude: 12.1712, longitude: -68.3012, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Eden Beach Resort Area", pointType: "Beach / Waterfront", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1789, longitude: -68.3089, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Sunset Beach Resort Coast", pointType: "Beach / Waterfront", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1812, longitude: -68.3112, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Captain Don's Habitat Dive Resort", pointType: "Mixed-Use Development", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1667, longitude: -68.2934, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Bonaire Free Zone Industrial Park", pointType: "Industrial / Logistics Zone", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1389, longitude: -68.2589, sourceReference: "https://www.bonairegov.com/" }),
  pt({ name: "Bonaire Economic Development Zone", pointType: "Future Growth Node", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1412, longitude: -68.2612, sourceReference: "https://www.bonairegov.com/" }),
  pt({ name: "Antriol Residential and Dive Lodge Corridor", pointType: "Mixed-Use Development", city: "Antriol", submarket: "Kralendijk", latitude: 12.1589, longitude: -68.2512, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Hato Agricultural and Cactus Country", pointType: "Tourist Attraction", city: "Hato", submarket: "Other", latitude: 12.1912, longitude: -68.2712, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Bonaire Aloe Factory", pointType: "Tourist Attraction", city: "Rincon", submarket: "Rincon", latitude: 12.2289, longitude: -68.3234, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Pos Mangel Retail and Dining Hub", pointType: "Entertainment District", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1534, longitude: -68.2689, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Bonaire Deep South Dive Coast", pointType: "Future Growth Node", city: "Kralendijk", submarket: "Other", latitude: 12.0489, longitude: -68.2612, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Tera Kora Hills Mountain Biking Zone", pointType: "Tourist Attraction", city: "Rincon", submarket: "Rincon", latitude: 12.2389, longitude: -68.3112, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Bonaire Mangrove Kayak Launch — Lac Cai", pointType: "Tourist Attraction", city: "Lac Cai", submarket: "Other", latitude: 12.1189, longitude: -68.2189, sourceReference: "https://www.tourismbonaire.com/" }),
  pt({ name: "Bonaire Cruise Tourism Growth Node", pointType: "Future Growth Node", city: "Kralendijk", submarket: "Kralendijk", latitude: 12.1501, longitude: -68.2778, sourceReference: "https://www.tourismbonaire.com/" }),
];

export function getBonaireCandidates() {
  return BONAIRE_CANDIDATES;
}

export { BONAIRE_SUBMARKETS };
