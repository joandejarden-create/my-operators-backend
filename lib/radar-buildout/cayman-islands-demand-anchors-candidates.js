/**
 * Cayman Islands countrywide demand anchor candidates (source-backed).
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import {
  applyCaymanIslandsGovernanceDefaults,
  CAYMAN_ISLANDS_SUBMARKETS,
} from "./cayman-islands-demand-anchor-governance.js";

const COUNTRY = "Cayman Islands";
const REGION = "Caribbean";

function pt(v) {
  const defaults = getPointTypeDefaults(v.pointType);
  const rationale =
    v.hotelDemandNote ||
    defaults.hotelDemandRationale ||
    "Supports identifiable hotel demand in this Cayman corridor.";
  const base = {
    name: v.name,
    pointType: v.pointType,
    city: v.city,
    country: COUNTRY,
    region: REGION,
    submarket: v.submarket,
    latitude: v.latitude,
    longitude: v.longitude,
    source: "Public Source",
    sourceReference: v.sourceReference,
    dataConfidence: v.dataConfidence || "Medium",
    notes:
      v.notes ||
      `Submarket: ${v.submarket}. ${rationale} Candidate pending Google pre-import verification.`,
  };
  if (v.googleSearchQuery) base.googleSearchQuery = v.googleSearchQuery;
  if (v.manuallyVerified) {
    base.notes = `${base.notes} Manually verified using official/public source; Google Maps match was not used as final authority.`;
    base.dataConfidence = v.dataConfidence || "High";
    base.manuallyVerified = true;
  }
  return applyCaymanIslandsGovernanceDefaults(base, v.governance || {});
}

export const CAYMAN_ISLANDS_COUNTRYWIDE_CANDIDATES = [
  // Grand Cayman (16)
  pt({ name: "Owen Roberts International Airport Corridor", pointType: "Future Growth Node", city: "George Town", submarket: "Grand Cayman", latitude: 19.2928, longitude: -81.3577, sourceReference: "https://ciaa.ky/", manuallyVerified: true }),
  pt({ name: "George Town Cruise Port Waterfront", pointType: "Mixed-Use Development", city: "George Town", submarket: "Grand Cayman", latitude: 19.2866, longitude: -81.3744, sourceReference: "https://www.caymanport.com/" }),
  pt({ name: "Seven Mile Beach Resort Corridor", pointType: "Beach / Waterfront", city: "Seven Mile Beach", submarket: "Grand Cayman", latitude: 19.3296, longitude: -81.3848, sourceReference: "https://www.visitcaymanislands.com/en-us/things-to-do/beaches/seven-mile-beach", manuallyVerified: true }),
  pt({ name: "Camana Bay Mixed-Use District", pointType: "Mixed-Use Development", city: "George Town", submarket: "Grand Cayman", latitude: 19.3239, longitude: -81.3798, sourceReference: "https://www.camanabay.com/" }),
  pt({ name: "Cayman Islands National Museum", pointType: "Tourist Attraction", city: "George Town", submarket: "Grand Cayman", latitude: 19.2873, longitude: -81.3743, sourceReference: "https://www.museum.ky/" }),
  pt({ name: "Cayman Crystal Caves", pointType: "Tourist Attraction", city: "North Side", submarket: "Grand Cayman", latitude: 19.3537, longitude: -81.1978, sourceReference: "https://caymancrystalcaves.com/" }),
  pt({ name: "Rum Point Beach", pointType: "Beach / Waterfront", city: "North Side", submarket: "Grand Cayman", latitude: 19.3727, longitude: -81.2705, sourceReference: "https://www.visitcaymanislands.com/" }),
  pt({ name: "Starfish Point", pointType: "Tourist Attraction", city: "North Side", submarket: "Grand Cayman", latitude: 19.3822, longitude: -81.2793, sourceReference: "https://www.visitcaymanislands.com/" }),
  pt({ name: "Bodden Town Heritage Waterfront", pointType: "Beach / Waterfront", city: "Bodden Town", submarket: "Grand Cayman", latitude: 19.2818, longitude: -81.2539, sourceReference: "https://www.visitcaymanislands.com/" }),
  pt({ name: "East End Dive Resort Corridor", pointType: "Beach / Waterfront", city: "East End", submarket: "Grand Cayman", latitude: 19.3001, longitude: -81.0937, sourceReference: "https://www.visitcaymanislands.com/", manuallyVerified: true }),
  pt({ name: "Health City Cayman Islands", pointType: "Medical Campus", city: "East End", submarket: "Grand Cayman", latitude: 19.3118, longitude: -81.1075, sourceReference: "https://www.healthcitycaymanislands.com/" }),
  pt({ name: "George Town Financial District", pointType: "Business District", city: "George Town", submarket: "Grand Cayman", latitude: 19.2869, longitude: -81.3676, sourceReference: "https://www.cima.ky/", manuallyVerified: true }),
  pt({ name: "Cayman Islands Parliament Precinct", pointType: "Government / Civic Center", city: "George Town", submarket: "Grand Cayman", latitude: 19.2868, longitude: -81.3671, sourceReference: "https://www.gov.ky/" }),
  pt({ name: "Cayman Islands Hospital", pointType: "Medical Campus", city: "George Town", submarket: "Grand Cayman", latitude: 19.2966, longitude: -81.3695, sourceReference: "https://www.hsa.ky/" }),
  pt({ name: "Cayman Islands Further Education Centre", pointType: "University / College", city: "George Town", submarket: "Grand Cayman", latitude: 19.2932, longitude: -81.3659, sourceReference: "https://schools.edu.ky/" }),
  pt({ name: "West Bay Growth Corridor", pointType: "Future Growth Node", city: "West Bay", submarket: "Grand Cayman", latitude: 19.3764, longitude: -81.4209, sourceReference: "https://www.planning.gov.ky/", manuallyVerified: true }),

  // Cayman Brac (6)
  pt({ name: "Charles Kirkconnell International Airport", pointType: "Future Growth Node", city: "Cayman Brac", submarket: "Cayman Brac", latitude: 19.687, longitude: -79.8828, sourceReference: "https://ciaa.ky/", manuallyVerified: true }),
  pt({ name: "Stake Bay Waterfront", pointType: "Beach / Waterfront", city: "Stake Bay", submarket: "Cayman Brac", latitude: 19.7041, longitude: -79.8462, sourceReference: "https://www.visitcaymanislands.com/" }),
  pt({ name: "Cayman Brac Bluff Trail Zone", pointType: "Tourist Attraction", city: "Cayman Brac", submarket: "Cayman Brac", latitude: 19.7168, longitude: -79.8084, sourceReference: "https://www.nationaltrust.org.ky/" }),
  pt({ name: "Cayman Brac Reef Dive Corridor", pointType: "Beach / Waterfront", city: "Cayman Brac", submarket: "Cayman Brac", latitude: 19.7099, longitude: -79.8675, sourceReference: "https://www.visitcaymanislands.com/", manuallyVerified: true }),
  pt({ name: "Cayman Brac Sports Complex", pointType: "Sports Venue", city: "Cayman Brac", submarket: "Cayman Brac", latitude: 19.7064, longitude: -79.8493, sourceReference: "https://www.gov.ky/" }),
  pt({ name: "Cayman Brac District Admin Centre", pointType: "Government / Civic Center", city: "Cayman Brac", submarket: "Cayman Brac", latitude: 19.7057, longitude: -79.8474, sourceReference: "https://www.gov.ky/" }),

  // Little Cayman (6)
  pt({ name: "Edward Bodden Airfield", pointType: "Future Growth Node", city: "Little Cayman", submarket: "Little Cayman", latitude: 19.6599, longitude: -80.0906, sourceReference: "https://ciaa.ky/", manuallyVerified: true }),
  pt({ name: "South Hole Sound Anchorage", pointType: "Beach / Waterfront", city: "Little Cayman", submarket: "Little Cayman", latitude: 19.6762, longitude: -80.0878, sourceReference: "https://www.visitcaymanislands.com/" }),
  pt({ name: "Bloody Bay Marine Park", pointType: "Tourist Attraction", city: "Little Cayman", submarket: "Little Cayman", latitude: 19.7425, longitude: -80.1196, sourceReference: "https://doe.ky/" }),
  pt({ name: "Point of Sand Beach", pointType: "Beach / Waterfront", city: "Little Cayman", submarket: "Little Cayman", latitude: 19.7362, longitude: -80.1607, sourceReference: "https://www.visitcaymanislands.com/", manuallyVerified: true }),
  pt({ name: "Little Cayman Dive Resort Zone", pointType: "Mixed-Use Development", city: "Little Cayman", submarket: "Little Cayman", latitude: 19.6848, longitude: -80.0919, sourceReference: "https://www.visitcaymanislands.com/" }),
  pt({ name: "Little Cayman Eco-Tourism Growth Node", pointType: "Future Growth Node", city: "Little Cayman", submarket: "Little Cayman", latitude: 19.7014, longitude: -80.1048, sourceReference: "https://www.planning.gov.ky/", manuallyVerified: true }),

  // Other (4)
  pt({ name: "National Gallery of the Cayman Islands", pointType: "Tourist Attraction", city: "George Town", submarket: "Other", latitude: 19.3174, longitude: -81.3805, sourceReference: "https://www.nationalgallery.org.ky/" }),
  pt({ name: "Pedro St. James Castle", pointType: "Tourist Attraction", city: "Savannah", submarket: "Other", latitude: 19.2842, longitude: -81.2105, sourceReference: "https://pedrostjames.ky/" }),
  pt({ name: "Queen Elizabeth II Botanic Park", pointType: "Tourist Attraction", city: "North Side", submarket: "Other", latitude: 19.3432, longitude: -81.1851, sourceReference: "https://www.botanic-park.ky/" }),
  pt({ name: "Cayman Enterprise City Tech Corridor", pointType: "Business District", city: "George Town", submarket: "Other", latitude: 19.2938, longitude: -81.3561, sourceReference: "https://www.caymanenterprisecity.com/" }),
];

export function getCaymanIslandsCandidates() {
  return CAYMAN_ISLANDS_COUNTRYWIDE_CANDIDATES;
}

export { CAYMAN_ISLANDS_SUBMARKETS };
