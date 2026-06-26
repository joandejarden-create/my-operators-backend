/**
 * U.S. Virgin Islands Countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyUsVirginIslandsGovernanceDefaults,
  US_VIRGIN_ISLANDS_SUBMARKETS,
} from "./us-virgin-islands-demand-anchor-governance.js";

const COUNTRY = "U.S. Virgin Islands";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyUsVirginIslandsGovernanceDefaults);

export const US_VIRGIN_ISLANDS_CANDIDATES = [
  pt({ name: "Cyril E. King International Airport Corridor", pointType: "Future Growth Node", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3373, longitude: -64.9734, sourceReference: "https://www.viport.com/", manuallyVerified: true }),
  pt({ name: "Charlotte Amalie Cruise Port — Havensight", pointType: "Mixed-Use Development", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3312, longitude: -64.9289, sourceReference: "https://www.visitusvi.com/", manuallyVerified: true }),
  pt({ name: "Charlotte Amalie Historic District", pointType: "Tourist Attraction", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3412, longitude: -64.9312, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Main Street Duty-Free Shopping Corridor", pointType: "Entertainment District", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3423, longitude: -64.9334, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Magens Bay Beach", pointType: "Beach / Waterfront", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3689, longitude: -64.9234, sourceReference: "https://www.visitusvi.com/", manuallyVerified: true }),
  pt({ name: "Red Hook Ferry Terminal and Marina", pointType: "Beach / Waterfront", city: "Red Hook", submarket: "St. Thomas", latitude: 18.3189, longitude: -64.8512, sourceReference: "https://www.visitusvi.com/", manuallyVerified: true }),
  pt({ name: "Sapphire Beach Resort Corridor", pointType: "Beach / Waterfront", city: "Red Hook", submarket: "St. Thomas", latitude: 18.3234, longitude: -64.8567, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Coki Beach and Coral World", pointType: "Tourist Attraction", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3512, longitude: -64.8678, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "University of the Virgin Islands — St. Thomas", pointType: "University / College", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3389, longitude: -64.9812, sourceReference: "https://www.uvi.edu/" }),
  pt({ name: "Schneider Regional Medical Center", pointType: "Medical Campus", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3456, longitude: -64.9456, sourceReference: "https://www.srmedicalcenter.org/" }),
  pt({ name: "Emancipation Garden Civic Precinct", pointType: "Government / Civic Center", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3401, longitude: -64.9345, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Yacht Haven Grande Marina", pointType: "Mixed-Use Development", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3356, longitude: -64.9267, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Drake's Seat Scenic Overlook", pointType: "Tourist Attraction", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3612, longitude: -64.9123, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Henry E. Rohlsen International Airport Corridor", pointType: "Future Growth Node", city: "St. Croix", submarket: "St. Croix", latitude: 17.7019, longitude: -64.7986, sourceReference: "https://www.viport.com/", manuallyVerified: true }),
  pt({ name: "Christiansted Historic Waterfront", pointType: "Tourist Attraction", city: "Christiansted", submarket: "St. Croix", latitude: 17.7467, longitude: -64.7034, sourceReference: "https://www.visitusvi.com/", manuallyVerified: true }),
  pt({ name: "Fort Christiansvaern Historic Site", pointType: "Tourist Attraction", city: "Christiansted", submarket: "St. Croix", latitude: 17.7478, longitude: -64.7012, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Frederiksted Cruise Pier", pointType: "Mixed-Use Development", city: "Frederiksted", submarket: "St. Croix", latitude: 17.7123, longitude: -64.8834, sourceReference: "https://www.visitusvi.com/", manuallyVerified: true }),
  pt({ name: "Buck Island Reef National Monument", pointType: "Tourist Attraction", city: "St. Croix", submarket: "St. Croix", latitude: 17.7867, longitude: -64.6234, sourceReference: "https://www.nps.gov/buis/" }),
  pt({ name: "Sandy Point National Wildlife Refuge", pointType: "Tourist Attraction", city: "St. Croix", submarket: "St. Croix", latitude: 17.6734, longitude: -64.8912, sourceReference: "https://www.fws.gov/refuge/sandy-point" }),
  pt({ name: "University of the Virgin Islands — St. Croix", pointType: "University / College", city: "Kingshill", submarket: "St. Croix", latitude: 17.7234, longitude: -64.7512, sourceReference: "https://www.uvi.edu/" }),
  pt({ name: "Juan F. Luis Hospital", pointType: "Medical Campus", city: "St. Croix", submarket: "St. Croix", latitude: 17.7312, longitude: -64.7589, sourceReference: "https://www.jflusvi.org/" }),
  pt({ name: "Hovensa Industrial Heritage Zone", pointType: "Industrial / Logistics Zone", city: "St. Croix", submarket: "St. Croix", latitude: 17.6989, longitude: -64.7612, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Point Udall — Easternmost U.S. Point", pointType: "Tourist Attraction", city: "St. Croix", submarket: "St. Croix", latitude: 17.7567, longitude: -64.5667, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Cruz Bay Ferry Terminal", pointType: "Mixed-Use Development", city: "Cruz Bay", submarket: "St. John", latitude: 18.3312, longitude: -64.7934, sourceReference: "https://www.visitusvi.com/", manuallyVerified: true }),
  pt({ name: "Virgin Islands National Park — Trunk Bay", pointType: "Tourist Attraction", city: "St. John", submarket: "St. John", latitude: 18.3489, longitude: -64.7678, sourceReference: "https://www.nps.gov/viis/", manuallyVerified: true }),
  pt({ name: "Cinnamon Bay Beach Campground", pointType: "Beach / Waterfront", city: "St. John", submarket: "St. John", latitude: 18.3567, longitude: -64.7512, sourceReference: "https://www.nps.gov/viis/" }),
  pt({ name: "Maho Bay Beach", pointType: "Beach / Waterfront", city: "St. John", submarket: "St. John", latitude: 18.3612, longitude: -64.7456, sourceReference: "https://www.nps.gov/viis/" }),
  pt({ name: "Coral Bay Village", pointType: "Tourist Attraction", city: "Coral Bay", submarket: "St. John", latitude: 18.3234, longitude: -64.7234, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Estate Whim Plantation Museum", pointType: "Tourist Attraction", city: "St. Croix", submarket: "St. Croix", latitude: 17.7012, longitude: -64.8234, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Cane Bay Beach Dive Corridor", pointType: "Beach / Waterfront", city: "St. Croix", submarket: "St. Croix", latitude: 17.7634, longitude: -64.7912, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Water Island Ferry Access", pointType: "Future Growth Node", city: "Water Island", submarket: "Other", latitude: 18.3189, longitude: -64.9567, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "St. Thomas Skyride to Paradise Point", pointType: "Tourist Attraction", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3389, longitude: -64.9389, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Bolongo Bay Beach Resort Area", pointType: "Beach / Waterfront", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3012, longitude: -64.8912, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Frenchtown Dining and Marina District", pointType: "Entertainment District", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3334, longitude: -64.9412, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Renaissance St. Croix Convention Zone", pointType: "Convention Center", city: "Christiansted", submarket: "St. Croix", latitude: 17.7512, longitude: -64.7089, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Tutu Park Mall Commercial Hub", pointType: "Business District", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3489, longitude: -64.9612, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Hull Bay Surf and Fishing Village", pointType: "Beach / Waterfront", city: "St. Thomas", submarket: "St. Thomas", latitude: 18.3712, longitude: -64.9567, sourceReference: "https://www.visitusvi.com/" }),
  pt({ name: "Salt River Bay National Historical Park", pointType: "Tourist Attraction", city: "St. Croix", submarket: "St. Croix", latitude: 17.7789, longitude: -64.7612, sourceReference: "https://www.nps.gov/sari/" }),
  pt({ name: "Annaberg Plantation Ruins", pointType: "Tourist Attraction", city: "St. John", submarket: "St. John", latitude: 18.3612, longitude: -64.7289, sourceReference: "https://www.nps.gov/viis/" }),
  pt({ name: "St. Thomas Legislature and Government District", pointType: "Government / Civic Center", city: "Charlotte Amalie", submarket: "St. Thomas", latitude: 18.3434, longitude: -64.9289, sourceReference: "https://www.vilegis.gov/" }),
  pt({ name: "St. Croix Renaissance Park Growth Node", pointType: "Future Growth Node", city: "St. Croix", submarket: "St. Croix", latitude: 17.7189, longitude: -64.7489, sourceReference: "https://www.visitusvi.com/" }),
];

export function getUsVirginIslandsCandidates() {
  return US_VIRGIN_ISLANDS_CANDIDATES;
}

export { US_VIRGIN_ISLANDS_SUBMARKETS };
