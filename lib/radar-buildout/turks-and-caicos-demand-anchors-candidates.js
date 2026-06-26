/**
 * Turks & Caicos countrywide demand anchor candidates (source-backed).
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import {
  applyTurksAndCaicosGovernanceDefaults,
  TURKS_AND_CAICOS_SUBMARKETS,
} from "./turks-and-caicos-demand-anchor-governance.js";

const COUNTRY = "Turks & Caicos";
const REGION = "Caribbean";

function pt(v) {
  const defaults = getPointTypeDefaults(v.pointType);
  const rationale =
    v.hotelDemandNote ||
    defaults.hotelDemandRationale ||
    "Supports identifiable hotel demand in this Turks & Caicos corridor.";
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
  return applyTurksAndCaicosGovernanceDefaults(base, v.governance || {});
}

export const TURKS_AND_CAICOS_COUNTRYWIDE_CANDIDATES = [
  // Providenciales (16)
  pt({ name: "Providenciales International Airport Corridor", pointType: "Future Growth Node", city: "Providenciales", submarket: "Providenciales", latitude: 21.7736, longitude: -72.2659, sourceReference: "https://www.tciairports.com/", manuallyVerified: true }),
  pt({ name: "Grace Bay Beach Resort Corridor", pointType: "Beach / Waterfront", city: "Grace Bay", submarket: "Providenciales", latitude: 21.7984, longitude: -72.1765, sourceReference: "https://www.visittci.com/", manuallyVerified: true }),
  pt({ name: "Leeward Marina and Yacht Basin", pointType: "Beach / Waterfront", city: "Leeward", submarket: "Providenciales", latitude: 21.8151, longitude: -72.1499, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "The Bight and Turtle Cove Marina", pointType: "Beach / Waterfront", city: "Providenciales", submarket: "Providenciales", latitude: 21.7734, longitude: -72.2461, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Long Bay Beach", pointType: "Beach / Waterfront", city: "Long Bay", submarket: "Providenciales", latitude: 21.7724, longitude: -72.1518, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Chalk Sound National Park", pointType: "Tourist Attraction", city: "Providenciales", submarket: "Providenciales", latitude: 21.7458, longitude: -72.2848, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Sapodilla Bay", pointType: "Beach / Waterfront", city: "Providenciales", submarket: "Providenciales", latitude: 21.7459, longitude: -72.2934, sourceReference: "https://www.visittci.com/", manuallyVerified: true }),
  pt({ name: "Taylor Bay", pointType: "Beach / Waterfront", city: "Providenciales", submarket: "Providenciales", latitude: 21.7471, longitude: -72.2918, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Blue Haven Marina District", pointType: "Mixed-Use Development", city: "Leeward", submarket: "Providenciales", latitude: 21.8165, longitude: -72.1507, sourceReference: "https://www.bluehaventci.com/" }),
  pt({ name: "Grace Bay Retail and Dining District", pointType: "Entertainment District", city: "Grace Bay", submarket: "Providenciales", latitude: 21.7954, longitude: -72.1762, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "South Dock Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Providenciales", submarket: "Providenciales", latitude: 21.7423, longitude: -72.2704, sourceReference: "https://www.gov.tc/" }),
  pt({ name: "Cockburn Town Road Business Corridor", pointType: "Business District", city: "Providenciales", submarket: "Providenciales", latitude: 21.7785, longitude: -72.2743, sourceReference: "https://www.investturksandcaicos.tc/" }),
  pt({ name: "Turks and Caicos Hospital Providenciales", pointType: "Medical Campus", city: "Providenciales", submarket: "Providenciales", latitude: 21.777, longitude: -72.2819, sourceReference: "https://www.interhealthcanada.tc/" }),
  pt({ name: "TCI Community College Providenciales Campus", pointType: "University / College", city: "Providenciales", submarket: "Providenciales", latitude: 21.7781, longitude: -72.2866, sourceReference: "https://www.tcicc.edu.tc/" }),
  pt({ name: "National Stadium Providenciales", pointType: "Sports Venue", city: "Providenciales", submarket: "Providenciales", latitude: 21.7796, longitude: -72.2794, sourceReference: "https://www.gov.tc/" }),
  pt({ name: "Providenciales Resort Expansion Corridor", pointType: "Future Growth Node", city: "Providenciales", submarket: "Providenciales", latitude: 21.7901, longitude: -72.2003, sourceReference: "https://www.investturksandcaicos.tc/", manuallyVerified: true }),

  // Grand Turk (8)
  pt({ name: "JAGS McCartney International Airport", pointType: "Future Growth Node", city: "Grand Turk", submarket: "Grand Turk", latitude: 21.4446, longitude: -71.1419, sourceReference: "https://www.tciairports.com/", manuallyVerified: true }),
  pt({ name: "Grand Turk Cruise Center", pointType: "Mixed-Use Development", city: "Grand Turk", submarket: "Grand Turk", latitude: 21.4329, longitude: -71.1286, sourceReference: "https://www.grandturkcc.com/" }),
  pt({ name: "Cockburn Town Heritage District", pointType: "Tourist Attraction", city: "Cockburn Town", submarket: "Grand Turk", latitude: 21.4611, longitude: -71.1416, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Governor's Beach", pointType: "Beach / Waterfront", city: "Grand Turk", submarket: "Grand Turk", latitude: 21.4652, longitude: -71.1373, sourceReference: "https://www.visittci.com/", manuallyVerified: true }),
  pt({ name: "Grand Turk Lighthouse", pointType: "Tourist Attraction", city: "Grand Turk", submarket: "Grand Turk", latitude: 21.5104, longitude: -71.1342, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Salt Raking Historic Sites Corridor", pointType: "Tourist Attraction", city: "Grand Turk", submarket: "Grand Turk", latitude: 21.4565, longitude: -71.1427, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Columbus Landfall Marine Zone", pointType: "Beach / Waterfront", city: "Grand Turk", submarket: "Grand Turk", latitude: 21.4932, longitude: -71.1385, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Grand Turk Civic and Government Precinct", pointType: "Government / Civic Center", city: "Cockburn Town", submarket: "Grand Turk", latitude: 21.4602, longitude: -71.1403, sourceReference: "https://www.gov.tc/" }),

  // Other (6)
  pt({ name: "North Caicos Ferry Gateway", pointType: "Future Growth Node", city: "North Caicos", submarket: "Other", latitude: 21.9506, longitude: -72.0673, sourceReference: "https://www.visittci.com/", manuallyVerified: true }),
  pt({ name: "Middle Caicos Mudjin Harbour", pointType: "Tourist Attraction", city: "Middle Caicos", submarket: "Other", latitude: 21.8898, longitude: -71.9781, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "South Caicos Airport Corridor", pointType: "Future Growth Node", city: "South Caicos", submarket: "Other", latitude: 21.5158, longitude: -71.5285, sourceReference: "https://www.tciairports.com/", manuallyVerified: true }),
  pt({ name: "South Caicos Fishing Port District", pointType: "Industrial / Logistics Zone", city: "South Caicos", submarket: "Other", latitude: 21.4788, longitude: -71.5392, sourceReference: "https://www.gov.tc/" }),
  pt({ name: "Salt Cay Heritage Waterfront", pointType: "Tourist Attraction", city: "Salt Cay", submarket: "Other", latitude: 21.3353, longitude: -71.2016, sourceReference: "https://www.visittci.com/" }),
  pt({ name: "Parrot Cay Luxury Resort Zone", pointType: "Mixed-Use Development", city: "North Caicos", submarket: "Other", latitude: 21.8647, longitude: -72.0142, sourceReference: "https://www.comohotels.com/turks-and-caicos/como-parrot-cay" }),
];

export function getTurksAndCaicosCandidates() {
  return TURKS_AND_CAICOS_COUNTRYWIDE_CANDIDATES;
}

export { TURKS_AND_CAICOS_SUBMARKETS };
