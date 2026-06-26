/**
 * Barbados countrywide demand anchor candidates (source-backed).
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import {
  applyBarbadosGovernanceDefaults,
  BARBADOS_SUBMARKETS,
} from "./barbados-demand-anchor-governance.js";

const COUNTRY = "Barbados";
const REGION = "Caribbean";

function pt(v) {
  const defaults = getPointTypeDefaults(v.pointType);
  const rationale =
    v.hotelDemandNote ||
    defaults.hotelDemandRationale ||
    "Supports identifiable hotel demand in this Barbados corridor.";
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
  return applyBarbadosGovernanceDefaults(base, v.governance || {});
}

/** @type {ReturnType<typeof pt>[]} */
export const BARBADOS_COUNTRYWIDE_CANDIDATES = [
  // Bridgetown (10)
  pt({
    name: "Historic Bridgetown and its Garrison",
    pointType: "Government / Civic Center",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.0975,
    longitude: -59.6167,
    sourceReference: "https://whc.unesco.org/en/list/1376/",
    manuallyVerified: true,
  }),
  pt({
    name: "Port of Bridgetown Cruise Terminal",
    pointType: "Mixed-Use Development",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.106,
    longitude: -59.627,
    sourceReference: "https://www.barbadosport.com/",
  }),
  pt({
    name: "Parliament Buildings Barbados",
    pointType: "Government / Civic Center",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.0979,
    longitude: -59.6128,
    sourceReference: "https://www.barbadosparliament.com/",
  }),
  pt({
    name: "National Heroes Square",
    pointType: "Government / Civic Center",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.0973,
    longitude: -59.6132,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Kensington Oval",
    pointType: "Sports Venue",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.1054,
    longitude: -59.6249,
    sourceReference: "https://www.windiescricket.com/",
  }),
  pt({
    name: "Pelican Craft Centre",
    pointType: "Entertainment District",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.1046,
    longitude: -59.6222,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Cheapside Market District",
    pointType: "Business District",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.1039,
    longitude: -59.6145,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Queen Elizabeth Hospital Barbados",
    pointType: "Medical Campus",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.0844,
    longitude: -59.5987,
    sourceReference: "https://www.qehconnect.com/",
  }),
  pt({
    name: "University of the West Indies Cave Hill",
    pointType: "University / College",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.1422,
    longitude: -59.6376,
    sourceReference: "https://www.cavehill.uwi.edu/",
  }),
  pt({
    name: "Bridgetown Waterfront Boardwalk",
    pointType: "Beach / Waterfront",
    city: "Bridgetown",
    submarket: "Bridgetown",
    latitude: 13.0995,
    longitude: -59.6209,
    sourceReference: "https://www.visitbarbados.org/",
    manuallyVerified: true,
  }),

  // West Coast (10)
  pt({
    name: "Holetown Heritage District",
    pointType: "Tourist Attraction",
    city: "Holetown",
    submarket: "West Coast",
    latitude: 13.1865,
    longitude: -59.6387,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Limegrove Lifestyle Centre",
    pointType: "Mixed-Use Development",
    city: "Holetown",
    submarket: "West Coast",
    latitude: 13.1879,
    longitude: -59.6355,
    sourceReference: "https://www.limegrove.com/",
  }),
  pt({
    name: "Sandy Lane Resort Corridor",
    pointType: "Beach / Waterfront",
    city: "Holetown",
    submarket: "West Coast",
    latitude: 13.1752,
    longitude: -59.6362,
    sourceReference: "https://www.sandylane.com/",
  }),
  pt({
    name: "Royal Westmoreland Golf Community",
    pointType: "Sports Venue",
    city: "Westmoreland",
    submarket: "West Coast",
    latitude: 13.2164,
    longitude: -59.6244,
    sourceReference: "https://www.royalwestmoreland.com/",
  }),
  pt({
    name: "Speightstown Waterfront",
    pointType: "Beach / Waterfront",
    city: "Speightstown",
    submarket: "West Coast",
    latitude: 13.2506,
    longitude: -59.6418,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Port St. Charles Marina",
    pointType: "Mixed-Use Development",
    city: "Speightstown",
    submarket: "West Coast",
    latitude: 13.2501,
    longitude: -59.6465,
    sourceReference: "https://www.portstcharles.com/",
  }),
  pt({
    name: "Mullins Beach",
    pointType: "Beach / Waterfront",
    city: "Mullins",
    submarket: "West Coast",
    latitude: 13.2346,
    longitude: -59.6424,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Folkestone Marine Park",
    pointType: "Tourist Attraction",
    city: "Holetown",
    submarket: "West Coast",
    latitude: 13.1842,
    longitude: -59.6384,
    sourceReference: "https://www.barbadosmarinedebris.org/folkestone-marine-park",
  }),
  pt({
    name: "Paynes Bay Resort Strip",
    pointType: "Beach / Waterfront",
    city: "Paynes Bay",
    submarket: "West Coast",
    latitude: 13.1605,
    longitude: -59.6363,
    sourceReference: "https://www.visitbarbados.org/",
    manuallyVerified: true,
  }),
  pt({
    name: "West Coast Luxury Growth Corridor",
    pointType: "Future Growth Node",
    city: "St James",
    submarket: "West Coast",
    latitude: 13.2065,
    longitude: -59.6335,
    sourceReference: "https://www.investbarbados.org/",
    manuallyVerified: true,
  }),

  // South Coast (10)
  pt({
    name: "St Lawrence Gap Entertainment District",
    pointType: "Entertainment District",
    city: "Christ Church",
    submarket: "South Coast",
    latitude: 13.0672,
    longitude: -59.5693,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Dover Beach",
    pointType: "Beach / Waterfront",
    city: "Christ Church",
    submarket: "South Coast",
    latitude: 13.0647,
    longitude: -59.5681,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Rockley Beach Boardwalk",
    pointType: "Beach / Waterfront",
    city: "Rockley",
    submarket: "South Coast",
    latitude: 13.0779,
    longitude: -59.5864,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Worthing Main Road Commercial Corridor",
    pointType: "Business District",
    city: "Worthing",
    submarket: "South Coast",
    latitude: 13.0719,
    longitude: -59.5781,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Hastings Boardwalk",
    pointType: "Beach / Waterfront",
    city: "Hastings",
    submarket: "South Coast",
    latitude: 13.0803,
    longitude: -59.5915,
    sourceReference: "https://www.visitbarbados.org/",
    manuallyVerified: true,
  }),
  pt({
    name: "Graeme Hall Nature Sanctuary Area",
    pointType: "Tourist Attraction",
    city: "Christ Church",
    submarket: "South Coast",
    latitude: 13.0724,
    longitude: -59.5764,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Oistins Fish Market and Bay Gardens",
    pointType: "Entertainment District",
    city: "Oistins",
    submarket: "South Coast",
    latitude: 13.0708,
    longitude: -59.5538,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "South Point Lighthouse Area",
    pointType: "Tourist Attraction",
    city: "Christ Church",
    submarket: "South Coast",
    latitude: 13.0468,
    longitude: -59.5236,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Maxwell Beach Resort Corridor",
    pointType: "Beach / Waterfront",
    city: "Maxwell",
    submarket: "South Coast",
    latitude: 13.0588,
    longitude: -59.5608,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "South Coast Redevelopment Growth Node",
    pointType: "Future Growth Node",
    city: "Christ Church",
    submarket: "South Coast",
    latitude: 13.0743,
    longitude: -59.582,
    sourceReference: "https://www.investbarbados.org/",
    manuallyVerified: true,
  }),

  // Other (10)
  pt({
    name: "Grantley Adams International Airport",
    pointType: "Future Growth Node",
    city: "Seawell",
    submarket: "Other",
    latitude: 13.0746,
    longitude: -59.4925,
    sourceReference: "https://www.gaia.bb/",
    manuallyVerified: true,
  }),
  pt({
    name: "Airport Access Commercial Zone",
    pointType: "Industrial / Logistics Zone",
    city: "Seawell",
    submarket: "Other",
    latitude: 13.0812,
    longitude: -59.5011,
    sourceReference: "https://www.gaia.bb/",
  }),
  pt({
    name: "Barbados Cruise Homeport Logistics",
    pointType: "Industrial / Logistics Zone",
    city: "Bridgetown",
    submarket: "Other",
    latitude: 13.1092,
    longitude: -59.6281,
    sourceReference: "https://www.barbadosport.com/",
  }),
  pt({
    name: "Harrison's Cave Eco-Adventure Park",
    pointType: "Tourist Attraction",
    city: "Welchman Hall",
    submarket: "Other",
    latitude: 13.1745,
    longitude: -59.5715,
    sourceReference: "https://chukka.com/attractions/harrisons-cave-eco-adventure-park/",
  }),
  pt({
    name: "Flower Forest Botanical Gardens",
    pointType: "Tourist Attraction",
    city: "St Joseph",
    submarket: "Other",
    latitude: 13.1831,
    longitude: -59.5423,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Bathsheba Surf and East Coast Lookout",
    pointType: "Beach / Waterfront",
    city: "Bathsheba",
    submarket: "Other",
    latitude: 13.2111,
    longitude: -59.525,
    sourceReference: "https://www.visitbarbados.org/",
    manuallyVerified: true,
  }),
  pt({
    name: "Animal Flower Cave",
    pointType: "Tourist Attraction",
    city: "North Point",
    submarket: "Other",
    latitude: 13.3334,
    longitude: -59.6274,
    sourceReference: "https://www.animalflowercave.com/",
  }),
  pt({
    name: "Crane Beach Resort Area",
    pointType: "Beach / Waterfront",
    city: "St Philip",
    submarket: "Other",
    latitude: 13.1048,
    longitude: -59.4326,
    sourceReference: "https://www.visitbarbados.org/",
  }),
  pt({
    name: "Sam Lord's Castle Redevelopment",
    pointType: "Mixed-Use Development",
    city: "St Philip",
    submarket: "Other",
    latitude: 13.1312,
    longitude: -59.4371,
    sourceReference: "https://www.wyndhamhotels.com/",
  }),
  pt({
    name: "Scotland District Nature Corridor",
    pointType: "Future Growth Node",
    city: "St Andrew",
    submarket: "Other",
    latitude: 13.2548,
    longitude: -59.5657,
    sourceReference: "https://www.visitbarbados.org/",
    manuallyVerified: true,
  }),
];

export function getBarbadosCandidates() {
  return BARBADOS_COUNTRYWIDE_CANDIDATES;
}

export { BARBADOS_SUBMARKETS };
