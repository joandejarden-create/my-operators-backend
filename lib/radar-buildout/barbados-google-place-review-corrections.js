/**
 * Google Places review corrections for Barbados countrywide candidates.
 */

const REVIEW_TAG = "[Google review correction applied]";

/** @type {Record<string, object>} */
export const BARBADOS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Historic Bridgetown and its Garrison": {
    googleSearchQuery: "Historic Bridgetown and its Garrison UNESCO",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Grantley Adams International Airport": {
    googleSearchQuery: "Grantley Adams International Airport Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "West Coast Luxury Growth Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "South Coast Redevelopment Growth Node": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Scotland District Nature Corridor": {
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Port of Bridgetown Cruise Terminal": {
    googleSearchQuery: "Port of Bridgetown cruise terminal Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Pelican Craft Centre": {
    googleSearchQuery: "Pelican Craft Centre Bridgetown Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Cheapside Market District": {
    googleSearchQuery: "Cheapside Market Bridgetown Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "University of the West Indies Cave Hill": {
    googleSearchQuery: "UWI Cave Hill Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Holetown Heritage District": {
    googleSearchQuery: "Holetown Barbados heritage",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Sandy Lane Resort Corridor": {
    googleSearchQuery: "Sandy Lane Resort Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Royal Westmoreland Golf Community": {
    latitude: 13.2090338,
    longitude: -59.6232874,
    googleSearchQuery: "Royal Westmoreland Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Speightstown Waterfront": {
    googleSearchQuery: "Speightstown waterfront Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Folkestone Marine Park": {
    latitude: 13.1915353,
    longitude: -59.6398716,
    googleSearchQuery: "Folkestone Marine Park Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "St Lawrence Gap Entertainment District": {
    googleSearchQuery: "St Lawrence Gap Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Worthing Main Road Commercial Corridor": {
    googleSearchQuery: "Worthing Christ Church Barbados commercial district",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Graeme Hall Nature Sanctuary Area": {
    googleSearchQuery: "Graeme Hall Nature Sanctuary Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Oistins Fish Market and Bay Gardens": {
    latitude: 13.0635519,
    longitude: -59.5436082,
    googleSearchQuery: "Oistins Fish Fry Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Maxwell Beach Resort Corridor": {
    googleSearchQuery: "Maxwell Beach Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Airport Access Commercial Zone": {
    googleSearchQuery: "Grantley Adams airport access commercial area Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Barbados Cruise Homeport Logistics": {
    googleSearchQuery: "Port of Bridgetown logistics Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
  "Crane Beach Resort Area": {
    googleSearchQuery: "Crane Beach Barbados",
    manuallyVerified: true,
    reviewAction: "manual_corridor",
  },
};

export function applyBarbadosPlaceReviewCorrection(point) {
  const fix = BARBADOS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;

  let notes = String(point.notes || "");
  if (!notes.includes(REVIEW_TAG)) notes = `${notes} ${REVIEW_TAG}`.trim();
  if (fix.manuallyVerified && !/Manually verified using official/i.test(notes)) {
    notes = `${notes} Manually verified using official/public source; Google Maps match was not used as final authority.`;
  }

  return {
    ...point,
    name: fix.name ?? point.name,
    latitude: fix.latitude ?? point.latitude,
    longitude: fix.longitude ?? point.longitude,
    city: fix.city ?? point.city,
    submarket: fix.submarket ?? point.submarket,
    googleSearchQuery: fix.googleSearchQuery ?? point.googleSearchQuery,
    dataConfidence: fix.dataConfidence || (fix.manuallyVerified ? "High" : point.dataConfidence || "High"),
    manuallyVerified: fix.manuallyVerified === true || point.manuallyVerified,
    notes,
  };
}

export function applyBarbadosPlaceReviewCorrections(points) {
  return points.map(applyBarbadosPlaceReviewCorrection);
}
