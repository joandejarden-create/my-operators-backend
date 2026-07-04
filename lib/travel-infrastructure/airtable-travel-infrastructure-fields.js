/**
 * Travel Infrastructure Data — Airtable table and field map.
 * Table lives on Deal Capture Platform (AIRTABLE_BASE_ID_ALT).
 *
 * Legacy fields (Name, Type, City, Country, Region, Latitude, Longitude) are preserved.
 * New radar fields are additive; readers use compatibility aliases in normalize-radar-map-point.js.
 */

import { SUBMARKET_FIELD_NAME, ALL_SUBMARKET_OPTIONS } from "../radar-submarket.js";

/** Primary table name in Airtable (audit: Travel Infrastructure Data). */
export const TRAVEL_INFRASTRUCTURE_TABLE =
  process.env.AIRTABLE_TABLE_TRAVEL_INFRASTRUCTURE || "Travel Infrastructure Data";

/** Legacy table name variant used in older API code. */
export const TRAVEL_INFRASTRUCTURE_TABLE_LEGACY = "Travel Infrastructure data";

export const TRAVEL_INFRASTRUCTURE_SOURCE_TABLE = "Travel Infrastructure Data";

/** Text field for MVP Deals record id when deal-scoped points are added. */
export const TRAVEL_INFRA_DEAL_RECORD_ID_FIELD =
  process.env.AIRTABLE_TRAVEL_INFRA_DEAL_RECORD_ID_FIELD || "Deal Record ID";

// ---------------------------------------------------------------------------
// Field map — legacy + radar extensions
// ---------------------------------------------------------------------------
export const TRAVEL_INFRASTRUCTURE_FIELDS = {
  // Legacy (do not rename)
  name: "Name",
  type: "Type",
  lat: "Latitude",
  lng: "Longitude",
  city: "City",
  country: "Country",
  region: "Region",
  submarket: SUBMARKET_FIELD_NAME,
  iataCode: "IATA Code",
  icaoCode: "ICAO Code",
  unLocode: "UN/LOCODE",
  scaleTier: "Scale Tier",
  infrastructureRole: "Infrastructure Role",
  airportType: "Airport Type",
  sourceUrl: "Source URL",
  lastVerified: "Last Verified",

  // Radar / demand extensions (additive)
  radarCategory: "Radar Category",
  pointType: "Point Type",
  pointSubtype: "Point Subtype",
  linkedMarket: "Linked Market",
  dealRecordId: TRAVEL_INFRA_DEAL_RECORD_ID_FIELD,
  address: "Address / Location",
  distanceFromDeal: "Distance From Deal",
  estimatedDriveTime: "Estimated Drive Time",
  demandRelevance: "Demand Relevance",
  demandPattern: "Demand Pattern",
  relevantHotelTypes: "Relevant Hotel Types",
  hotelDemandRationale: "Hotel Demand Rationale",
  source: "Source",
  sourceReference: "Source URL / Reference",
  dataConfidence: "Data Confidence",
  includeOnRadarMap: "Include On Radar Map",
  mapLayer: "Map Layer",
  mapIconType: "Map Icon Type",
  visibility: "Visibility",
  notes: "Notes",
};

export const RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE = "Travel Infrastructure";

export const POINT_TYPES = [
  "Airport",
  "Cruise Port",
  "Train Station",
  "Highway Access",
  "Bus Terminal",
  "Ferry Terminal",
  "Port / Maritime",
];

/** Legacy Type values still present in existing records. */
export const LEGACY_POINT_TYPES = ["Convention Center"];

export const POINT_SUBTYPES = [
  "International Airport",
  "Regional Airport",
  "Cruise Terminal",
  "Rail Hub",
  "Metro Hub",
  "Highway Exit",
  "Major Corridor",
  "Intercity Bus Terminal",
  "Ferry Terminal",
  "Island Access",
  "Cargo Port",
  "Marina",
  "Ferry Port",
];

export const MAP_ICON_TYPES = [
  "Airport",
  "Cruise Port",
  "Train",
  "Highway",
  "Bus",
  "Ferry",
  "Port",
  "Convention",
];

export const DEMAND_PATTERN_OPTIONS = [
  "Transient",
  "Crew",
  "Leisure",
  "Business",
  "Group",
  "Seasonal",
  "Year-Round",
  "Weekday",
  "Weekend",
  "Drive-To",
  "Price-Sensitive",
  "Extended-Stay",
];

export const RELEVANT_HOTEL_TYPES_OPTIONS = [
  "Airport",
  "Select-Service",
  "Full-Service",
  "Extended-Stay",
  "Resort",
  "Lifestyle",
  "Economy",
  "Midscale",
  "Upper-Midscale",
  "Upscale",
  "Upper-Upscale",
  "Luxury",
  "Roadside",
  "Urban",
  "Marina / Waterfront",
];

export const DEMAND_RELEVANCE_OPTIONS = ["High", "Medium", "Low", "Unknown"];

export const DATA_CONFIDENCE_OPTIONS = ["High", "Medium", "Low"];

export const VISIBILITY_OPTIONS = [
  "Internal Only",
  "Owner Visible",
  "Brand Visible",
  "Operator Visible",
  "Demo",
];

export { ALL_SUBMARKET_OPTIONS as SUBMARKET_OPTIONS };

export const TRAVEL_INFRA_LAYER_NAME = "Travel Infrastructure";

export const TRAVEL_INFRA_LAYER_FILTERS = [
  { id: "all", label: "All Travel Infrastructure" },
  { id: "Airport", label: "Airport" },
  { id: "Cruise Port", label: "Cruise Port" },
  { id: "Train Station", label: "Train Station" },
  { id: "Highway Access", label: "Highway Access" },
  { id: "Bus Terminal", label: "Bus Terminal" },
  { id: "Ferry Terminal", label: "Ferry Terminal" },
  { id: "Port / Maritime", label: "Port / Maritime" },
];

/** Map legacy Infrastructure Role → Point Subtype hint. */
export const INFRASTRUCTURE_ROLE_TO_SUBTYPE = {
  "International Hub (Airport)": "International Airport",
  "Domestic / Regional Hub (Airport)": "Regional Airport",
  "Leisure / Resort Gateway (Airport)": "Regional Airport",
  "Secondary / Spoke (Airport)": "Regional Airport",
  "Cruise Homeport": "Cruise Terminal",
  "Cruise Port of Call": "Cruise Terminal",
};

/** Map legacy Type → default Map Icon Type. */
export const LEGACY_TYPE_TO_MAP_ICON = {
  Airport: "Airport",
  "Cruise Port": "Cruise Port",
  "Convention Center": "Convention",
};

/** Map Point Type → Map Icon Type. */
export const POINT_TYPE_TO_MAP_ICON = {
  Airport: "Airport",
  "Cruise Port": "Cruise Port",
  "Train Station": "Train",
  "Highway Access": "Highway",
  "Bus Terminal": "Bus",
  "Ferry Terminal": "Ferry",
  "Port / Maritime": "Port",
  "Convention Center": "Convention",
};
