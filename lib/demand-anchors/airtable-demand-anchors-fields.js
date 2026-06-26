/**
 * Demand Anchors — Airtable table and field map (Deal Capture Platform).
 */

import { SUBMARKET_FIELD_NAME, ALL_SUBMARKET_OPTIONS } from "../radar-submarket.js";

export const DEMAND_ANCHORS_TABLE =
  process.env.AIRTABLE_TABLE_DEMAND_ANCHORS || "Demand Anchors";

export const DEMAND_ANCHORS_SOURCE_TABLE = "Demand Anchors";

export const DEMAND_ANCHOR_DEAL_RECORD_ID_FIELD =
  process.env.AIRTABLE_DEMAND_ANCHOR_DEAL_RECORD_ID_FIELD || "Deal Record ID";

export const DEMAND_ANCHORS_FIELDS = {
  name: "Demand Anchor Name",
  radarCategory: "Radar Category",
  pointType: "Point Type",
  pointSubtype: "Point Subtype",
  linkedMarket: "Linked Market",
  dealRecordId: DEMAND_ANCHOR_DEAL_RECORD_ID_FIELD,
  linkedDeals: "Linked Deals",
  city: "City",
  country: "Country",
  region: "Region",
  submarket: SUBMARKET_FIELD_NAME,
  address: "Address / Location",
  lat: "Latitude",
  lng: "Longitude",
  distanceFromDeal: "Distance From Deal",
  estimatedDriveTime: "Estimated Drive Time",
  demandRelevance: "Demand Relevance",
  demandSegment: "Demand Segment",
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
  lastVerified: "Last Verified",
};

export const RADAR_CATEGORY_DEMAND_ANCHORS = "Demand Anchors";

export const POINT_TYPES = [
  "Convention Center",
  "Medical Campus",
  "University / College",
  "Sports Venue",
  "Entertainment District",
  "Tourist Attraction",
  "Beach / Waterfront",
  "Business District",
  "Industrial / Logistics Zone",
  "Government / Civic Center",
  "Mixed-Use Development",
  "Future Growth Node",
];

export const DEMAND_SEGMENT_OPTIONS = [
  "Group / Event",
  "Medical",
  "Education",
  "Leisure",
  "Corporate",
  "Industrial",
  "Government",
  "Mixed-Use",
  "Future Growth",
];

export const DEMAND_PATTERN_OPTIONS = [
  "Weekday",
  "Weekend",
  "Seasonal",
  "Year-Round",
  "Event-Based",
  "Group",
  "Transient",
  "Extended-Stay",
  "Business",
  "Leisure",
  "Crew",
  "Project-Based",
];

export const RELEVANT_HOTEL_TYPES_OPTIONS = [
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
  "Urban",
  "Roadside",
  "Marina / Waterfront",
  "Mixed-Use",
];

export const MAP_ICON_TYPES = [
  "Event",
  "Medical",
  "Education",
  "Sports",
  "Entertainment",
  "Attraction",
  "Beach",
  "Business",
  "Industrial",
  "Government",
  "Mixed-Use",
  "Growth Node",
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

export const SOURCE_OPTIONS = [
  "Manual Research",
  "Broker Insight",
  "Owner Input",
  "Public Source",
  "Analyst Review",
];

export { ALL_SUBMARKET_OPTIONS as SUBMARKET_OPTIONS };

export const DEMAND_ANCHORS_LAYER_NAME = "Demand Anchors";

export const DEMAND_ANCHORS_LAYER_FILTERS = [
  { id: "all", label: "All Demand Anchors" },
  ...POINT_TYPES.map((pt) => ({ id: pt, label: pt })),
];

export const POINT_TYPE_TO_MAP_ICON = {
  "Convention Center": "Event",
  "Medical Campus": "Medical",
  "University / College": "Education",
  "Sports Venue": "Sports",
  "Entertainment District": "Entertainment",
  "Tourist Attraction": "Attraction",
  "Beach / Waterfront": "Beach",
  "Business District": "Business",
  "Industrial / Logistics Zone": "Industrial",
  "Government / Civic Center": "Government",
  "Mixed-Use Development": "Mixed-Use",
  "Future Growth Node": "Growth Node",
};
