/**
 * Hotel Intelligence Airtable schema V1 — six shared tables on ADP base.
 * HPC remains identity SoT. These tables hold commercial/research intelligence.
 */

import { createHash } from "node:crypto";

export const HOTEL_INTELLIGENCE_SCHEMA_VERSION = "hotel_intelligence_schema_v1";

export const HI_TABLES = Object.freeze({
  commercialProfiles: "Hotel Commercial Profiles",
  eventSpaces: "Hotel Event Spaces",
  demandNodes: "Hotel Demand Nodes",
  seasonalityNeedPeriods: "Hotel Seasonality & Need Periods",
  evidence: "Hotel Intelligence Evidence",
  adpAttributes: "Hotel ADP Attributes",
});

export const VAL_RESEARCH_STATUS = Object.freeze([
  "Not Started",
  "In Progress",
  "Complete",
  "Partial",
  "Blocked",
  "Needs Review",
]);

export const VAL_CONFIDENCE = Object.freeze(["HIGH", "MEDIUM", "LOW", "UNVERIFIED"]);

export const VAL_DEMAND_NODE_TYPE = Object.freeze([
  "Medical / Healthcare",
  "Government",
  "Military",
  "Corporate",
  "University",
  "Convention / Conference",
  "Sports",
  "Entertainment",
  "Airport",
  "Transit",
  "Tourism",
  "Religious",
  "Association",
  "Industrial",
  "Other",
]);

export const VAL_PERIOD_TYPE = Object.freeze([
  "Public Seasonality",
  "Public Need Period",
  "Hotel-Supplied Need Period",
  "Hotel-Supplied Compression Period",
  "Historical Observation",
  "Model-Derived",
]);

export const VAL_EVIDENCE_ENTITY_TYPE = Object.freeze([
  "Commercial Profile",
  "Event Space",
  "Demand Node",
  "Seasonality",
  "Need Period",
  "Other",
]);

export const VAL_ATTRIBUTE_CATEGORY = Object.freeze([
  "Identity",
  "Location",
  "Commercial",
  "Demand Node",
  "Meeting / Group",
  "Positioning",
  "Amenity",
  "Seasonality",
  "Need Period",
  "Segment Relevance",
  "Competitive Context",
  "Other",
]);

export const VAL_ADP_USE_TYPE = Object.freeze([
  "Query Generation",
  "Prompt Context",
  "Demand Family Selection",
  "Scoring",
  "Exclusion Logic",
  "Recommendation Context",
  "Reporting Only",
]);

export const VAL_SOURCE_TYPE = Object.freeze([
  "HPC",
  "Hotel Commercial Profile",
  "Hotel Demand Node",
  "Hotel Event Space",
  "Hotel Seasonality",
  "Hotel Supplied",
  "Public Research",
  "Model Derived",
]);

export const MAP_COMMERCIAL_PROFILE = Object.freeze({
  profileKey: "Profile Key",
  hpcHotelId: "HPC Hotel ID",
  dealalityHotelId: "Dealality Hotel ID",
  adpPropertyId: "ADP Property ID",
  hotelName: "Hotel Name",
  brand: "Brand",
  roomsKeys: "Rooms / Keys",
  suites: "Suites",
  singleBedRooms: "Single-Bed Rooms",
  doubleBedRooms: "Double-Bed Rooms",
  floors: "Floors / Stories",
  yearBuilt: "Year Built",
  yearRenovated: "Year Renovated",
  siteAcres: "Site Acres",
  parkingType: "Parking Type",
  parkingDailyRate: "Parking Daily Rate",
  busParking: "Bus Parking",
  airportDistanceMiles: "Airport Distance Miles",
  resortFee: "Resort Fee",
  destinationFee: "Destination Fee",
  meetingSpaceFlag: "Meeting Space Flag",
  totalMeetingSpaceSqFt: "Total Meeting Space Sq Ft",
  meetingRoomCount: "Meeting Room Count",
  largestMeetingSpaceSqFt: "Largest Meeting Space Sq Ft",
  largestEventCapacity: "Largest Event Capacity",
  outdoorEventSpaceSqFt: "Outdoor Event Space Sq Ft",
  semiPrivateSpaceSqFt: "Semi-Private Space Sq Ft",
  checkInTime: "Check-In Time",
  checkOutTime: "Check-Out Time",
  officialPropertyUrl: "Official Property URL",
  officialEventsUrl: "Official Events URL",
  profileCompletenessPct: "Profile Completeness %",
  lastResearchedAt: "Last Researched At",
  researchVersion: "Research Version",
  researchStatus: "Research Status",
  confidence: "Confidence",
  conflictStatus: "Conflict Status",
  notes: "Notes",
  schemaVersion: "Schema Version",
});

export const MAP_EVENT_SPACE = Object.freeze({
  spaceKey: "Space Key",
  hpcHotelId: "HPC Hotel ID",
  spaceName: "Space Name",
  spaceType: "Space Type",
  floor: "Floor",
  sqFt: "Sq Ft",
  dimensions: "Dimensions",
  ceilingHeight: "Ceiling Height",
  theaterCapacity: "Theater Capacity",
  classroomCapacity: "Classroom Capacity",
  banquetCapacity: "Banquet Capacity",
  receptionCapacity: "Reception Capacity",
  conferenceCapacity: "Conference Capacity",
  uShapeCapacity: "U-Shape Capacity",
  hollowSquareCapacity: "Hollow Square Capacity",
  boardroomCapacity: "Boardroom Capacity",
  outdoor: "Outdoor?",
  naturalLight: "Natural Light?",
  notes: "Notes",
  sourceRecordId: "Source Record ID",
  sourceUrl: "Source URL",
  lastVerifiedAt: "Last Verified At",
  confidence: "Confidence",
  active: "Active?",
  schemaVersion: "Schema Version",
});

export const MAP_DEMAND_NODE = Object.freeze({
  nodeKey: "Node Key",
  hpcHotelId: "HPC Hotel ID",
  demandNodeName: "Demand Node Name",
  demandNodeType: "Demand Node Type",
  address: "Address",
  city: "City",
  state: "State",
  latitude: "Latitude",
  longitude: "Longitude",
  distanceMiles: "Distance Miles",
  driveTimeMinutes: "Drive Time Minutes",
  transitTimeMinutes: "Transit Time Minutes",
  segmentRelevance: "Segment Relevance",
  groupRelevance: "Group Relevance",
  transientRelevance: "Transient Relevance",
  demandStrength: "Demand Strength",
  whyRelevant: "Why Relevant",
  sourceUrl: "Source URL",
  sourceName: "Source Name",
  sourceType: "Source Type",
  lastVerifiedAt: "Last Verified At",
  confidence: "Confidence",
  active: "Active?",
  schemaVersion: "Schema Version",
});

export const MAP_SEASONALITY = Object.freeze({
  periodKey: "Period Key",
  hpcHotelId: "HPC Hotel ID",
  periodType: "Period Type",
  startMonthDay: "Start Month-Day",
  endMonthDay: "End Month-Day",
  seasonLabel: "Season Label",
  priority: "Priority",
  segment: "Segment",
  dayOfWeekPattern: "Day-of-Week Pattern",
  description: "Description",
  sourceUrl: "Source URL",
  sourceName: "Source Name",
  hotelSupplied: "Hotel Supplied?",
  lastVerifiedAt: "Last Verified At",
  confidence: "Confidence",
  active: "Active?",
  schemaVersion: "Schema Version",
});

export const MAP_EVIDENCE = Object.freeze({
  evidenceId: "Evidence ID",
  hpcHotelId: "HPC Hotel ID",
  entityType: "Entity Type",
  entityRecordId: "Entity Record ID",
  fieldName: "Field Name",
  valueObserved: "Value Observed",
  sourceName: "Source Name",
  sourceType: "Source Type",
  sourceUrl: "Source URL",
  sourcePublishedDate: "Source Published Date",
  retrievedAt: "Retrieved At",
  evidenceSnippet: "Evidence Snippet",
  confidence: "Confidence",
  evidenceStrength: "Evidence Strength",
  current: "Current?",
  supersededBy: "Superseded By",
  notes: "Notes",
  schemaVersion: "Schema Version",
});

/** Re-export ADP attributes map from dedicated module for ensure script. */
export {
  MAP_HOTEL_ADP_ATTRIBUTE,
  HOTEL_ADP_ATTRIBUTES_TABLE,
  HOTEL_ADP_ATTRIBUTE_VERSION,
  buildAttributeDedupeKey,
} from "../adp-attributes/field-map.js";

export function normalizeKeyPart(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
}

export function commercialProfileDedupeKey(hpcHotelId) {
  return String(hpcHotelId || "").trim();
}

export function eventSpaceDedupeKey(hpcHotelId, spaceName) {
  return `${hpcHotelId}::${normalizeKeyPart(spaceName)}`;
}

export function demandNodeDedupeKey(hpcHotelId, nodeName, nodeType) {
  return `${hpcHotelId}::${normalizeKeyPart(nodeName)}::${normalizeKeyPart(nodeType)}`;
}

export function seasonalityDedupeKey(hpcHotelId, periodType, start, end, segment) {
  return `${hpcHotelId}::${normalizeKeyPart(periodType)}::${normalizeKeyPart(start)}::${normalizeKeyPart(end)}::${normalizeKeyPart(segment || "all")}`;
}

export function evidenceDedupeKey(hpcHotelId, entityType, fieldName, sourceUrl, value) {
  const raw = `${hpcHotelId}|${entityType}|${fieldName}|${sourceUrl}|${value}`;
  const h = createHash("sha256").update(raw).digest("hex").slice(0, 16);
  return `${hpcHotelId}::${normalizeKeyPart(entityType)}::${normalizeKeyPart(fieldName)}::${h}`;
}

export function adpAttributeDedupeKey(hpcHotelId, category, attributeName, version) {
  return `${hpcHotelId}::${normalizeKeyPart(category)}::${normalizeKeyPart(attributeName)}::${normalizeKeyPart(version)}`;
}
