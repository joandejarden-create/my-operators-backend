/**
 * Market Demand Intelligence — Airtable table names and field maps.
 * Tables live on Deal Capture Platform (AIRTABLE_BASE_ID_ALT), not MVP.
 */

import { DEALS_TABLE } from "../../api/schemas/deal-setup-fields.js";

/** Text field storing MVP Deals record id (rec…) — required for cross-base linking. */
export const MARKET_DEMAND_DEAL_RECORD_ID_FIELD =
  process.env.AIRTABLE_MARKET_DEMAND_DEAL_RECORD_ID_FIELD || "Deal Record ID";

/** Optional text market id on Deals table (MVP) when mirroring market link cross-base. */
export const DEALS_LINKED_MARKET_RECORD_ID_FIELD =
  process.env.AIRTABLE_DEALS_LINKED_MARKET_RECORD_ID_FIELD || "Linked Market Record ID";

// ---------------------------------------------------------------------------
// Table names (env override supported)
// ---------------------------------------------------------------------------
export const MARKETS_TABLE = process.env.AIRTABLE_TABLE_MARKETS || "Markets";
export const DEMAND_CENTERS_TABLE =
  process.env.AIRTABLE_TABLE_DEMAND_CENTERS || "Demand Centers";
export const DEMAND_CATEGORIES_TABLE =
  process.env.AIRTABLE_TABLE_DEMAND_CATEGORIES || "Demand Categories";
export const NEARBY_HOTEL_SUPPLY_TABLE =
  process.env.AIRTABLE_TABLE_NEARBY_HOTEL_SUPPLY || "Nearby Hotel Supply";
export const MARKET_DEMAND_SNAPSHOTS_TABLE =
  process.env.AIRTABLE_TABLE_MARKET_DEMAND_SNAPSHOTS || "Market Demand Snapshots";

export const MARKET_DEMAND_TABLES = [
  MARKETS_TABLE,
  DEMAND_CENTERS_TABLE,
  DEMAND_CATEGORIES_TABLE,
  NEARBY_HOTEL_SUPPLY_TABLE,
  MARKET_DEMAND_SNAPSHOTS_TABLE,
];

// ---------------------------------------------------------------------------
// Markets
// ---------------------------------------------------------------------------
export const MARKET_FIELDS = {
  name: "Market Name",
  country: "Country",
  region: "Region",
  subregion: "Subregion",
  marketType: "Market Type",
  latitude: "Latitude",
  longitude: "Longitude",
  primaryDemandProfile: "Primary Demand Profile",
  marketNotes: "Market Notes",
  dataConfidence: "Data Confidence",
  lastReviewed: "Last Reviewed",
};

// ---------------------------------------------------------------------------
// Demand Centers
// ---------------------------------------------------------------------------
export const DEMAND_CENTER_FIELDS = {
  name: "Demand Center Name",
  linkedMarket: "Linked Market",
  linkedDeals: "Linked Deals",
  dealRecordId: MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
  demandCategory: "Demand Category",
  demandSubcategory: "Demand Subcategory",
  address: "Address",
  latitude: "Latitude",
  longitude: "Longitude",
  distanceFromDeal: "Distance From Deal",
  estimatedDriveTime: "Estimated Drive Time",
  demandStrength: "Demand Strength",
  relevanceToHotelDemand: "Relevance To Hotel Demand",
  demandPattern: "Demand Pattern",
  relevantHotelTypes: "Relevant Hotel Types",
  source: "Source",
  sourceReference: "Source URL / Reference",
  sourcePlaceId: "Source Place ID",
  dataConfidence: "Data Confidence",
  lastVerified: "Last Verified",
  notes: "Notes",
  aiInterpretation: "AI Interpretation",
  relevanceScore: "Relevance Score",
};

// ---------------------------------------------------------------------------
// Demand Categories (reference)
// ---------------------------------------------------------------------------
export const DEMAND_CATEGORY_FIELDS = {
  category: "Demand Category",
  description: "Category Description",
  typicalDemandPattern: "Typical Demand Pattern",
  mostRelevantHotelTypes: "Most Relevant Hotel Types",
  brandFitImplications: "Brand Fit Implications",
  operatorFitImplications: "Operator Fit Implications",
  scoringWeight: "Scoring Weight",
};

// ---------------------------------------------------------------------------
// Nearby Hotel Supply
// ---------------------------------------------------------------------------
export const NEARBY_HOTEL_SUPPLY_FIELDS = {
  hotelName: "Hotel Name",
  linkedMarket: "Linked Market",
  linkedDeal: "Linked Deal",
  dealRecordId: MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
  brand: "Brand",
  parentCompany: "Parent Company",
  chainScale: "Chain Scale",
  hotelType: "Hotel Type",
  rooms: "Rooms",
  distanceFromDeal: "Distance From Deal",
  estimatedDriveTime: "Estimated Drive Time",
  competitiveRelevance: "Competitive Relevance",
  source: "Source",
  dataConfidence: "Data Confidence",
  notes: "Notes",
};

// ---------------------------------------------------------------------------
// Market Demand Snapshots
// ---------------------------------------------------------------------------
export const MARKET_DEMAND_SNAPSHOT_FIELDS = {
  snapshotName: "Snapshot Name",
  linkedDeal: "Linked Deal",
  dealRecordId: MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
  linkedMarket: "Linked Market",
  leisureDemandScore: "Leisure Demand Score",
  corporateDemandScore: "Corporate Demand Score",
  groupDemandScore: "Group Demand Score",
  medicalDemandScore: "Medical Demand Score",
  educationDemandScore: "Education Demand Score",
  transportationDemandScore: "Transportation Demand Score",
  industrialDemandScore: "Industrial Demand Score",
  retailMixedUseDemandScore: "Retail Mixed Use Demand Score",
  governmentDemandScore: "Government Demand Score",
  overallDemandStrength: "Overall Demand Strength",
  primaryDemandProfile: "Primary Demand Profile",
  demandSummary: "Demand Summary",
  demandGaps: "Demand Gaps",
  brandImplications: "Brand Implications",
  operatorImplications: "Operator Implications",
  recommendedFollowUp: "Recommended Follow-Up",
  dataConfidence: "Data Confidence",
  lastGenerated: "Last Generated",
};

/** Snapshot score Airtable column → normalized score key */
export const SNAPSHOT_SCORE_FIELD_TO_KEY = {
  [MARKET_DEMAND_SNAPSHOT_FIELDS.leisureDemandScore]: "leisure",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.corporateDemandScore]: "corporate",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.groupDemandScore]: "group",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.medicalDemandScore]: "medical",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.educationDemandScore]: "education",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.transportationDemandScore]: "transportation",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.industrialDemandScore]: "industrial",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.retailMixedUseDemandScore]: "retailMixedUse",
  [MARKET_DEMAND_SNAPSHOT_FIELDS.governmentDemandScore]: "government",
};

export const SNAPSHOT_SCORE_KEY_TO_FIELD = Object.fromEntries(
  Object.entries(SNAPSHOT_SCORE_FIELD_TO_KEY).map(([field, key]) => [key, field])
);

// ---------------------------------------------------------------------------
// Optional Deals table extensions (safe when absent)
// ---------------------------------------------------------------------------
export const DEALS_MARKET_DEMAND_FIELDS = {
  linkedMarket: "Linked Market",
  linkedMarketRecordId: DEALS_LINKED_MARKET_RECORD_ID_FIELD,
  demandCenterCount: "Demand Center Count",
  primaryDemandDrivers: "Primary Demand Drivers",
  demandStrengthScore: "Demand Strength Score",
  demandConfidence: "Demand Confidence",
  demandSummary: "Demand Summary",
  demandGapsQuestions: "Demand Gaps / Questions",
};

export { DEALS_TABLE };

/** UI labels for demand mix categories */
export const DEMAND_CATEGORY_LABELS = {
  leisure: "Leisure",
  corporate: "Corporate",
  group: "Group / Event",
  medical: "Medical",
  education: "Education",
  transportation: "Transportation",
  industrial: "Industrial",
  retailMixedUse: "Retail / Mixed-Use",
  government: "Government",
};

export const DEMAND_CATEGORY_KEYS = Object.keys(DEMAND_CATEGORY_LABELS);

/**
 * Map Airtable Demand Category value → normalized score key.
 * @param {unknown} category
 * @returns {string | null}
 */
export function mapDemandCategoryToKey(category) {
  const c = String(category || "")
    .trim()
    .toLowerCase();
  if (!c) return null;
  if (c.includes("leisure")) return "leisure";
  if (c.includes("corporate")) return "corporate";
  if (c.includes("group") || c.includes("event")) return "group";
  if (c.includes("medical") || c.includes("hospital") || c.includes("health")) return "medical";
  if (c.includes("education") || c.includes("university") || c.includes("school")) return "education";
  if (c.includes("transport") || c.includes("airport") || c.includes("cruise")) return "transportation";
  if (c.includes("industrial") || c.includes("logistics")) return "industrial";
  if (c.includes("retail") || c.includes("mixed")) return "retailMixedUse";
  if (c.includes("government") || c.includes("civic")) return "government";
  return null;
}
