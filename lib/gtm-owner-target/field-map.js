/**
 * GTM Owner Target List — Airtable field mapping (internal-only base).
 *
 * Base: AIRTABLE_GTM_BASE_ID (separate from Dealality product bases).
 * CoStar-derived fields must never sync to Hotel Census, Scout, or public APIs.
 */

export const GTM_OWNER_TARGET_TABLES = {
  ownerTargets: process.env.AIRTABLE_GTM_OWNER_TARGETS_TABLE || "Owner Targets",
  properties: process.env.AIRTABLE_GTM_PROPERTIES_TABLE || "Properties",
  importBatches: process.env.AIRTABLE_GTM_IMPORT_BATCHES_TABLE || "GTM Import Batches",
};

/** CoStar Properties table (existing in Owner Targets Table base). */
/** @type {Record<string, string>} */
export const MAP_GTM_PROPERTIES = {
  buildingName: "Building Name",
  trueOwner: "True Owner",
  recordedOwner: "Recorded Owner",
  propertyId: "Property ID",
  submarket: "Submarket",
  market: "Market",
  country: "Country",
  city: "City",
  zipCode: "ZIP Code",
  starRating: "Star Rating",
  rbaGla: "RBA/GLA",
  yearBuilt: "Year Built",
  yearRenov: "Year Renov",
  brand: "Brand",
  parentCompany: "Parent Company",
  hotelOperator: "Hotel Operator",
  rooms: "Rooms",
  propertyType: "Type",
  ownerTargetLink: "Owner Target",
};

/** @type {Record<string, string>} */
export const MAP_GTM_OWNER_TARGET = {
  ownerName: "Owner Name",
  ownerNameNormalized: "Owner Name Normalized",
  ownerType: "Owner Type",
  priorityTier: "Priority Tier",
  outreachStatus: "Outreach Status",
  pitchStatus: "Dealality Pitch Status",
  pitchAngle: "Pitch Angle",
  propertyCount: "Property Count",
  totalRbaSf: "Total RBA SF",
  marketsSummary: "Markets Summary",
  countriesSummary: "Countries Summary",
  sampleProperties: "Sample Properties",
  contactPath: "Contact Path",
  primaryContactName: "Primary Contact Name",
  primaryContactEmail: "Primary Contact Email",
  primaryContactPhone: "Primary Contact Phone",
  nextAction: "Next Action",
  nextActionDate: "Next Action Date",
  assignedTo: "Assigned To",
  internalNotes: "Internal Notes",
  dataSource: "Data Source",
  dataLicense: "Data License",
  visibility: "Visibility",
  lastCostarSyncAt: "Last CoStar Sync At",
  importBatch: "Import Batch",
  properties: "Properties",
  contacts: "Contacts",
  icpSegment: "ICP Segment",
  strikeList: "Strike List",
  dealTrigger: "Deal Trigger",
  icpClassificationNotes: "ICP Classification Notes",
  calaPropertyCount: "CALA Property Count",
};

/** @type {Record<string, string>} */
export const MAP_GTM_TARGET_PROPERTY = {
  buildingName: "Building Name",
  ownerTarget: "Owner Target",
  trueOwnerRaw: "True Owner Raw",
  costarPropertyId: "CoStar Property ID",
  submarket: "Submarket",
  market: "Market",
  country: "Country",
  city: "City",
  zipCode: "ZIP Code",
  starRating: "Star Rating",
  rbaGlaSf: "RBA GLA SF",
  yearBuilt: "Year Built",
  yearRenovated: "Year Renovated",
  brandAffiliation: "Brand Affiliation",
  propertyType: "Property Type",
  builtRenovText: "Built Renov Text",
  importBatch: "Import Batch",
  internalNotes: "Internal Notes",
  sourceRowKey: "Source Row Key",
};

/** @type {Record<string, string>} */
export const MAP_GTM_IMPORT_BATCH = {
  batchLabel: "Batch Label",
  sourceFileName: "Source File Name",
  sourceFilePath: "Source File Path",
  rowCount: "Row Count",
  ownerCount: "Owner Count",
  propertyCreateCount: "Property Create Count",
  propertyUpdateCount: "Property Update Count",
  appliedAt: "Applied At",
  appliedBy: "Applied By",
  status: "Status",
  previewReportPath: "Preview Report Path",
  notes: "Notes",
};

export const VAL_GTM_OWNER_TYPE = [
  "regional_operator",
  "integrated_operator",
  "family_office",
  "institutional",
  "reit",
  "spv",
  "gaming_hospitality",
  "individual",
  "unknown",
];

export const VAL_GTM_PRIORITY_TIER = ["A", "B", "C"];

export const VAL_GTM_OUTREACH_STATUS = [
  "not_contacted",
  "researching",
  "intro_sent",
  "responded",
  "meeting_scheduled",
  "pilot_discussion",
  "active",
  "passed",
  "parked",
];

export const VAL_GTM_PITCH_STATUS = [
  "not_pitched",
  "pitched",
  "interested",
  "not_interested",
  "on_hold",
];

export const VAL_GTM_CONTACT_PATH = [
  "warm_intro",
  "linkedin",
  "conference",
  "broker",
  "cold_email",
  "phone",
  "other",
];

export const VAL_GTM_DATA_SOURCE = ["costar_internal", "manual", "mixed"];

export const VAL_GTM_DATA_LICENSE = ["costar_licensed_internal"];

export const VAL_GTM_VISIBILITY = ["internal_only"];

export const VAL_GTM_IMPORT_BATCH_STATUS = ["preview", "applied", "failed"];

/** GTM outreach ICP segmentation (internal only). */
export const VAL_GTM_ICP_SEGMENT = [
  "owner_operator",
  "asset_owner",
  "regional_operator",
  "institutional",
  "reit",
  "franchisor_brand",
  "broker_advisor",
  "spv_single_asset",
  "gaming_hospitality",
  "individual",
  "skip",
  "unknown",
];

export const VAL_GTM_DEAL_TRIGGER = [
  "none_known",
  "conversion",
  "reflag",
  "operator_rfp",
  "new_build",
  "portfolio_standardization",
  "sale_process",
  "independent_unbranded",
  "brand_renewal_window",
  "development_pipeline",
  "recent_open_branded",
];

/** ICP segments eligible for direct owner outreach when other strike criteria pass. */
export const VAL_GTM_ICP_STRIKE_ELIGIBLE = [
  "owner_operator",
  "asset_owner",
  "regional_operator",
  "institutional",
  "reit",
];
