/**
 * Central field mapping for Deal Setup (PATCH) write path.
 * Single source of truth for Airtable table names, link fields, and form→Airtable column names.
 * Used by api/my-deals.js updateMyDealById and by deal-setup validation.
 */

import { isOperatorInScopeFromFields } from "../../lib/operator-capability-inputs.js";
import { OAS_DEAL_MP_FIELD_NAMES, OAS_DEAL_SI_FIELD_NAMES } from "../../lib/operator-alignment-field-options.js";
import {
  MIXED_USE_INTAKE_FIELD_NAMES,
  isMixedUseIntakeInScopeFromFields,
  mixedUseIntakeConditionalRequiredFields,
  coerceMixedUseMpFieldForWrite,
  coerceMixedUseMpFieldForRead,
  coerceMixedUseSelectForWrite,
  coerceMixedUseSelectForRead,
} from "../../lib/mixed-use-intake-field-options.js";

export {
  isMixedUseIntakeInScopeFromFields,
  mixedUseIntakeConditionalRequiredFields,
  coerceMixedUseMpFieldForWrite,
  coerceMixedUseMpFieldForRead,
  coerceMixedUseSelectForWrite,
  coerceMixedUseSelectForRead,
} from "../../lib/mixed-use-intake-field-options.js";

// ---------------------------------------------------------------------------
// Table names (env or default)
// ---------------------------------------------------------------------------
export const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
export const LOCATION_PROPERTY_TABLE = process.env.AIRTABLE_TABLE_LOCATION_PROPERTY || "Location & Property";
export const MARKET_PERFORMANCE_TABLE = "Market - Performance - Deal & Capital Structure";
export const STRATEGIC_INTENT_TABLE = process.env.AIRTABLE_TABLE_STRATEGIC_INTENT || "Strategic Intent - Operational - Key Challenges";
export const CONTACT_UPLOADS_TABLE = process.env.AIRTABLE_TABLE_CONTACT_UPLOADS || "Contact & Uploads";
export const LEASE_STRUCTURE_TABLE = process.env.AIRTABLE_TABLE_LEASE_STRUCTURE || "Lease Structure";

// ---------------------------------------------------------------------------
// Deal Status: one field only (env or default "Deal Status")
// ---------------------------------------------------------------------------
export const DEALS_STATUS_FIELD = process.env.AIRTABLE_DEALS_STATUS_FIELD || "Deal Status";

// ---------------------------------------------------------------------------
// Deal Readiness Review — persisted on the Deals table (override names via env if your base differs)
// ---------------------------------------------------------------------------
export const DEAL_READINESS_SCORE_AIRTABLE_FIELD =
  process.env.DEAL_READINESS_SCORE_FIELD || "Deal Readiness Score";
export const DEAL_READINESS_STAGE_AIRTABLE_FIELD =
  process.env.DEAL_READINESS_STAGE_FIELD || "Deal Readiness Stage";
/** Long text / multiline summary; optional — only written when set in env. */
export const DEAL_READINESS_SUMMARY_AIRTABLE_FIELD = process.env.DEAL_READINESS_SUMMARY_FIELD || "";
/**
 * Date or date-time on Deals when saving a review.
 * Default: "Deal Readiness Last Reviewed". Set to "0" or "false" to skip PATCH (bases with only score + stage).
 */
export const DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD = (() => {
  const v = process.env.DEAL_READINESS_LAST_REVIEWED_FIELD;
  if (v === "0" || v === "false") return "";
  return v || "Deal Readiness Last Reviewed";
})();

/** Map Airtable Deals fields → My Deals list / readiness UI (missing keys when column absent). */
export function extractDealReadinessListFields(airtableFields) {
  const f = airtableFields || {};
  const out = {};
  const rawSc = f[DEAL_READINESS_SCORE_AIRTABLE_FIELD];
  if (rawSc != null && rawSc !== "") {
    const n = Number(rawSc);
    if (Number.isFinite(n)) out.dealReadinessScore = Math.round(n);
  }
  const st = f[DEAL_READINESS_STAGE_AIRTABLE_FIELD];
  if (st != null && String(st).trim() !== "") out.dealReadinessStage = String(st).trim();
  const missKey = process.env.DEAL_READINESS_MISSING_COUNT_FIELD || "Deal Readiness Missing Count";
  const blockKey = process.env.DEAL_READINESS_BLOCKING_COUNT_FIELD || "Deal Readiness Blocking Count";
  const rawM = f[missKey];
  if (rawM != null && rawM !== "") {
    const n = Number(rawM);
    if (Number.isFinite(n)) out.dealReadinessMissingCount = n;
  }
  const rawB = f[blockKey];
  if (rawB != null && rawB !== "") {
    const n = Number(rawB);
    if (Number.isFinite(n)) out.dealReadinessBlockingCount = n;
  }
  if (DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD) {
    const rawL = f[DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD];
    if (rawL != null && rawL !== "") {
      out.dealReadinessLastReviewed =
        typeof rawL === "string" ? rawL : rawL instanceof Date ? rawL.toISOString() : String(rawL);
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Commercial Readiness Snapshot — persisted on Deals table (env-overridable)
// ---------------------------------------------------------------------------
export const COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_STATUS_FIELD || "Commercial Readiness Status";
export const COMMERCIAL_READINESS_INPUTS_JSON_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_INPUTS_JSON_FIELD || "Commercial Readiness Inputs JSON";
export const COMMERCIAL_READINESS_SNAPSHOT_JSON_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_SNAPSHOT_JSON_FIELD || "Commercial Readiness Snapshot JSON";
export const COMMERCIAL_READINESS_NARRATIVE_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_NARRATIVE_FIELD || "Commercial Readiness Narrative";
export const COMMERCIAL_READINESS_LEVEL_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_LEVEL_FIELD || "Commercial Readiness Level";
export const COMMERCIAL_READINESS_EVIDENCE_CONFIDENCE_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_EVIDENCE_CONFIDENCE_FIELD || "Commercial Readiness Evidence Confidence";
export const COMMERCIAL_READINESS_OTA_RISK_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_OTA_RISK_FIELD || "Commercial OTA Dependency Risk";
export const COMMERCIAL_READINESS_DIRECT_CAPABILITY_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_DIRECT_CAPABILITY_FIELD || "Commercial Direct Booking Capability";
export const COMMERCIAL_READINESS_BRAND_NEED_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_BRAND_NEED_FIELD || "Commercial Brand Distribution Need";
export const COMMERCIAL_READINESS_OPERATOR_NEED_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_OPERATOR_NEED_FIELD || "Commercial Operator Capability Need";
export const COMMERCIAL_READINESS_LAST_GENERATED_AT_AIRTABLE_FIELD =
  process.env.COMMERCIAL_READINESS_LAST_GENERATED_AT_FIELD || "Commercial Readiness Last Generated At";

/** Map Airtable Deals fields -> My Deals list/commercial readiness badges. */
export function extractCommercialReadinessListFields(airtableFields) {
  const f = airtableFields || {};
  const out = {};
  const status = f[COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD];
  if (status != null && String(status).trim() !== "") {
    out.commercialReadinessStatus = String(status).trim();
  }
  const level = f[COMMERCIAL_READINESS_LEVEL_AIRTABLE_FIELD];
  if (level != null && String(level).trim() !== "") {
    out.commercialReadinessLevel = String(level).trim();
  }
  const confidence = f[COMMERCIAL_READINESS_EVIDENCE_CONFIDENCE_AIRTABLE_FIELD];
  if (confidence != null && String(confidence).trim() !== "") {
    out.commercialReadinessEvidenceConfidence = String(confidence).trim();
  }
  const rawLast = f[COMMERCIAL_READINESS_LAST_GENERATED_AT_AIRTABLE_FIELD];
  if (rawLast != null && String(rawLast).trim() !== "") {
    out.commercialReadinessLastGeneratedAt =
      typeof rawLast === "string" ? rawLast : rawLast instanceof Date ? rawLast.toISOString() : String(rawLast);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Deals table: form field name → Airtable column name (Batch 1 and other Deals-only fields)
// Use when form key and Airtable column differ (typos, renames).
// Franchise/affiliation: many bases use the legacy typo "agreeement"; we default write to that.
// If your base uses the correct spelling "agreement", set AIRTABLE_DEALS_FRANCHISE_AFFILIATION_FIELD
// to the full correct column name.
// ---------------------------------------------------------------------------
/** Correct spelling (for read mapping and env override). */
const DEALS_FRANCHISE_AFFILIATION_AIRTABLE_CORRECT =
  "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?";
/** Typo column name (legacy): base often has "agreeement"; default PATCH write to this so write succeeds. */
const DEALS_FRANCHISE_AFFILIATION_AIRTABLE_TYPO =
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?";

const DEALS_FRANCHISE_AFFILIATION_AIRTABLE =
  process.env.AIRTABLE_DEALS_FRANCHISE_AFFILIATION_FIELD ||
  DEALS_FRANCHISE_AFFILIATION_AIRTABLE_TYPO;

const DEALS_OPERATOR_NAME_AIRTABLE =
  process.env.AIRTABLE_DEALS_OPERATOR_NAME_FIELD || "Operator Name Current";

const DEALS_FRANCHISE_AFFILIATION_FORM_KEY =
  "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?";

export const DEALS_FORM_TO_AIRTABLE = {
  [DEALS_FRANCHISE_AFFILIATION_FORM_KEY]: DEALS_FRANCHISE_AFFILIATION_AIRTABLE,
  "Are you open to considering other brands with favorable terms?": "Are you open to lesser-known or emerging brands with favorable terms?",
  "Operator Name Current": DEALS_OPERATOR_NAME_AIRTABLE,
};

/** P0 Operator Capability Snapshot — Deals table only (form name = Airtable column). */
export const DEALS_OPERATOR_CAPABILITY_FORM_FIELDS = [
  "Current Operating Model",
  "Opening / Transition Phase",
];

/** P0 Operator Capability — re-exported field names for APIs and scripts. */
export {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  P0_DEALS_FORM_FIELDS,
  P0_LOCATION_FORM_FIELDS,
  P0_SI_FORM_FIELDS,
} from "../../lib/operator-capability-inputs.js";

export {
  PROJECT_TYPE_CANONICAL_OPTIONS,
  normalizeProjectTypeLabel,
  resolveProjectTypeKind,
  isDeprecatedProjectTypeWriteValue,
} from "../../lib/project-type.js";

/** Airtable column name → form field name (for GET merge so client rebind uses form keys). */
export const DEALS_AIRTABLE_TO_FORM = {
  [DEALS_FRANCHISE_AFFILIATION_AIRTABLE_CORRECT]: DEALS_FRANCHISE_AFFILIATION_FORM_KEY,
  [DEALS_FRANCHISE_AFFILIATION_AIRTABLE_TYPO]: DEALS_FRANCHISE_AFFILIATION_FORM_KEY,
  "Are you open to lesser-known or emerging brands with favorable terms?": "Are you open to considering other brands with favorable terms?",
  [DEALS_OPERATOR_NAME_AIRTABLE]: "Operator Name Current",
};

// ---------------------------------------------------------------------------
// Link fields (Deals ↔ linked tables)
// ---------------------------------------------------------------------------
export const LOCATION_LINK_FIELD = "Location & Property";
export const LOCATION_LINK_ALIAS = "Location and Property";
export const LOCATION_PROPERTY_ID_FIELD = process.env.AIRTABLE_LOCATION_PROPERTY_ID_FIELD || "Location_Property_ID";

export const MARKET_PERFORMANCE_LINK_FIELD = "Market - Performance - Deal & Capital Structure";
export const MP_DEAL_LINK_FIELD = process.env.AIRTABLE_MP_DEAL_LINK_FIELD || "Deal_ID";

export const STRATEGIC_INTENT_LINK_FIELD = "Strategic Intent - Operational - Key Challenges";

export const CONTACT_UPLOADS_LINK_FIELD = "Contact & Uploads";
export const CU_DEAL_LINK_FIELD = process.env.AIRTABLE_CU_DEAL_LINK_FIELD || "Deal_ID";

/** Form key for Tab 13 generic supporting-doc upload UI (not an Airtable column). */
export const CU_ATTACHMENT_FORM_KEY = "Upload Supporting Docs";

/**
 * Contact & Uploads attachment columns in Airtable (exact names from base schema).
 * Generic pilot uploads write to CU_ATTACHMENT_FIELD; reads aggregate every column below.
 *
 * Live Deal Capture base (2026-06): single bucket "Pro Forma or Financials".
 * Override full list: AIRTABLE_CU_ATTACHMENT_FIELDS=Pro Forma or Financials,Other Field
 */
const DEFAULT_CU_ATTACHMENT_AIRTABLE_FIELDS = ["Pro Forma or Financials"];

export const CU_ATTACHMENT_AIRTABLE_FIELDS = (() => {
  const env = process.env.AIRTABLE_CU_ATTACHMENT_FIELDS;
  if (env && String(env).trim()) {
    return String(env)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return DEFAULT_CU_ATTACHMENT_AIRTABLE_FIELDS.slice();
})();

/**
 * Airtable column for generic Tab 13 uploads when the UI does not classify by document type.
 * Override with AIRTABLE_CU_ATTACHMENT_FIELD if your base uses a different default bucket.
 */
export const CU_ATTACHMENT_FIELD =
  process.env.AIRTABLE_CU_ATTACHMENT_FIELD || "Pro Forma or Financials";

/** True when URL is hosted by Airtable (persists in attachment fields). */
export function isAirtableHostedAttachmentUrl(url) {
  return /airtableusercontent\.com/i.test(String(url || ""));
}

/** Normalize one attachment entry from Airtable or API payload. */
export function normalizeCuAttachmentItem(raw) {
  if (!raw) return null;
  if (typeof raw === "object" && raw.url) {
    return {
      id: raw.id,
      url: String(raw.url).trim(),
      filename: String(raw.filename ?? raw.name ?? "").trim(),
      size: raw.size,
      type: raw.type,
      thumbnails: raw.thumbnails,
    };
  }
  if (typeof raw === "string" && raw.trim()) {
    return { url: raw.trim(), filename: "" };
  }
  return null;
}

/** Check that every expected filename exists on a CU attachment field array. */
export function cuAttachmentFieldHasFilenames(fieldAttachments, expectedFilenames) {
  const norm = (s) => String(s || "").trim().toLowerCase();
  const inField = new Set(
    (Array.isArray(fieldAttachments) ? fieldAttachments : []).map((a) =>
      norm(a?.filename ?? a?.name)
    )
  );
  return (expectedFilenames || []).every((name) => inField.has(norm(name)));
}

/** Merge all CU attachment columns into one list for the Tab 13 UI. */
export function aggregateCuAttachmentsFromFields(cuFields) {
  if (!cuFields || typeof cuFields !== "object") return [];
  const out = [];
  const seen = new Set();
  for (const fieldName of CU_ATTACHMENT_AIRTABLE_FIELDS) {
    const raw = cuFields[fieldName];
    if (!Array.isArray(raw)) continue;
    for (const item of raw) {
      const norm = normalizeCuAttachmentItem(item);
      if (!norm || !norm.url || seen.has(norm.url)) continue;
      seen.add(norm.url);
      out.push(norm);
    }
  }
  return out;
}

export const LEASE_STRUCTURE_LINK_FIELD = process.env.AIRTABLE_DEALS_LINK_FIELD_LEASE_STRUCTURE || "Lease Structure";
export const LS_DEAL_LINK_FIELD = process.env.AIRTABLE_LEASE_STRUCTURE_DEAL_LINK_FIELD || "Deal_ID";

// ---------------------------------------------------------------------------
// Location & Property: form → Airtable column name
// Source of truth for which keys sync to the linked Location row (not Deals).
// Full inventory (all #dealForm fields → all 6 tables): npm run verify-deal-setup-routing
// Tota Site Size Unit: default is typo "Tota". If your base uses "Total Site Size Unit", set:
//   AIRTABLE_LOCATION_TOTAL_SITE_SIZE_UNIT_FIELD=Total Site Size Unit
// ---------------------------------------------------------------------------
const LOCATION_TOTAL_SITE_SIZE_UNIT_AIRTABLE = process.env.AIRTABLE_LOCATION_TOTAL_SITE_SIZE_UNIT_FIELD || "Tota Site Size Unit";

export const LOCATION_FORM_TO_AIRTABLE = {
  "Full Address": "Full Address",
  "City & State": "City",
  "Country": "Country",
  "Hotel Type": "Hotel Type",
  "Hotel Chain Scale": "Hotel Chain Scale",
  "Hotel Submarket & Location": "Hotel Submarket & Location",
  "Hotel Service Model": "Hotel Service Model",
  "Ownership/Brand History or Track Record": "Ownership/Brand History or Track Record",
  "Portfolio Size": "Portfolio Size",
  "Company Executive Summary": "Company Executive Summary",
  "Zoned for Hotel Development": "Zoned for Hotel Development",
  "Site/Development Restrictions?": "Site/Development Restrictions?",
  "Site/Development Restrictions Description": "Site Restrictions Describe",
  "Total Site Size": "Total Site Size",
  "Total Site Size Unit": LOCATION_TOTAL_SITE_SIZE_UNIT_AIRTABLE,
  "Max height Allowed By Zoning": "Max Height Allowed By Zoning",
  "Max Height Allowed By Zoning": "Max Height Allowed By Zoning",
  "Max height Unit": "Max Height Allowed By Zoning Unit",
  "Max Height Allowed By Zoning Unit": "Max Height Allowed By Zoning Unit",
  "Ownership Type": "Ownership Type",
  "Ownership Type Other Text": "Ownership Type Other Text",
  "Current Form of Site Control": "Current Form of Site Control",
  "Current Form of Site Control Other Text": "Current Form of Site Control Other Text",
  "Zoning Status": "Zoning Status",
  "Zoning Status Other Text": "Zoning Status Other Text",
  "Parking Ratio": "Parking Ratio",
  "Access to Transit or Highway": "Access to Transit / Highway",
  "Access to Transit or Highway Other Text": "Access to Transit / Highway Other Text",
  "Total Number of Rooms/Keys": "Total Number of Rooms/Keys",
  "Number of Standard Rooms": "Number of Standard Rooms",
  "Number of Suites": "Number of Suites",
  // Form uses "Number of Stories"; Airtable column is "# of Stories" in Location & Property
  "Number of Stories": "# of Stories",
  "# of Stories": "# of Stories",
  "Building Type": "Building Type",
  "Year Built (Years Open as a Hotel)": "Year Built (Years Open as a Hotel)",
  "PMS or Tech is in Place": "PMS or Tech is in Place",
  "Ceiling Heights": "Ceiling Heights",
  "Ceiling Heights Unit": "Ceiling Heights Unit",
  "Column Spacing": "Column Spacing",
  "Column Spacing Unit": "Column Spacing Unit",
  "Existing MEP Capacity (Conversion)": "Existing MEP Capacity (Conversion)",
  // "Amenities & Facilities" (HTML data-section="5"): F&B, meeting, parking, spa, sustainability, additional amenities → Deals
  "Micro-Location Type": "Micro-Location Type",
  "Demand Mix Targets": "Demand Mix Targets",
  "Operational Complexity Profile": "Operational Complexity Profile",
  "Primary Market Region": "Primary Market Region",
  [MIXED_USE_INTAKE_FIELD_NAMES.numberOfCondoUnits]: MIXED_USE_INTAKE_FIELD_NAMES.numberOfCondoUnits,
};

/** Form field names that belong to Location & Property (used to route and delete from deal fields). */
export const LOCATION_FORM_FIELDS = Object.keys(LOCATION_FORM_TO_AIRTABLE);

/** Location form keys stored as multi-select (array) in Airtable. */
export const LOCATION_MULTI_SELECT_FORM_KEYS = new Set([
  "Ownership Type",
  "Access to Transit or Highway",
  "Demand Mix Targets",
  "Operational Complexity Profile",
]);

/** Deals-only form keys (not in LOCATION/SI lists). */
export const DEALS_ONLY_FORM_FIELDS = new Set([
  "Property Name",
  "Project Type",
  "Stage of Development",
  "Expected Opening or Rebranding Date",
  "F&B Complexity",
  MIXED_USE_INTAKE_FIELD_NAMES.fbOperatingModel,
  "Opening Timeline",
  ...DEALS_OPERATOR_CAPABILITY_FORM_FIELDS,
  "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?",
  "Is the hotel currently branded?",
  "Is the hotel currently managed by a third-party operator?",
  "Are you open to considering other brands with favorable terms?",
  "Have you worked with any of your preferred brands/operators before?",
  "Operator Name Current",
  "Current Brand Affiliation",
  "Parent Company Name",
  "F&B Outlets?",
  "Meeting Space",
  "Number of Meeting Rooms",
  "Condo Residences?",
  "Hotel Rental Program?",
  "Parking Amenities?",
  "Additional Amenities",
]);

/**
 * expandedLocation (camelCase) key → form field name. Used to merge Location data into deal.fields
 * so GET and save-response rebind have form keys (Batch 2 Q2).
 */
export const LOCATION_EXPANDED_TO_FORM = {
  fullAddress: "Full Address",
  city: "City & State",
  country: "Country",
  hotelType: "Hotel Type",
  hotelChainScale: "Hotel Chain Scale",
  submarket: "Hotel Submarket & Location",
  hotelServiceModel: "Hotel Service Model",
  ownershipTrackRecord: "Ownership/Brand History or Track Record",
  portfolioSize: "Portfolio Size",
  companyExecutiveSummary: "Company Executive Summary",
  zonedForHotelDevelopment: "Zoned for Hotel Development",
  siteDevelopmentRestrictions: "Site/Development Restrictions?",
  siteDevelopmentRestrictionsDescription: "Site/Development Restrictions Description",
  totalSiteSize: "Total Site Size",
  totalSiteSizeUnit: "Total Site Size Unit",
  maxHeightAllowedByZoning: "Max height Allowed By Zoning",
  maxHeightAllowedByZoningUnit: "Max height Unit",
  ownershipType: "Ownership Type",
  ownershipTypeOtherText: "Ownership Type Other Text",
  currentFormOfSiteControl: "Current Form of Site Control",
  currentFormOfSiteControlOtherText: "Current Form of Site Control Other Text",
  zoningStatus: "Zoning Status",
  zoningStatusOtherText: "Zoning Status Other Text",
  parkingRatio: "Parking Ratio",
  accessToTransit: "Access to Transit or Highway",
  accessToTransitOtherText: "Access to Transit or Highway Other Text",
  totalNumberOfRoomsKeys: "Total Number of Rooms/Keys",
  numberStandardRooms: "Number of Standard Rooms",
  numberSuites: "Number of Suites",
  numberStories: "Number of Stories",
  buildingType: "Building Type",
  yearBuilt: "Year Built (Years Open as a Hotel)",
  pmsOrTech: "PMS or Tech is in Place",
  ceilingHeights: "Ceiling Heights",
  ceilingHeightsUnit: "Ceiling Heights Unit",
  columnSpacing: "Column Spacing",
  columnSpacingUnit: "Column Spacing Unit",
  existingMEPCapacity: "Existing MEP Capacity (Conversion)",
};

// ---------------------------------------------------------------------------
// Market - Performance
// ---------------------------------------------------------------------------
export const MARKET_PERFORMANCE_FIELD_NAMES = new Set([
  "Primary Demand Drivers",
  "Primary Demand Drivers Other",
  "Estimated or Actual RevPAR",
  "Regulatory or Permitting Issues?",
  "Regulatory or Permitting Issues Description",
  "Key Competitors",
  "Group vs Transient Mix",
  "Total Project Cost Range",
  "PIP Budget Range (if conversion)",
  "Equity vs Debt Split",
  "Ownership Structure",
  "Preferred Deal Structure",
  "PIP / CapEx Status",
  "Royalty Fee Expectations",
  "Marketing Fee Expectations",
  "Loyalty Fee Expectations",
  "Estimated Dev. Cost per Key (Room)",
  "Is the property encumbered",
  "Property Encumbered Description",
  "PIP / CapEx Status Amount",
  "PIP Cap Ex Status Timeline",
  "Is Financing Secured?",
  "Comfort Level with Upfront Investment",
  "Fee Tolerance Level",
  "Incentive Requirement Level",
  "Primary Incentive Type",
  "CapEx Tolerance Band",
  OAS_DEAL_MP_FIELD_NAMES.preferredOperatorManagementStructure,
  MIXED_USE_INTAKE_FIELD_NAMES.stabilizedAdrUsd,
  MIXED_USE_INTAKE_FIELD_NAMES.stabilizedOccupancyPct,
]);

/** Exact Airtable column names on "Market - Performance - Deal & Capital Structure" for the three fee-expectation selects.
 * Form `name` attributes stay "Royalty Fee Expectations", etc. If PATCH fails with UNKNOWN_FIELD_NAME, set env to your base’s exact field labels. */
export const MP_AIRTABLE_ROYALTY_FEE_EXPECTATIONS =
  (process.env.AIRTABLE_MP_ROYALTY_FEE_EXPECTATIONS_COLUMN || "").trim() || "Royalty Fee Expectations";
export const MP_AIRTABLE_MARKETING_FEE_EXPECTATIONS =
  (process.env.AIRTABLE_MP_MARKETING_FEE_EXPECTATIONS_COLUMN || "").trim() || "Marketing Fee Expectations";
export const MP_AIRTABLE_LOYALTY_FEE_EXPECTATIONS =
  (process.env.AIRTABLE_MP_LOYALTY_FEE_EXPECTATIONS_COLUMN || "").trim() || "Loyalty Fee Expectations";

export const MP_FORM_TO_TABLE = {
  "Group vs Transient Mix": "Group vs Transient Mix (If Known)",
  "Regulatory or Permitting Issues Description": "Regulatory or Permitting Issues Text",
  "Primary Demand Drivers Other": "Primary Demand Drivers Other Text",
  "PIP Budget Range (if conversion)": "PIP Budget Range (If Conversion)",
  "Royalty Fee Expectations": MP_AIRTABLE_ROYALTY_FEE_EXPECTATIONS,
  "Marketing Fee Expectations": MP_AIRTABLE_MARKETING_FEE_EXPECTATIONS,
  "Loyalty Fee Expectations": MP_AIRTABLE_LOYALTY_FEE_EXPECTATIONS,
};

export const MP_TABLE_TO_FORM = {
  "Group vs Transient Mix (If Known)": "Group vs Transient Mix",
  "Regulatory or Permitting Issues Text": "Regulatory or Permitting Issues Description",
  "Primary Demand Drivers Other Text": "Primary Demand Drivers Other",
  "PIP Budget Range (If Conversion)": "PIP Budget Range (if conversion)",
  ...(MP_AIRTABLE_ROYALTY_FEE_EXPECTATIONS !== "Royalty Fee Expectations"
    ? { [MP_AIRTABLE_ROYALTY_FEE_EXPECTATIONS]: "Royalty Fee Expectations" }
    : {}),
  ...(MP_AIRTABLE_MARKETING_FEE_EXPECTATIONS !== "Marketing Fee Expectations"
    ? { [MP_AIRTABLE_MARKETING_FEE_EXPECTATIONS]: "Marketing Fee Expectations" }
    : {}),
  ...(MP_AIRTABLE_LOYALTY_FEE_EXPECTATIONS !== "Loyalty Fee Expectations"
    ? { [MP_AIRTABLE_LOYALTY_FEE_EXPECTATIONS]: "Loyalty Fee Expectations" }
    : {}),
};

// ---------------------------------------------------------------------------
// Strategic Intent
// ---------------------------------------------------------------------------
export const STRATEGIC_INTENT_FORM_FIELDS = [
  "Soft vs Hard Brand Preference",
  "Preferred Chain Scales",
  "Open to Soft Brand First Then Reflag?",
  "Target Guest Segment",
  "Target Guest Segment Other",
  "Brand Flexibility vs Prestige",
  "IRR/Yield Goals",
  "Open to Outside Capital or Partnerships?",
  "Preferred Brands (up to 4)",
  "Planned Hold Period",
  "Primary Goal for the Hotel",
  "Primary Goal for the Hotel Other",
  "Strategy Type",
  "Brand Role Intent",
  "Decision Horizon",
  "Owner Control Priorities",
  "Contract Flexibility Priorities",
  "Plan to Self-Manage or Hire Third Party?",
  "Preferred Future Operating Model",
  "Operator Strategy Status",
  "Operator Review Status",
  "Preferred Management Structure",
  OAS_DEAL_SI_FIELD_NAMES.operatorStructureIntent,
  "Required Operator Services",
  "Must-Have Operator Services",
  "Nice-to-Have Operator Services",
  "Market Presence Requirement",
  "Pre-Opening Support Needed",
  "Owner Reporting Expectations",
  "Brand / Operator Responsibility Split",
  "Owner Control Preference",
  "Brand Agreement Structure",
  "Operating Model",
  "Operator Scope",
  "Commercial Priority",
  "Local Labor / HR Support Needed",
  "Procurement Support Needed",
  "Owner Internal Ops Capability",
  "Operator Capability Priorities",
  "Owner Reporting Package",
  "Owner Reporting Frequency",
  "Who should receive bids for this project?",
  "Minimum Operator Experience (years)",
  "Preferred Third-Party Operators (names)",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Other Operator Criteria or Notes",
  "Level of Involvement in Day-to-Day Ops",
  "Preferred Reporting Frequency",
  "On-Site vs Remote Owner Representation",
  "Speed to Market Importance",
  "Development / Renovation Timeline Importance",
  "CapEx / PIP Execution Importance",
  "Revenue / Yield Management Importance",
  "Marketing & Distribution Importance",
  "Loyalty Program Importance",
  "Brand Recognition Importance",
  "Brand Equity Increase on Exit Importance",
  "Guest Experience / Satisfaction Importance",
  "Cost Control / Operational Efficiency Importance",
  "Staffing & Talent Importance",
  "Technology & Systems Importance",
  "Incentive Alignment Importance",
  "ESG / Sustainability Importance",
  "Top 3 Success Metrics",
  "Top 3 Success Metrics Other",
  "Top Priorities for Project",
  "Top Priorities for Project Other",
  "Top Concerns for this Project",
  "Top Concerns for this Project Other",
  "Decision Timeline for Brand/Operator",
  "Critical deadlines for application",
  "Critical Deadlines Description",
  "Top 3 Deal Breakers",
  "Top 3 Deal Breakers Other",
  "Must-haves From Brand or Operator",
  "Must-haves From Brand or Operator Other",
  "Incentive Types Interested In",
  "Incentive Types Interested In Other",
  MIXED_USE_INTAKE_FIELD_NAMES.brandedResidenceProgramModel,
  MIXED_USE_INTAKE_FIELD_NAMES.condoRentalProgramModel,
];

export const SI_FORM_TO_AIRTABLE = {
  "Preferred Brands (up to 4)": "Preferred Brands",
  "Open to Soft Brand First Then Reflag?": "Open to Soft Brand First, Then Reflag?",
  "Primary Goal for the Hotel Other": "Primary Goal for the Hotel Other Text",
  "Target Guest Segment Other": "Target Guest Segment Other Text",
  "Who should receive bids for this project?": "Who Should Receive Bids for This Project?",
  "Minimum Operator Experience (years)": "Minimum Operator Experience (Years)",
  "Preferred Third-Party Operators (names)": "Preferred Third-Party Operators (Names)",
  "Speed to Market Importance": "Speed to Market",
  "Development / Renovation Timeline Importance": "Development / Renovation Timeline",
  "CapEx / PIP Execution Importance": "CapEx / PIP Execution",
  "Revenue / Yield Management Importance": "Revenue / Yield Management",
  "Marketing & Distribution Importance": "Marketing & Distribution",
  "Loyalty Program Importance": "Loyalty Program",
  "Brand Recognition Importance": "Brand Recognition",
  "Brand Equity Increase on Exit Importance": "Brand Equity Increase on Exit",
  "Guest Experience / Satisfaction Importance": "Guest Experience / Satisfaction",
  "Cost Control / Operational Efficiency Importance": "Cost Control / Operational Efficiency",
  "Staffing & Talent Importance": "Staffing & Talent",
  "Technology & Systems Importance": "Technology & Systems",
  "Incentive Alignment Importance": "Incentive Alignment",
  "ESG / Sustainability Importance": "ESG / Sustainability",
  "Critical deadlines for application": "Critical Deadlines",
  "Critical Deadlines Description": "Critical Deadlines Text",
  "Must-haves From Brand or Operator": "Must-Haves From Brand/Operator",
  "Must-haves From Brand or Operator Other": "Must-Haves From Brand/Operator Other Text",
  "Incentive Types Interested In Other": "Incentive Types Interested In Other Text",
};

/** Strategic Intent fields stored as multi-select in Airtable. */
export const SI_MULTI_SELECT_FORM_KEYS = new Set([
  "Owner Control Priorities",
  "Contract Flexibility Priorities",
  "Operator Capability Priorities",
  "Owner Reporting Package",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Preferred Management Structure",
  "Required Operator Services",
  "Must-Have Operator Services",
  "Nice-to-Have Operator Services",
  "Operator Scope",
  "Commercial Priority",
  "Top 3 Success Metrics",
  "Top Priorities for Project",
  "Top Concerns for this Project",
  "Top 3 Deal Breakers",
  "Must-haves From Brand or Operator",
  "Incentive Types Interested In",
  "Preferred Chain Scales",
  "Preferred Brands (up to 4)",
  "Target Guest Segment",
]);

export const SI_AIRTABLE_TO_FORM = {
  "Preferred Brands": "Preferred Brands (up to 4)",
  "Open to Soft Brand First, Then Reflag?": "Open to Soft Brand First Then Reflag?",
  "Primary Goal for the Hotel Other Text": "Primary Goal for the Hotel Other",
  "Target Guest Segment Other Text": "Target Guest Segment Other",
  "Who Should Receive Bids for This Project?": "Who should receive bids for this project?",
  "Minimum Operator Experience (Years)": "Minimum Operator Experience (years)",
  "Preferred Third-Party Operators (Names)": "Preferred Third-Party Operators (names)",
  "Speed to Market": "Speed to Market Importance",
  "Development / Renovation Timeline": "Development / Renovation Timeline Importance",
  "CapEx / PIP Execution": "CapEx / PIP Execution Importance",
  "Revenue / Yield Management": "Revenue / Yield Management Importance",
  "Marketing & Distribution": "Marketing & Distribution Importance",
  "Loyalty Program": "Loyalty Program Importance",
  "Brand Recognition": "Brand Recognition Importance",
  "Brand Equity Increase on Exit": "Brand Equity Increase on Exit Importance",
  "Guest Experience / Satisfaction": "Guest Experience / Satisfaction Importance",
  "Cost Control / Operational Efficiency": "Cost Control / Operational Efficiency Importance",
  "Staffing & Talent": "Staffing & Talent Importance",
  "Technology & Systems": "Technology & Systems Importance",
  "Incentive Alignment": "Incentive Alignment Importance",
  "ESG / Sustainability": "ESG / Sustainability Importance",
  "Critical Deadlines": "Critical deadlines for application",
  "Critical Deadlines Text": "Critical Deadlines Description",
  "Must-Haves From Brand/Operator": "Must-haves From Brand or Operator",
  "Must-Haves From Brand/Operator Other Text": "Must-haves From Brand or Operator Other",
  "Incentive Types Interested In Other Text": "Incentive Types Interested In Other"
};

// ---------------------------------------------------------------------------
// Contact & Uploads
// ---------------------------------------------------------------------------
export const CONTACT_UPLOADS_FORM_FIELDS = [
  "Would you like to filter out brands without key money?",
  "Would you like to meet consultants?",
  "Legal Support Needed?",
  "Financial Model Available?",
  MIXED_USE_INTAKE_FIELD_NAMES.developmentProformaAvailable,
  "Proposal Deadline",
  "Would you like to receive regular updates?",
  "Working with Broker/Advisor?",
  "Broker/Advisor Company and Contract Details",
  "Other Projects Nearing Contract Expiration?",
  "Contact Source",
  "Main Contact Name",
  "Main Contact Title",
  "Entity or Company Name",
  "Company HQ Location",
  "Email Address",
  "Secondary Contact",
  "Best Time or Method to Reach",
  "What makes this opportunity stand out to a brand or operator?",
  "Additional Notes or Unique Project Aspects",
  "Anything else you'd like to add?",
  CU_ATTACHMENT_FORM_KEY,
];

export const CU_FORM_TO_AIRTABLE = {
  "Would you like to filter out brands without key money?": "Would You Like to Filter Out Brands Without Key Money?",
  "Would you like to meet consultants?": "Would You Like to Meet Consultants?",
  "Would you like to receive regular updates?": "Would You Like to Receive Regular Updates?",
  /** Live split: contract/details text; firm name uses `Broker/Firm Name` when form adds separate input. */
  "Broker/Advisor Company and Contract Details": "Working with Broker/Advisor? Text",
};

// ---------------------------------------------------------------------------
// Lease Structure
// ---------------------------------------------------------------------------
export const LEASE_STRUCTURE_FORM_FIELDS = [
  "Lease Type",
  "Initial Lease Term (years)",
  "Lease Start Date (or Availability)",
  "Lease Expiration or End Date",
  "Base Rent (annual or structure)",
  "Percentage Rent (if applicable)",
  "CAM Insurance Tax Responsibility",
  "Key Money or TI Allowance",
  "Renewal Options",
  "Early Termination or Break Clause",
  "Security Deposit or Guarantees",
  "Lease Structure Notes",
];

/**
 * True when Deal Setup shows the Lease Structure section (same as new-deal-setup isLeaseStructureVisible).
 * Preferred Deal Structure is stored on Market–Performance but merged onto deal.fields for reads/review.
 */
export function isLeaseStructureDealApplicableFromMergedFields(fields) {
  if (!fields || typeof fields !== "object") return false;
  const raw = fields["Preferred Deal Structure"];
  const v =
    typeof raw === "string"
      ? raw.trim()
      : raw != null && typeof raw === "object" && typeof raw.name === "string"
        ? String(raw.name).trim()
        : raw != null && typeof raw !== "object"
          ? String(raw).trim()
          : "";
  return v === "Lease" || v === "Flexible/Open";
}

export const LS_FORM_TO_AIRTABLE = {
  "Initial Lease Term (years)": "Initial Lease Term (Years)",
  "Lease Expiration or End Date": "Lease Expiration / End Date",
  "Base Rent (annual or structure)": "Base Rent (Annual or Structure)",
  "CAM Insurance Tax Responsibility": "CAM / Insurance / Tax Responsibility",
  "Key Money or TI Allowance": "Key Money / TI Allowance",
  "Early Termination or Break Clause": "Early Termination / Break Clause",
  "Security Deposit or Guarantees": "Security Deposit / Guarantees",
};

export const LS_AIRTABLE_TO_FORM = {
  "Initial Lease Term (Years)": "Initial Lease Term (years)",
  "Lease Expiration / End Date": "Lease Expiration or End Date",
  "Base Rent (Annual or Structure)": "Base Rent (annual or structure)",
  "CAM / Insurance / Tax Responsibility": "CAM Insurance Tax Responsibility",
  "Key Money / TI Allowance": "Key Money or TI Allowance",
  "Early Termination / Break Clause": "Early Termination or Break Clause",
  "Security Deposit / Guarantees": "Security Deposit or Guarantees",
};

// ---------------------------------------------------------------------------
// Deal Setup PATCH: which Airtable table each form `name` routes to (see updateMyDealById).
// HTML #dealForm: data-section 3 = Location & Site through Operational Complexity → Location;
// data-section 4 = Property Specs (keys, building, MEP) → Location; data-section 5 = Amenities & Facilities → Deals.
// ---------------------------------------------------------------------------
export const DEAL_SETUP_AIRTABLE_TABLE_NAMES = {
  DEALS: "Deals",
  LOCATION: "Location & Property",
  MARKET_PERFORMANCE: "Market - Performance - Deal & Capital Structure",
  LEASE: "Lease Structure",
  STRATEGIC_INTENT: "Strategic Intent - Operational - Key Challenges",
  CONTACT_UPLOADS: "Contact & Uploads",
};

const _DEAL_SETUP_LOC_KEYS = new Set(LOCATION_FORM_FIELDS);
const _DEAL_SETUP_SI_KEYS = new Set(STRATEGIC_INTENT_FORM_FIELDS);
const _DEAL_SETUP_CU_KEYS = new Set(CONTACT_UPLOADS_FORM_FIELDS);
const _DEAL_SETUP_LS_KEYS = new Set(LEASE_STRUCTURE_FORM_FIELDS);
const _DEAL_SETUP_DEALS_ONLY_KEYS = DEALS_ONLY_FORM_FIELDS;

/** Target Airtable table for a Deal Setup form field name (same routing as PATCH). */
export function classifyDealSetupFormField(fieldName) {
  if (_DEAL_SETUP_LOC_KEYS.has(fieldName)) return DEAL_SETUP_AIRTABLE_TABLE_NAMES.LOCATION;
  if (MARKET_PERFORMANCE_FIELD_NAMES.has(fieldName)) return DEAL_SETUP_AIRTABLE_TABLE_NAMES.MARKET_PERFORMANCE;
  if (_DEAL_SETUP_SI_KEYS.has(fieldName)) return DEAL_SETUP_AIRTABLE_TABLE_NAMES.STRATEGIC_INTENT;
  if (_DEAL_SETUP_CU_KEYS.has(fieldName)) return DEAL_SETUP_AIRTABLE_TABLE_NAMES.CONTACT_UPLOADS;
  if (_DEAL_SETUP_LS_KEYS.has(fieldName)) return DEAL_SETUP_AIRTABLE_TABLE_NAMES.LEASE;
  if (_DEAL_SETUP_DEALS_ONLY_KEYS.has(fieldName)) return DEAL_SETUP_AIRTABLE_TABLE_NAMES.DEALS;
  return DEAL_SETUP_AIRTABLE_TABLE_NAMES.DEALS;
}

/**
 * Conditional required fields for Operator Capability P0 (when operator path is in scope).
 * @param {Record<string, unknown>} fields — merged deal fields
 * @returns {string[]}
 */
export function operatorCapabilityConditionalRequiredFields(fields) {
  if (!isOperatorInScopeFromFields(fields)) return [];
  const out = [
    "Current Operating Model",
    "Preferred Future Operating Model",
    "Primary Market Region",
    "Opening / Transition Phase",
    "Operator Strategy Status",
    "Operator Capability Priorities",
  ];
  const preferred = String(
    fields["Preferred Future Operating Model"] ||
      fields["Plan to Self-Manage or Hire Third Party?"] ||
      ""
  );
  if (/third.party|brand \+ third/i.test(preferred)) {
    out.push("Owner Reporting Frequency");
  }
  return out;
}

// ---------------------------------------------------------------------------
// Required fields by section (M1 source of truth; section 7 = Lease, skip when Lease hidden)
// ---------------------------------------------------------------------------
export const REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION = {
  0: [
    "Property Name",
    "Project Type",
    "Stage of Development",
    "Opening / Transition Phase",
  ],
  1: [
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?",
    "Is the hotel currently branded?",
    "Is the hotel currently managed by a third-party operator?",
    "Current Operating Model",
    "Are you open to considering other brands with favorable terms?",
    "Have you worked with any of your preferred brands/operators before?",
  ],
  2: [
    "Full Address",
    "Country",
    "Hotel Chain Scale",
    "Hotel Type",
    "Hotel Submarket & Location",
    "Hotel Service Model",
    "Primary Market Region",
    "Ownership/Brand History or Track Record",
    "Zoned for Hotel Development",
    "Site/Development Restrictions?",
    "Total Site Size",
    "Total Site Size Unit",
    "Max height Allowed By Zoning",
    "Max height Unit",
    "Current Form of Site Control",
  ],
  3: ["Total Number of Rooms/Keys", "Number of Standard Rooms", "Number of Suites", "Building Type", "Number of Stories"],
  4: ["F&B Outlets?", "Meeting Space", "Number of Meeting Rooms", "Condo Residences?", "Hotel Rental Program?", "Parking Amenities?", "Additional Amenities"],
  5: [
    "Estimated or Actual RevPAR",
    "Regulatory or Permitting Issues?",
    "Key Competitors",
  ],
  6: [
    "Total Project Cost Range",
    "PIP Budget Range (if conversion)",
    "Equity vs Debt Split",
    "Ownership Structure",
    "Preferred Deal Structure",
    "PIP / CapEx Status",
  ],
  7: ["Lease Type"],
  8: [
    "Soft vs Hard Brand Preference",
    "Preferred Brands (up to 4)",
    "IRR/Yield Goals",
    "Open to Outside Capital or Partnerships?",
    "Plan to Self-Manage or Hire Third Party?",
    "Preferred Future Operating Model",
    "Preferred Chain Scales",
    "Open to Soft Brand First Then Reflag?",
    "Target Guest Segment",
    "Brand Flexibility vs Prestige",
    "Planned Hold Period",
    "Primary Goal for the Hotel",
    "Who should receive bids for this project?",
    "Operator Strategy Status",
    "Minimum Operator Experience (years)",
    "Preferred Third-Party Operators (names)",
    "Preferred Third-Party Operator Profile",
    "Services Required From Operator",
    "Operator Capability Priorities",
    "Other Operator Criteria or Notes",
    "Level of Involvement in Day-to-Day Ops",
  ],
  9: ["Top Priorities for Project", "Top Concerns for this Project", "Top 3 Success Metrics", "Top 3 Deal Breakers", "Must-haves From Brand or Operator", "Decision Timeline for Brand/Operator"],
  10: [],
  11: [
    "Would you like to filter out brands without key money?",
    "Would you like to meet consultants?",
    "Legal Support Needed?",
    "Financial Model Available?",
    "Proposal Deadline",
    "Would you like to receive regular updates?",
    "Working with Broker/Advisor?",
    "Other Projects Nearing Contract Expiration?",
  ],
  12: ["Main Contact Name", "Entity or Company Name", "Company HQ Location", "Email Address"],
  13: [],
};
