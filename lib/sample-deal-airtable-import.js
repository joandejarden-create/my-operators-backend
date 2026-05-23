/**
 * Build Airtable import bundles from CALA sample-deal fixtures.
 * Routing mirrors Deal Setup PATCH (api/schemas/deal-setup-fields.js + api/my-deals.js).
 */
import {
  classifyDealSetupFormField,
  DEAL_SETUP_AIRTABLE_TABLE_NAMES,
  DEALS_FORM_TO_AIRTABLE,
  DEALS_STATUS_FIELD,
  LOCATION_FORM_TO_AIRTABLE,
  LOCATION_MULTI_SELECT_FORM_KEYS,
  MARKET_PERFORMANCE_FIELD_NAMES,
  MP_FORM_TO_TABLE,
  STRATEGIC_INTENT_FORM_FIELDS,
  SI_FORM_TO_AIRTABLE,
  CONTACT_UPLOADS_FORM_FIELDS,
  CU_FORM_TO_AIRTABLE,
  LEASE_STRUCTURE_FORM_FIELDS,
  LS_FORM_TO_AIRTABLE,
  DEALS_TABLE,
  LOCATION_PROPERTY_TABLE,
  MARKET_PERFORMANCE_TABLE,
  STRATEGIC_INTENT_TABLE,
  CONTACT_UPLOADS_TABLE,
  LEASE_STRUCTURE_TABLE,
  LOCATION_LINK_FIELD,
  MARKET_PERFORMANCE_LINK_FIELD,
  STRATEGIC_INTENT_LINK_FIELD,
  CONTACT_UPLOADS_LINK_FIELD,
  MP_DEAL_LINK_FIELD,
  CU_DEAL_LINK_FIELD,
  LS_DEAL_LINK_FIELD,
  LEASE_STRUCTURE_LINK_FIELD,
} from "../api/schemas/deal-setup-fields.js";
import { flattenSampleDealToIntakeFields, SAMPLE_DISCLAIMER } from "./sample-opportunity-deal-schema.js";
import { normalizeDealSetupFields } from "./deal-setup-form-value-normalize.js";
import { sanitizeDemoIntakeFields } from "./demo-intake-copy-sanitize.js";

const TARGET_LIST_TABLE = process.env.AIRTABLE_TABLE_TARGET_LIST || "Target List";
const TARGET_LIST_NOTES_FIELD = process.env.AIRTABLE_TARGET_LIST_NOTES_FIELD || "Notes";

/** Form fields omitted when base schema lacks the column (CALA seed safety). */
const IMPORT_SKIP_FORM_FIELDS = new Set([
  "Site/Development Restrictions Description",
  "Contact Source",
  "Main Contact Title",
  "Secondary Contact",
  "Best Time or Method to Reach",
  "What makes this opportunity stand out to a brand or operator?",
  "Additional Notes or Unique Project Aspects",
  "Anything else you'd like to add?",
  "Would you like to meet consultants?",
  "Other Projects Nearing Contract Expiration?",
  "Broker/Advisor Company and Contract Details",
]);

const SI_MULTI_ARRAY_FIELDS = new Set([
  "Preferred Brands (up to 4)",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Top 3 Success Metrics",
  "Top Priorities for Project",
  "Top Concerns for this Project",
  "Top 3 Deal Breakers",
  "Must-haves From Brand or Operator",
  "Incentive Types Interested In",
  "Operator Capability Priorities",
]);

const LOCATION_NUMERIC_KEYS = new Set([
  "Total Number of Rooms/Keys",
  "Number of Standard Rooms",
  "Number of Suites",
  "# of Stories",
  "Number of Stories",
]);

const MP_MULTI_KEYS = new Set(["Primary Demand Drivers", "Is the property encumbered"]);

/** SI columns that differ from form keys (extends SI_FORM_TO_AIRTABLE). */
const SI_FORM_TO_AIRTABLE_IMPORT = {
  ...SI_FORM_TO_AIRTABLE,
  "Top 3 Deal Breakers Other": "Top 3 Deal Breakers Other Text",
  "Top 3 Success Metrics Other": "Top 3 Success Metrics Other Text",
  "Top Priorities for Project Other": "Top Priorities for Project Other Text",
  "Top Concerns for this Project Other": "Top Concerns for this Project Other Text",
};

/**
 * Merge reference + fictional intake fields (fictional wins).
 * @param {object} record
 */
export function mergeSampleDealIntakeFields(record) {
  const ref = record.referenceProperty?.fields || {};
  const fic = record.fictionalDeal?.fields || {};
  const merged = { ...ref };
  for (const [k, v] of Object.entries(fic)) {
    if (v !== undefined && v !== "") merged[k] = v;
  }
  const { fields } = normalizeDealSetupFields(merged);
  return fields;
}

/**
 * @param {string} formName
 * @param {unknown} val
 * @param {string} table
 */
function transformFieldValue(formName, val, table) {
  if (val === undefined || val === null) return undefined;
  if (typeof val === "string" && val.trim() === "") return undefined;

  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.LOCATION) {
    if (LOCATION_NUMERIC_KEYS.has(formName)) {
      const num = typeof val === "number" ? val : parseInt(String(val).trim(), 10);
      return Number.isNaN(num) ? undefined : num;
    }
    if (LOCATION_MULTI_SELECT_FORM_KEYS.has(formName)) {
      return Array.isArray(val)
        ? val.map((v) => String(v).trim()).filter(Boolean)
        : String(val)
            .split(/\s*,\s*/)
            .map((s) => s.trim())
            .filter(Boolean);
    }
  }

  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.MARKET_PERFORMANCE) {
    if (MP_MULTI_KEYS.has(formName)) {
      return Array.isArray(val)
        ? val.map((v) => String(v).trim()).filter(Boolean)
        : String(val)
            .split(/\s*,\s*/)
            .map((s) => s.trim())
            .filter(Boolean);
    }
    if (typeof val === "string") return val.replace(/\u2013/g, "-").trim();
  }

  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.STRATEGIC_INTENT) {
    if (formName === "Preferred Chain Scales") {
      const s = Array.isArray(val) ? String(val[0] ?? "").trim() : String(val).trim();
      return s || undefined;
    }
    if (SI_MULTI_ARRAY_FIELDS.has(formName)) {
      return Array.isArray(val)
        ? val.map((v) => String(v).trim()).filter(Boolean)
        : String(val)
            .split(/\s*,\s*/)
            .map((s) => s.trim())
            .filter(Boolean);
    }
  }

  if (Array.isArray(val)) return val;
  if (typeof val === "number") return val;
  return typeof val === "string" ? val.trim() : String(val);
}

/**
 * Map form field name → Airtable column for a table.
 * @param {string} formName
 * @param {string} table
 */
function airtableColumnForField(formName, table) {
  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.LOCATION) {
    return LOCATION_FORM_TO_AIRTABLE[formName] ?? formName;
  }
  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.MARKET_PERFORMANCE) {
    return MP_FORM_TO_TABLE[formName] ?? formName;
  }
  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.STRATEGIC_INTENT) {
    return SI_FORM_TO_AIRTABLE_IMPORT[formName] ?? formName;
  }
  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.CONTACT_UPLOADS) {
    return CU_FORM_TO_AIRTABLE[formName] ?? formName;
  }
  if (table === DEAL_SETUP_AIRTABLE_TABLE_NAMES.LEASE) {
    return LS_FORM_TO_AIRTABLE[formName] ?? formName;
  }
  return DEALS_FORM_TO_AIRTABLE[formName] ?? formName;
}

/**
 * @param {object} record — sample-deal fixture
 * @param {{ sampleDealStatus?: string }} [opts]
 */
export function buildSampleDealImportBundle(record, opts = {}) {
  const formFields = mergeSampleDealIntakeFields(record);
  const tables = {
    [DEALS_TABLE]: {},
    [LOCATION_PROPERTY_TABLE]: {},
    [MARKET_PERFORMANCE_TABLE]: {},
    [STRATEGIC_INTENT_TABLE]: {},
    [CONTACT_UPLOADS_TABLE]: {},
    [LEASE_STRUCTURE_TABLE]: {},
  };

  for (const [formName, rawVal] of Object.entries(formFields)) {
    if (IMPORT_SKIP_FORM_FIELDS.has(formName)) continue;
    const table = classifyDealSetupFormField(formName);
    const val = transformFieldValue(formName, rawVal, table);
    if (val === undefined) continue;
    const col = airtableColumnForField(formName, table);
    tables[table][col] = val;
  }

  const projectName =
    formFields["Project Name"] || formFields["Property Name"] || record.fictionalDeal?.projectName;
  if (projectName) {
    tables[DEALS_TABLE]["Project Name"] = projectName;
    tables[DEALS_TABLE]["Property Name"] = projectName;
  }

  tables[DEALS_TABLE][DEALS_STATUS_FIELD] =
    opts.sampleDealStatus || process.env.CALA_SAMPLE_DEALS_STATUS || "In Review";

  const targetList = (record.targetListRows || []).map((row) => ({
    table: TARGET_LIST_TABLE,
    fields: {
      "Brand Name": row.brandName,
      Status: "Considering",
      [TARGET_LIST_NOTES_FIELD]: [row.whyInReviewSet, row.notes || ""].filter(Boolean).join(" | "),
    },
    meta: {
      parentCompany: row.parentCompany,
      sourceType: row.sourceType,
      reviewSetSource: row.reviewSetSource,
    },
  }));

  const slug = (record.meta?.sampleId || "sample")
    .replace(/^cala-/, "")
    .replace(/-00\d$/, "");

  return {
    version: 1,
    generatedAt: new Date().toISOString(),
    fixtureFile: opts.fixtureFile || null,
    sampleId: record.meta?.sampleId,
    sampleSlug: slug,
    projectName,
    disclaimer: record.disclaimer || "",
    expectedReadinessStage: record.expectedReadinessStage || record.meta?.expectedReadinessStage,
    expectedBrandAlignmentBehavior: record.expectedBrandAlignmentBehavior || null,
    intentionalGaps: record.intentionalGaps || [],
    referenceProperty: {
      displayLabel: record.referenceProperty?.displayLabel,
      publicName: record.referenceProperty?.publicName,
      secondaryReferenceHotels: record.referenceProperty?.secondaryReferenceHotels,
      sources: record.referenceProperty?.sources,
    },
    tables: {
      deals: {
        table: DEALS_TABLE,
        fields: tables[DEALS_TABLE],
        fieldCount: Object.keys(tables[DEALS_TABLE]).length,
      },
      locationProperty: {
        table: LOCATION_PROPERTY_TABLE,
        fields: tables[LOCATION_PROPERTY_TABLE],
        fieldCount: Object.keys(tables[LOCATION_PROPERTY_TABLE]).length,
        dealLinkField: "Deal_ID",
      },
      marketPerformance: {
        table: MARKET_PERFORMANCE_TABLE,
        fields: tables[MARKET_PERFORMANCE_TABLE],
        fieldCount: Object.keys(tables[MARKET_PERFORMANCE_TABLE]).length,
        dealLinkField: MP_DEAL_LINK_FIELD,
      },
      strategicIntent: {
        table: STRATEGIC_INTENT_TABLE,
        fields: tables[STRATEGIC_INTENT_TABLE],
        fieldCount: Object.keys(tables[STRATEGIC_INTENT_TABLE]).length,
        linkFromDealsField: STRATEGIC_INTENT_LINK_FIELD,
      },
      contactUploads: {
        table: CONTACT_UPLOADS_TABLE,
        fields: tables[CONTACT_UPLOADS_TABLE],
        fieldCount: Object.keys(tables[CONTACT_UPLOADS_TABLE]).length,
        dealLinkField: CU_DEAL_LINK_FIELD,
      },
      leaseStructure: {
        table: LEASE_STRUCTURE_TABLE,
        fields: tables[LEASE_STRUCTURE_TABLE],
        fieldCount: Object.keys(tables[LEASE_STRUCTURE_TABLE]).length,
        dealLinkField: LS_DEAL_LINK_FIELD,
      },
    },
    targetList,
    applyOrder: [
      "deals",
      "locationProperty",
      "marketPerformance",
      "strategicIntent",
      "contactUploads",
      "leaseStructure",
      "targetList",
    ],
    dealLinksAfterCreate: {
      [LOCATION_LINK_FIELD]: "locationProperty",
      [MARKET_PERFORMANCE_LINK_FIELD]: "marketPerformance",
      [STRATEGIC_INTENT_LINK_FIELD]: "strategicIntent",
      [CONTACT_UPLOADS_LINK_FIELD]: "contactUploads",
      [LEASE_STRUCTURE_LINK_FIELD]: "leaseStructure",
    },
  };
}

/**
 * @param {object} bundle
 */
export function bundleFieldStats(bundle) {
  const t = bundle.tables;
  return {
    deals: t.deals.fieldCount,
    location: t.locationProperty.fieldCount,
    market: t.marketPerformance.fieldCount,
    strategicIntent: t.strategicIntent.fieldCount,
    contact: t.contactUploads.fieldCount,
    lease: t.leaseStructure.fieldCount,
    targetList: bundle.targetList.length,
  };
}
