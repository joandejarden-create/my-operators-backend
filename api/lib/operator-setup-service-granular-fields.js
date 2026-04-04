/**
 * Operator Setup “Support & Services” groups: the form submits the same multi-select arrays as My Brands
 * (`select[multiple]` per category).
 *
 * **Storage (two supported modes):**
 * - **Aggregate multi-select (recommended):** eight Airtable `multipleSelects` columns on
 *   `3rd Party Operator - Service Offerings` (`Revenue Management Services`, …), plus eight **long-text**
 *   columns for free-form “Other” details (`Revenue Management Other`, …).
 * - **Legacy Service Offerings:** per-option checkbox columns — opt-in with `OPERATOR_SERVICE_GRANULAR_CHECKBOX_WRITES=1`.
 *
 * **Governance (new base)** uses the same `* Other` long-text column titles as Service Offerings, plus per-option
 * checkbox columns via `buildGovernanceGranularAirtableFields` (separate table).
 */

import { parseMultiValue } from "./third-party-operator-value-utils.js";

function normLabel(s) {
  return String(s ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function optionPrefixFromGranularColumn(airtableCol) {
  const parts = String(airtableCol).split(" - ");
  if (parts.length < 2) return "";
  return parts.slice(0, -1).join(" - ");
}

function selectionMatchesColumn(selectedValue, emit, airtableCol) {
  const n = normLabel(selectedValue);
  if (n === normLabel(emit)) return true;
  const prefix = optionPrefixFromGranularColumn(airtableCol);
  if (prefix && n === normLabel(prefix)) return true;
  return false;
}

/**
 * @typedef {{ col: string, emit: string }} GranularServiceColumn
 * @typedef {{
 *   aggregate: string,
 *   arrayBodyKey: string,
 *   otherBodyKey: string,
 *   otherTextField: string,
 *   columns: GranularServiceColumn[],
 * }} ServiceCategoryGranular
 */

/** Aligns with My Brands `brand-setup.html` multi-select option values (`OPERATIONAL_SUPPORT_SERVICE_COLUMNS` in `api/brand-library.js`). */
export const OPERATOR_SERVICE_GRANULAR = /** @type {ServiceCategoryGranular[]} */ ([
  {
    aggregate: "Revenue Management Services",
    arrayBodyKey: "revenueManagementServices",
    otherBodyKey: "revenueManagementOther",
    otherTextField: "Revenue Management Other",
    columns: [
      { col: "In-House Revenue Management Team - Revenue Management Services", emit: "In-House Revenue Management Team" },
      { col: "Outsourced Revenue Management - Revenue Management Services", emit: "Outsourced Revenue Management" },
      { col: "Dedicated Revenue Manager Per Property - Revenue Management Services", emit: "Dedicated Revenue Manager Per Property" },
      { col: "Regional Revenue Management Support - Revenue Management Services", emit: "Regional Revenue Management Support" },
      { col: "Advanced Analytics and Forecasting - Revenue Management Services", emit: "Advanced Analytics and Forecasting" },
      { col: "Dynamic Pricing Optimization - Revenue Management Services", emit: "Dynamic Pricing Optimization" },
      { col: "Market Intelligence and Benchmarking - Revenue Management Services", emit: "Market Intelligence and Benchmarking" },
      { col: "Other - Revenue Management Services", emit: "Other" },
    ],
  },
  {
    aggregate: "Sales Marketing Support",
    arrayBodyKey: "salesMarketingSupport",
    otherBodyKey: "salesMarketingOther",
    otherTextField: "Sales Marketing Other",
    columns: [
      { col: "Dedicated Sales Team - Sales & Marketing Support", emit: "Dedicated Sales Team" },
      { col: "Group Sales Support - Sales & Marketing Support", emit: "Group Sales Support" },
      { col: "Corporate Sales Support - Sales & Marketing Support", emit: "Corporate Sales Support" },
      { col: "Digital Marketing Services - Sales & Marketing Support", emit: "Digital Marketing Services" },
      { col: "Social Media Management - Sales & Marketing Support", emit: "Social Media Management" },
      { col: "Brand Marketing Support - Sales & Marketing Support", emit: "Brand Marketing Support" },
      { col: "Local Marketing Programs - Sales & Marketing Support", emit: "Local Marketing Programs" },
      { col: "SEO and Online Presence - Sales & Marketing Support", emit: "SEO and Online Presence" },
      { col: "Other - Sales & Marketing Support", emit: "Other" },
    ],
  },
  {
    aggregate: "Accounting Reporting",
    arrayBodyKey: "accountingReporting",
    otherBodyKey: "accountingReportingOther",
    otherTextField: "Accounting Reporting Other",
    columns: [
      { col: "Daily Financial Reporting - Accounting & Financial Reporting", emit: "Daily Financial Reporting" },
      { col: "Weekly Financial Reporting - Accounting & Financial Reporting", emit: "Weekly Financial Reporting" },
      { col: "Monthly P&L Statements - Accounting & Financial Reporting", emit: "Monthly P&L Statements" },
      { col: "Cash Flow Management - Accounting & Financial Reporting", emit: "Cash Flow Management" },
      { col: "Budget vs. Actual Analysis - Accounting & Financial Reporting", emit: "Budget vs. Actual Analysis" },
      { col: "Forecasting and Projections - Accounting & Financial Reporting", emit: "Forecasting and Projections" },
      { col: "Owner Portal Access - Accounting & Financial Reporting", emit: "Owner Portal Access" },
      { col: "Real-Time Financial Data - Accounting & Financial Reporting", emit: "Real-Time Financial Data" },
      { col: "Other - Accounting & Financial Reporting", emit: "Other" },
    ],
  },
  {
    aggregate: "Procurement Services",
    arrayBodyKey: "procurementServices",
    otherBodyKey: "procurementServicesOther",
    otherTextField: "Procurement Services Other",
    columns: [
      { col: "Centralized Purchasing - Procurement Services", emit: "Centralized Purchasing" },
      { col: "Preferred Vendor Network - Procurement Services", emit: "Preferred Vendor Network" },
      { col: "Volume Discounts - Procurement Services", emit: "Volume Discounts" },
      { col: "Supply Chain Management - Procurement Services", emit: "Supply Chain Management" },
      { col: "Vendor Relationship Management - Procurement Services", emit: "Vendor Relationship Management" },
      { col: "Cost Savings Programs - Procurement Services", emit: "Cost Savings Programs" },
      { col: "Quality Assurance On Purchases - Procurement Services", emit: "Quality Assurance On Purchases" },
      { col: "Other - Procurement Services", emit: "Other" },
    ],
  },
  {
    aggregate: "HR Training Services",
    arrayBodyKey: "hrTrainingServices",
    otherBodyKey: "hrTrainingServicesOther",
    otherTextField: "HR Training Services Other",
    columns: [
      { col: "Recruitment and Hiring - HR & Training Services", emit: "Recruitment and Hiring" },
      { col: "Onboarding Programs - HR & Training Services", emit: "Onboarding Programs" },
      { col: "Ongoing Training Programs - HR & Training Services", emit: "Ongoing Training Programs" },
      { col: "Leadership Development - HR & Training Services", emit: "Leadership Development" },
      { col: "Certification Support (CHA, CHRM, etc.) - HR & Training Services", emit: "Certification Support (CHA, CHRM, etc.)" },
      { col: "Performance Management - HR & Training Services", emit: "Performance Management" },
      { col: "Employee Retention Programs - HR & Training Services", emit: "Employee Retention Programs" },
      { col: "HR Compliance and Administration - HR & Training Services", emit: "HR Compliance and Administration" },
      { col: "Other - HR & Training Services", emit: "Other" },
    ],
  },
  {
    aggregate: "Technology Services",
    arrayBodyKey: "technologyServices",
    otherBodyKey: "technologyServicesOther",
    otherTextField: "Technology Services Other",
    columns: [
      { col: "IT Support and Helpdesk - Technology Services", emit: "IT Support and Helpdesk" },
      { col: "System Integrations - Technology Services", emit: "System Integrations" },
      { col: "Technology Infrastructure Management - Technology Services", emit: "Technology Infrastructure Management" },
      { col: "Cybersecurity Services - Technology Services", emit: "Cybersecurity Services" },
      { col: "Data Analytics and Reporting - Technology Services", emit: "Data Analytics and Reporting" },
      { col: "Cloud Services Management - Technology Services", emit: "Cloud Services Management" },
      { col: "Hardware Procurement and Management - Technology Services", emit: "Hardware Procurement and Management" },
      { col: "Other - Technology Services", emit: "Other" },
    ],
  },
  {
    aggregate: "Design Renovation Support",
    arrayBodyKey: "designRenovationSupport",
    otherBodyKey: "designRenovationSupportOther",
    otherTextField: "Design Renovation Support Other",
    columns: [
      { col: "In-House Design Team - Design & Renovation Support", emit: "In-House Design Team" },
      { col: "Renovation Project Management - Design & Renovation Support", emit: "Renovation Project Management" },
      { col: "FF&E Procurement - Design & Renovation Support", emit: "FF&E Procurement" },
      { col: "Brand Standard Compliance - Design & Renovation Support", emit: "Brand Standard Compliance" },
      { col: "Space Planning and Design - Design & Renovation Support", emit: "Space Planning and Design" },
      { col: "Construction Management - Design & Renovation Support", emit: "Construction Management" },
      { col: "Vendor Coordination - Design & Renovation Support", emit: "Vendor Coordination" },
      { col: "Other - Design & Renovation Support", emit: "Other" },
    ],
  },
  {
    aggregate: "Development Services",
    arrayBodyKey: "developmentServices",
    otherBodyKey: "developmentServicesOther",
    otherTextField: "Development Services Other",
    columns: [
      { col: "Pre-Opening Services - Development Services", emit: "Pre-Opening Services" },
      { col: "New Build Project Management - Development Services", emit: "New Build Project Management" },
      { col: "Conversion Project Management - Development Services", emit: "Conversion Project Management" },
      { col: "Feasibility Studies - Development Services", emit: "Feasibility Studies" },
      { col: "Development Consulting - Development Services", emit: "Development Consulting" },
      { col: "Permit and Regulatory Support - Development Services", emit: "Permit and Regulatory Support" },
      { col: "Opening Team Deployment - Development Services", emit: "Opening Team Deployment" },
      { col: "Other - Development Services", emit: "Other" },
    ],
  },
]);

/** Legacy / Basics aggregate multi-select names (not written as columns on new-base Governance — only per-option checkboxes). */
export const OPERATOR_SERVICE_AGGREGATE_FIELD_NAMES = OPERATOR_SERVICE_GRANULAR.map((c) => c.aggregate);

/** Checkbox columns + `* Other` long-text field names (for scripts / audits). */
const seenGranularNames = new Set();
export const ALL_OPERATOR_SERVICE_GRANULAR_FIELD_NAMES = [];
for (const cat of OPERATOR_SERVICE_GRANULAR) {
  for (const { col } of cat.columns) {
    if (!seenGranularNames.has(col)) {
      seenGranularNames.add(col);
      ALL_OPERATOR_SERVICE_GRANULAR_FIELD_NAMES.push(col);
    }
  }
  if (!seenGranularNames.has(cat.otherTextField)) {
    seenGranularNames.add(cat.otherTextField);
    ALL_OPERATOR_SERVICE_GRANULAR_FIELD_NAMES.push(cat.otherTextField);
  }
}

/**
 * @param {unknown} value
 * @returns {string[]}
 */
function asStringArray(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map((v) => String(v)).filter((s) => s.trim() !== "");
  if (typeof value === "string") return parseMultiValue(value);
  return [];
}

/**
 * Adds per-option checkbox fields + `* Other` long-text from the same submission data.
 * @param {Record<string, unknown>} compactFields
 * @param {Record<string, unknown>} otherTexts — req.body subset with *Other keys
 * @param {{ writeGranularCheckboxes?: boolean }} [options]
 *   — `writeGranularCheckboxes`: default **false**; set **true** or `OPERATOR_SERVICE_GRANULAR_CHECKBOX_WRITES=1` for legacy per-option columns.
 * @returns {Record<string, unknown>}
 */
export function mergeGranularServiceSelectionsIntoCompactFields(compactFields, otherTexts = {}, options = {}) {
  const writeGranularCheckboxes = options.writeGranularCheckboxes === true;
  const out = { ...compactFields };
  for (const cat of OPERATOR_SERVICE_GRANULAR) {
    const selected = asStringArray(out[cat.aggregate]);
    if (writeGranularCheckboxes && selected.length > 0) {
      for (const { col, emit } of cat.columns) {
        const on = selected.some((s) => selectionMatchesColumn(s, emit, col));
        if (on) out[col] = true;
      }
    }
    const rawOther = otherTexts[cat.otherBodyKey];
    const t = rawOther != null ? String(rawOther).trim() : "";
    if (t) out[cat.otherTextField] = t;
  }
  return out;
}

/**
 * Per-option service checkboxes for **Operator Setup - Governance, Delivery & Diligence** (new base).
 * Writes the same `* Other` long-text columns as Service Offerings + checkbox columns; omits aggregate keys on the governance row.
 * @param {Record<string, unknown>} body
 * @returns {Record<string, unknown>}
 */
export function buildGovernanceGranularAirtableFields(body) {
  const compact = {};
  for (const cat of OPERATOR_SERVICE_GRANULAR) {
    const arr = asStringArray(body?.[cat.arrayBodyKey]);
    if (arr.length) compact[cat.aggregate] = arr;
  }
  const merged = mergeGranularServiceSelectionsIntoCompactFields(compact, body || {}, {
    writeGranularCheckboxes: true,
  });
  for (const name of OPERATOR_SERVICE_AGGREGATE_FIELD_NAMES) {
    delete merged[name];
  }
  return merged;
}

function str(v) {
  if (v == null || v === false) return "";
  return String(v).trim();
}

/** Lowercase alphanumerics only — matches Airtable titles to snake_case / alternate punctuation. */
function normAlnumKey(s) {
  return String(s ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

/**
 * Resolve Airtable field value when column titles differ (e.g. snake_case vs "Option - Category").
 * @param {Record<string, unknown> | null | undefined} sf
 * @param {string} col
 */
function getFieldValueFuzzy(sf, col) {
  if (sf == null || typeof sf !== "object") return undefined;
  if (Object.prototype.hasOwnProperty.call(sf, col)) return sf[col];
  const want = normAlnumKey(col);
  if (!want) return undefined;
  for (const k of Object.keys(sf)) {
    if (normAlnumKey(k) === want) return sf[k];
  }
  return undefined;
}

function isTruthyCheckbox(v) {
  if (v === true) return true;
  if (v === false || v == null) return false;
  if (typeof v === "number" && v === 1) return true;
  const t = String(v).toLowerCase().trim();
  return t === "yes" || t === "checked" || t === "true" || t === "1";
}

/**
 * Map aggregate multi-select / long-text tokens to form option values (`emit`).
 */
function mapAggValuesToFormEmits(cat, fromAgg) {
  const out = [];
  for (const raw of fromAgg) {
    const s = String(raw).trim();
    if (!s) continue;
    let hit = null;
    const n = normLabel(s);
    for (const { emit } of cat.columns) {
      if (normLabel(emit) === n) {
        hit = emit;
        break;
      }
    }
    if (!hit) {
      const want = normAlnumKey(s);
      for (const { emit } of cat.columns) {
        if (normAlnumKey(emit) === want) {
          hit = emit;
          break;
        }
      }
    }
    out.push(hit || s);
  }
  return out;
}

/**
 * Prefill service groups from Service Offerings row: prefer aggregate multi-select; if empty, reconstruct from per-option checkboxes.
 * @param {Record<string, unknown>} sf — Service Offerings fields
 * @param {Record<string, unknown>} prefill
 */
export function applyOperatorServiceGranularPrefill(sf, prefill) {
  for (const cat of OPERATOR_SERVICE_GRANULAR) {
    const aggRaw = getFieldValueFuzzy(sf, cat.aggregate);
    const fromAgg = parseMultiValue(aggRaw);
    if (fromAgg.length) {
      prefill[cat.arrayBodyKey] = mapAggValuesToFormEmits(cat, fromAgg);
    } else {
      const vals = [];
      for (const { col, emit } of cat.columns) {
        const v = getFieldValueFuzzy(sf, col);
        if (isTruthyCheckbox(v)) vals.push(emit);
      }
      if (vals.length) prefill[cat.arrayBodyKey] = vals;
    }
    const otherRaw = getFieldValueFuzzy(sf, cat.otherTextField);
    const other = str(otherRaw) || str(prefill[cat.otherBodyKey]);
    if (other) prefill[cat.otherBodyKey] = other;
  }
}
