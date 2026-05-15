/**
 * FDD Intelligence — idempotent Airtable schema setup (server-side only).
 *
 * Requires a Personal Access Token (or OAuth) with:
 *   - schema.bases:read
 *   - schema.bases:write
 * stored in AIRTABLE_API_KEY (same env as the rest of this app). Legacy API keys may not work for Metadata API.
 *
 * Default: dry-run (no writes). Pass --execute to create missing tables/fields.
 * Pass --dry-run to force print-only even if --execute were present (--dry-run wins).
 *
 * This script never deletes, renames, or updates existing fields — only creates missing tables/fields.
 */

import axios from "axios";

/** Same .env / .env.local loading as server.js (repo root; run from project root). */
function loadEnv() {
  return import("../../load-env.js");
}

const META_BASE = (baseId) => `https://api.airtable.com/v0/meta/bases/${baseId}`;

function parseArgs() {
  const argv = process.argv.slice(2);
  const dryRunForced = argv.includes("--dry-run");
  const execute = argv.includes("--execute");
  const dryRun = dryRunForced || !execute;
  return { dryRun, execute: execute && !dryRunForced };
}

function loadConfig() {
  const apiKey = process.env.AIRTABLE_API_KEY || "";
  const baseId = process.env.AIRTABLE_BASE_ID || "";
  const docsTable = process.env.AIRTABLE_TABLE_FDD_DOCUMENTS || "FDD Documents";
  const sectionsTable = process.env.AIRTABLE_TABLE_FDD_SECTIONS || "FDD Sections";
  const feeRowsTable = process.env.AIRTABLE_TABLE_FDD_FEE_ROWS || "FDD Fee Rows";
  const termsTable = process.env.AIRTABLE_TABLE_FDD_TERMS || "FDD Terms & Obligations";
  const fullTextField = process.env.AIRTABLE_FDD_DOCUMENT_FULL_TEXT_FIELD || "Full Text";
  return { apiKey, baseId, docsTable, sectionsTable, feeRowsTable, termsTable, fullTextField };
}

function metaHeaders(apiKey) {
  return {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
  };
}

async function getBaseSchema(apiKey, baseId) {
  const { data, status } = await axios.get(`${META_BASE(baseId)}/tables`, {
    headers: metaHeaders(apiKey),
    validateStatus: () => true,
  });
  if (status >= 400 || data?.error) {
    const msg = data?.error?.message || data?.message || `HTTP ${status}`;
    const err = new Error(msg);
    err.status = status;
    throw err;
  }
  return data;
}

function findTable(schema, tableName) {
  const tables = schema?.tables || [];
  return tables.find((t) => t.name === tableName) || null;
}

function findField(table, fieldName) {
  const fields = table?.fields || [];
  return fields.find((f) => f.name === fieldName) || null;
}

function dateTimeOptions() {
  return {
    timeZone: "utc",
    dateFormat: { name: "iso" },
    timeFormat: { name: "24hour" },
  };
}

function checkboxOptions() {
  return { color: "greenBright", icon: "check" };
}

function numberOptionsInt() {
  return { precision: 0 };
}

/** Build Airtable Metadata API field create body (name + type + options when required). */
function fieldCreateBody(name, type) {
  if (type === "checkbox") {
    return { name, type: "checkbox", options: checkboxOptions() };
  }
  if (type === "number") {
    return { name, type: "number", options: numberOptionsInt() };
  }
  if (type === "dateTime") {
    return { name, type: "dateTime", options: dateTimeOptions() };
  }
  if (type === "url") {
    return { name, type: "url" };
  }
  return { name, type };
}

async function createTable(apiKey, baseId, tableName, primaryField) {
  const url = `${META_BASE(baseId)}/tables`;
  const body = {
    name: tableName,
    fields: [fieldCreateBody(primaryField.name, primaryField.type)],
  };
  const { data, status } = await axios.post(url, body, {
    headers: metaHeaders(apiKey),
    validateStatus: () => true,
  });
  if (status >= 400) {
    const msg = data?.error?.message || data?.message || `HTTP ${status}`;
    const err = new Error(msg);
    err.status = status;
    err.body = data;
    throw err;
  }
  return data;
}

async function createField(apiKey, baseId, tableId, fieldSpec) {
  const url = `${META_BASE(baseId)}/tables/${tableId}/fields`;
  const body = fieldCreateBody(fieldSpec.name, fieldSpec.type);
  const { data, status } = await axios.post(url, body, {
    headers: metaHeaders(apiKey),
    validateStatus: () => true,
  });
  if (status >= 400) {
    const msg = data?.error?.message || data?.message || `HTTP ${status}`;
    const err = new Error(msg);
    err.status = status;
    err.body = data;
    throw err;
  }
  return data;
}

function buildDocumentsFieldSpecs(fullTextFieldName) {
  return [
    { name: "Parent Company", type: "singleLineText" },
    { name: "Brand Name", type: "singleLineText" },
    { name: "FDD Year", type: "number" },
    { name: "Country", type: "singleLineText" },
    { name: "Jurisdiction", type: "singleLineText" },
    { name: "Document Type", type: "singleLineText" },
    { name: "Source Type", type: "singleLineText" },
    { name: "Source URL", type: "url" },
    { name: "File Name", type: "singleLineText" },
    { name: "File Path", type: "singleLineText" },
    { name: fullTextFieldName, type: "multilineText" },
    { name: "Extraction Status", type: "singleLineText" },
    { name: "Extraction Notes", type: "multilineText" },
    { name: "Extracted At", type: "dateTime" },
    { name: "Reviewed At", type: "dateTime" },
    { name: "Reviewer", type: "singleLineText" },
    { name: "Notes", type: "multilineText" },
    { name: "Created At", type: "dateTime" },
    { name: "Updated At", type: "dateTime" },
  ];
}

const SECTIONS_FIELDS = [
  { name: "FDD Document ID", type: "singleLineText" },
  { name: "Item Number", type: "singleLineText" },
  { name: "Item Title", type: "singleLineText" },
  { name: "Section Text", type: "multilineText" },
  { name: "Page Start", type: "singleLineText" },
  { name: "Page End", type: "singleLineText" },
  { name: "Extraction Status", type: "singleLineText" },
  { name: "Source Format", type: "singleLineText" },
  { name: "Source Section Label", type: "singleLineText" },
  { name: "Source Section Heading", type: "singleLineText" },
  { name: "Extraction Target", type: "singleLineText" },
  { name: "Candidate Source Type", type: "singleLineText" },
  { name: "Segment Order", type: "number" },
  { name: "Created At", type: "dateTime" },
  { name: "Updated At", type: "dateTime" },
];

const FEE_ROWS_FIELDS = [
  { name: "FDD Document ID", type: "singleLineText" },
  { name: "Parent Company", type: "singleLineText" },
  { name: "Brand Name", type: "singleLineText" },
  { name: "FDD Year", type: "number" },
  { name: "Country", type: "singleLineText" },
  { name: "Fee / Obligation Name", type: "singleLineText" },
  { name: "Fee Type", type: "singleLineText" },
  { name: "Commercial Category", type: "singleLineText" },
  { name: "Amount", type: "multilineText" },
  { name: "Amount Type", type: "singleLineText" },
  { name: "Basis", type: "singleLineText" },
  { name: "Frequency", type: "singleLineText" },
  { name: "Due Timing", type: "multilineText" },
  { name: "Required / Optional", type: "singleLineText" },
  { name: "Lifecycle Phase", type: "singleLineText" },
  { name: "Applies When", type: "multilineText" },
  { name: "Conditional Trigger", type: "multilineText" },
  { name: "Responsible Party", type: "singleLineText" },
  { name: "Pass-Through Status", type: "singleLineText" },
  { name: "Bundled Status", type: "singleLineText" },
  { name: "Can Be Waived", type: "singleLineText" },
  { name: "Estimated Cost Impact", type: "singleLineText" },
  { name: "Implementation Risk", type: "singleLineText" },
  { name: "Intake Mapping", type: "multilineText" },
  { name: "Match Score Impact", type: "multilineText" },
  { name: "Documentation Reference", type: "singleLineText" },
  { name: "Documentation Reference Page Number", type: "singleLineText" },
  { name: "Source Text Excerpt", type: "multilineText" },
  { name: "Reviewer Notes", type: "multilineText" },
  { name: "Review Status", type: "singleLineText" },
  { name: "Extraction Run ID", type: "singleLineText" },
  { name: "Extraction Used AI", type: "checkbox" },
  { name: "Model Name Used", type: "singleLineText" },
  { name: "Source Item Number", type: "singleLineText" },
  { name: "Source Item Title", type: "singleLineText" },
  { name: "Source Document ID", type: "singleLineText" },
  { name: "Source Document Brand Name", type: "singleLineText" },
  { name: "Source Document FDD Year", type: "singleLineText" },
  { name: "Extraction Confidence", type: "singleLineText" },
  { name: "Source Chunk Index", type: "singleLineText" },
  { name: "Source Chunk Count", type: "singleLineText" },
  { name: "Needs Legal Review", type: "checkbox" },
  { name: "Needs Commercial Review", type: "checkbox" },
  { name: "Possible Duplicate", type: "checkbox" },
  { name: "Duplicate Group Key", type: "singleLineText" },
  { name: "Normalized Cost Basis", type: "singleLineText" },
  { name: "Raw Cost Basis Text", type: "multilineText" },
  { name: "Amount Formula Type", type: "singleLineText" },
  { name: "Calculation Unit", type: "singleLineText" },
  { name: "Revenue Base", type: "singleLineText" },
  { name: "Unit Rate", type: "singleLineText" },
  { name: "Percentage Rate", type: "singleLineText" },
  { name: "Fixed Amount", type: "singleLineText" },
  { name: "Formula Notes", type: "multilineText" },
  { name: "Basis Confidence", type: "singleLineText" },
  { name: "Basis Needs Review", type: "checkbox" },
  { name: "Created At", type: "dateTime" },
  { name: "Updated At", type: "dateTime" },
  { name: "Audit Score", type: "number" },
  { name: "Audit Confidence", type: "singleLineText" },
  { name: "Audit Status", type: "singleLineText" },
  { name: "Audit Issues", type: "multilineText" },
  { name: "Auto-Approve Eligible", type: "checkbox" },
  { name: "Last Audited At", type: "dateTime" },
  { name: "Audit Version", type: "singleLineText" },
  { name: "Source Support Score", type: "number" },
  { name: "Amount Quality Score", type: "number" },
  { name: "Basis Quality Score", type: "number" },
  { name: "Category Quality Score", type: "number" },
  { name: "Duplicate Risk Score", type: "number" },
  { name: "Legal Risk Score", type: "number" },
];

const TERMS_FIELDS = [
  { name: "FDD Document ID", type: "singleLineText" },
  { name: "Parent Company", type: "singleLineText" },
  { name: "Brand Name", type: "singleLineText" },
  { name: "FDD Year", type: "number" },
  { name: "Country", type: "singleLineText" },
  { name: "Source Item Number", type: "singleLineText" },
  { name: "Source Item Title", type: "singleLineText" },
  { name: "Term Category", type: "singleLineText" },
  { name: "Term Summary", type: "multilineText" },
  { name: "Owner Impact", type: "multilineText" },
  { name: "Required / Conditional / Optional", type: "singleLineText" },
  { name: "Trigger", type: "singleLineText" },
  { name: "Applies When", type: "multilineText" },
  { name: "Risk Level", type: "singleLineText" },
  { name: "Flexibility Level", type: "singleLineText" },
  { name: "Negotiability", type: "singleLineText" },
  { name: "Legal Review Required", type: "checkbox" },
  { name: "Commercial Review Required", type: "checkbox" },
  { name: "Source Text Excerpt", type: "multilineText" },
  { name: "Documentation Reference", type: "singleLineText" },
  { name: "Documentation Reference Page Number", type: "singleLineText" },
  { name: "Confidence", type: "singleLineText" },
  { name: "Review Status", type: "singleLineText" },
  { name: "Reviewer Notes", type: "multilineText" },
  { name: "Extraction Run ID", type: "singleLineText" },
  { name: "Extraction Used AI", type: "checkbox" },
  { name: "Model Name Used", type: "singleLineText" },
  { name: "Source Chunk Index", type: "singleLineText" },
  { name: "Source Chunk Count", type: "singleLineText" },
  { name: "Normalized Term Bucket", type: "singleLineText" },
  { name: "Comparable Term Group", type: "singleLineText" },
  { name: "Possible Duplicate Term", type: "checkbox" },
  { name: "Duplicate Term Group Key", type: "singleLineText" },
  { name: "Term Audit Score", type: "number" },
  { name: "Term Audit Confidence", type: "singleLineText" },
  { name: "Term Audit Status", type: "singleLineText" },
  { name: "Term Audit Issues", type: "multilineText" },
  { name: "Auto-Approve Eligible", type: "checkbox" },
  { name: "Last Audited At", type: "dateTime" },
  { name: "Audit Version", type: "singleLineText" },
  { name: "Source Support Score", type: "number" },
  { name: "Category Quality Score", type: "number" },
  { name: "Risk Quality Score", type: "number" },
  { name: "Owner Impact Score", type: "number" },
  { name: "Legal Sensitivity Score", type: "number" },
  { name: "Created At", type: "dateTime" },
  { name: "Updated At", type: "dateTime" },
];

async function ensureTable({
  apiKey,
  baseId,
  tableName,
  primaryField,
  summary,
  dryRun,
}) {
  let schema = summary.schema;
  let table = findTable(schema, tableName);
  if (table) {
    if (!summary.tablesFound.includes(tableName)) summary.tablesFound.push(tableName);
    return table;
  }
  if (dryRun) {
    summary.tablesWouldCreate.push(tableName);
    return null;
  }
  try {
    await createTable(apiKey, baseId, tableName, primaryField);
    summary.tablesCreated.push(tableName);
    schema = await getBaseSchema(apiKey, baseId);
    summary.schema = schema;
    table = findTable(schema, tableName);
    return table;
  } catch (e) {
    summary.permissionErrors.push(`Create table "${tableName}": ${e.message}`);
    summary.tablesMissing.push(tableName);
    return null;
  }
}

async function ensureField({ apiKey, baseId, table, fieldSpec, summary, dryRun }) {
  if (!table?.id) return false;
  const existing = findField(table, fieldSpec.name);
  if (existing) {
    summary.fieldsFound.push(`${table.name} → ${fieldSpec.name}`);
    return true;
  }
  if (dryRun) {
    summary.fieldsWouldCreate.push(`${table.name} → ${fieldSpec.name}`);
    return false;
  }
  try {
    await createField(apiKey, baseId, table.id, fieldSpec);
    summary.fieldsCreated.push(`${table.name} → ${fieldSpec.name}`);
    return true;
  } catch (e) {
    let msg = e.message;
    if (fieldSpec.name === "Source URL" && fieldSpec.type === "url") {
      try {
        await createField(apiKey, baseId, table.id, { name: "Source URL", type: "singleLineText" });
        summary.fieldsCreated.push(`${table.name} → Source URL (singleLineText fallback)`);
        return true;
      } catch (e2) {
        msg = `${e.message}; fallback failed: ${e2.message}`;
      }
    }
    summary.permissionErrors.push(`Create field "${table.name}"."${fieldSpec.name}": ${msg}`);
    summary.fieldsMissing.push(`${table.name} → ${fieldSpec.name}`);
    return false;
  }
}

async function ensureTableFields({ apiKey, baseId, tableName, primaryField, fieldSpecs, summary, dryRun }) {
  const table = await ensureTable({
    apiKey,
    baseId,
    tableName,
    primaryField,
    summary,
    dryRun,
  });
  if (!table && dryRun) {
    for (const f of fieldSpecs) {
      if (f.name === primaryField.name) continue;
      summary.fieldsWouldCreate.push(`${tableName} → ${f.name} (table missing)`);
    }
    return;
  }
  if (!table) return;

  let schema = summary.schema;
  let t = findTable(schema, tableName);

  for (const fieldSpec of fieldSpecs) {
    if (fieldSpec.name === primaryField.name) {
      if (findField(t, fieldSpec.name)) summary.fieldsFound.push(`${tableName} → ${fieldSpec.name} (primary)`);
      continue;
    }
    await ensureField({ apiKey, baseId, table: t, fieldSpec, summary, dryRun });
    if (!dryRun) {
      schema = await getBaseSchema(apiKey, baseId);
      summary.schema = schema;
      t = findTable(schema, tableName);
    }
  }
}

function summarizeResults(summary) {
  console.log("\n========== FDD Airtable schema setup summary ==========\n");
  console.log("Tables found:", summary.tablesFound.length ? summary.tablesFound.join(", ") : "(none)");
  console.log("Tables created:", summary.tablesCreated.length ? summary.tablesCreated.join(", ") : "(none)");
  if (summary.tablesWouldCreate.length) {
    console.log("Tables that would be created (dry-run):", summary.tablesWouldCreate.join(", "));
  }
  if (summary.tablesMissing.length) {
    console.log("Tables missing / could not create:\n  ", summary.tablesMissing.join(", "));
  }
  console.log("Fields matched (already present):", summary.fieldsFound.length);
  console.log("Fields created:", summary.fieldsCreated.length ? summary.fieldsCreated.join("\n  ") : "(none)");
  if (summary.fieldsWouldCreate.length) {
    console.log("Fields that would be created (dry-run):\n  ", summary.fieldsWouldCreate.join("\n  "));
  }
  if (summary.fieldsMissing.length) {
    console.log("Fields missing / could not create (execute mode):\n  ", summary.fieldsMissing.join("\n  "));
  }
  if (summary.fieldsStillIncomplete?.length) {
    console.log("Required checklist still incomplete:\n  ", summary.fieldsStillIncomplete.join("\n  "));
  }
  if (summary.permissionErrors.length) {
    console.log("\nErrors / permission issues:\n  ", summary.permissionErrors.join("\n  "));
    console.log(
      "\nEnsure your token has schema.bases:read and schema.bases:write, and that you are a base collaborator with schema edit access."
    );
  }
  console.log("\n========================================================\n");
}

function printInstructions(cfg) {
  console.log(`
Next steps (FDD Intelligence Airtable setup)
--------------------------------------------
1) Add or confirm in .env / .env.local (repo root):
   AIRTABLE_API_KEY=<Personal Access Token with schema.bases:read + schema.bases:write>
   AIRTABLE_BASE_ID=<your base id>
   AIRTABLE_TABLE_FDD_DOCUMENTS=${cfg.docsTable}
   AIRTABLE_TABLE_FDD_SECTIONS=${cfg.sectionsTable}
   AIRTABLE_TABLE_FDD_FEE_ROWS=${cfg.feeRowsTable}
   AIRTABLE_TABLE_FDD_TERMS=${cfg.termsTable}
   AIRTABLE_FDD_DOCUMENT_FULL_TEXT_FIELD=${cfg.fullTextField}

2) Dry-run (default — prints plan, no writes):
   npm run setup:fdd-airtable
   or: node scripts/fdd-intelligence/setup-airtable-schema.js --dry-run

3) Apply changes:
   npm run setup:fdd-airtable -- --execute

4) Restart the Dealality server so it picks up env.

5) Open /fdd-intelligence-admin.html

6) Confirm the FDD admin UI banner shows storage: airtable (when document, sections, and fee-row table env vars and credentials are set). Terms rows use a separate table: set AIRTABLE_TABLE_FDD_TERMS for terms persistence (see /fdd-terms-admin.html).
`);
}

await loadEnv();

const { dryRun } = parseArgs();
const cfg = loadConfig();

if (!cfg.apiKey || !cfg.baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID. Set them in .env or .env.local (repo root).");
  process.exit(1);
}

console.log(`Mode: ${dryRun ? "DRY-RUN (no API writes)" : "EXECUTE (will create missing tables/fields)"}`);
console.log(`Base: ${cfg.baseId}`);
console.log(`Tables: ${cfg.docsTable} | ${cfg.sectionsTable} | ${cfg.feeRowsTable} | ${cfg.termsTable}`);
console.log(`Full text field name on documents: ${cfg.fullTextField}\n`);

const summary = {
  schema: null,
  tablesFound: [],
  tablesCreated: [],
  tablesMissing: [],
  tablesWouldCreate: [],
  fieldsFound: [],
  fieldsCreated: [],
  fieldsWouldCreate: [],
  fieldsMissing: [],
  fieldsStillIncomplete: [],
  permissionErrors: [],
};

let exitCode = 0;

try {
  summary.schema = await getBaseSchema(cfg.apiKey, cfg.baseId);
} catch (e) {
  console.error("Failed to fetch base schema:", e.message);
  summary.permissionErrors.push(`GET /meta/bases/.../tables: ${e.message}`);
  summarizeResults(summary);
  printInstructions(cfg);
  process.exit(1);
}

const docsFields = buildDocumentsFieldSpecs(cfg.fullTextField);

await ensureTableFields({
  apiKey: cfg.apiKey,
  baseId: cfg.baseId,
  tableName: cfg.docsTable,
  primaryField: { name: "Brand Name", type: "singleLineText" },
  fieldSpecs: docsFields,
  summary,
  dryRun,
});

await ensureTableFields({
  apiKey: cfg.apiKey,
  baseId: cfg.baseId,
  tableName: cfg.sectionsTable,
  primaryField: { name: "Item Title", type: "singleLineText" },
  fieldSpecs: SECTIONS_FIELDS,
  summary,
  dryRun,
});

await ensureTableFields({
  apiKey: cfg.apiKey,
  baseId: cfg.baseId,
  tableName: cfg.feeRowsTable,
  primaryField: { name: "Fee / Obligation Name", type: "singleLineText" },
  fieldSpecs: FEE_ROWS_FIELDS,
  summary,
  dryRun,
});

await ensureTableFields({
  apiKey: cfg.apiKey,
  baseId: cfg.baseId,
  tableName: cfg.termsTable,
  primaryField: { name: "Term / Obligation Name", type: "singleLineText" },
  fieldSpecs: TERMS_FIELDS,
  summary,
  dryRun,
});

/** Re-verify required fields exist (post-run). */
function collectRequiredFieldChecks() {
  const s = summary.schema;
  const out = [];
  const tDocs = findTable(s, cfg.docsTable);
  const tSec = findTable(s, cfg.sectionsTable);
  const tFee = findTable(s, cfg.feeRowsTable);
  const tTerms = findTable(s, cfg.termsTable);
  if (!tDocs) out.push(`Missing table: ${cfg.docsTable}`);
  if (!tSec) out.push(`Missing table: ${cfg.sectionsTable}`);
  if (!tFee) out.push(`Missing table: ${cfg.feeRowsTable}`);
  if (!tTerms) out.push(`Missing table: ${cfg.termsTable}`);
  for (const f of docsFields) {
    if (tDocs && !findField(tDocs, f.name)) out.push(`Missing field: ${cfg.docsTable}.${f.name}`);
  }
  for (const f of SECTIONS_FIELDS) {
    if (tSec && !findField(tSec, f.name)) out.push(`Missing field: ${cfg.sectionsTable}.${f.name}`);
  }
  for (const f of FEE_ROWS_FIELDS) {
    if (tFee && !findField(tFee, f.name)) out.push(`Missing field: ${cfg.feeRowsTable}.${f.name}`);
  }
  for (const f of TERMS_FIELDS) {
    if (tTerms && !findField(tTerms, f.name)) out.push(`Missing field: ${cfg.termsTable}.${f.name}`);
  }
  return out;
}

if (!dryRun) {
  try {
    summary.schema = await getBaseSchema(cfg.apiKey, cfg.baseId);
  } catch (_) {
    /* ignore */
  }
  const stillMissing = collectRequiredFieldChecks();
  if (stillMissing.length) {
    console.error("\nRequired schema incomplete:\n", stillMissing.join("\n"));
    summary.fieldsStillIncomplete = stillMissing;
    exitCode = 1;
  }
}

summarizeResults(summary);
printInstructions(cfg);

process.exit(exitCode);
