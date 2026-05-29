#!/usr/bin/env node
/**
 * Phase A: Operator Setup field coverage diff (audit-only).
 * Outputs reports/operator-setup-field-coverage-diff.csv and docs/operator-setup-field-coverage-diff.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { OPERATOR_SERVICE_GRANULAR } from "../api/lib/operator-setup-service-granular-fields.js";
import { OAS_OPERATOR_PREFILL_KEY_ALIASES } from "../lib/operator-alignment-field-options.js";
import { getUsedThirdPartyOperatorFieldNamesByTable } from "../api/lib/third-party-operator-airtable-fields-used.js";
import { BASICS_AIRTABLE_TO_FORM_KEY } from "../api/lib/third-party-operator-basics-to-prefill.js";
import { BASICS_PRIMARY_FIELD_NAMES } from "../api/lib/operator-setup-write-plan.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const SCHEMA_PATH = path.join(ROOT, "reports/operator-alignment-5b-schema-backup-2026-05-25.json");
const INVENTORY_PATH = path.join(ROOT, "scripts/he-cala-form-inventory.json");
const BUILD_SHEET_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json");
const BINDINGS_PATH = path.join(ROOT, "api/lib/third-party-operator-new-two-field-bindings.json");
const OAS_INJECT_PATH = path.join(ROOT, "public/js/oas-inject-form-fields.js");
const PHASE_B_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-phase-b-fields.json");

const TABLE_KEY_TO_NAME = {
  master: "Operator Setup - Master",
  profile: "Operator Setup - Profile & Positioning",
  platform: "Operator Setup - Platform & Markets",
  commercial: "Operator Setup - Commercial Fit & Terms",
  governance: "Operator Setup - Governance, Delivery & Diligence",
};

const CHILD_TABLES = {
  "Operator Setup - Leadership Team Members": {
    fields: ["name", "title", "role", "summary", "bio", "headshot", "display_order"],
    payload: "exec_1..6_* (form) → leadership child rows",
    source: "operator-setup-new-base-writer.js",
  },
  "Operator Setup - Case Studies": {
    fields: [
      "property_name",
      "hotel_type",
      "region",
      "branded_independent",
      "situation",
      "services",
      "outcome",
      "owner_relevance",
      "image_url",
      "display_order",
    ],
    payload: "caseStudiesDetail (JSON)",
    source: "operator-setup-new-base-writer.js",
  },
  "Operator Setup - Diligence QA": {
    fields: ["category", "question", "answer", "display_order"],
    payload: "ownerDiligenceQa (JSON)",
    source: "operator-setup-new-base-writer.js",
  },
};

const JSON_PAYLOAD_FORMS = new Set([
  "caseStudiesDetail",
  "ownerDiligenceQa",
  "explorerProfileJson",
  "keyLeadership",
  "brandsPortfolioDetail",
  "companyLogo",
]);

const SYSTEM_FIELDS = new Set([
  "operator_id",
  "submission_status",
  "created_at",
  "updated_at",
  "Operator",
]);

const LINK_FIELD_RE = /^Operator Setup - /;
const NEEDS_DECISION_FIELDS = new Set(["dealTermsOptIn", "diligenceQaOptIn"]);

const GRANULAR_COLUMNS = new Set();
for (const cat of OPERATOR_SERVICE_GRANULAR) {
  for (const c of cat.columns) GRANULAR_COLUMNS.add(c.col);
}

const FORM_ALIAS_TO_SCHEMA = {
  companyName: { table: "Operator Setup - Profile & Positioning", field: "company_name" },
  regions: { table: "Operator Setup - Platform & Markets", field: "specificMarkets", note: "legacy Regions Supported; new-base uses geo_* totals for regionsSupported read" },
};

function csvEscape(s) {
  const t = String(s ?? "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (/[",\n]/.test(t)) return '"' + t.replace(/"/g, '""') + '"';
  return t;
}

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function extractOasInjectedFields(js) {
  const names = new Set();
  const re = /name="([a-zA-Z0-9_]+)"/g;
  let m;
  while ((m = re.exec(js)) !== null) {
    if (m[1].includes(" ")) continue;
    names.add(m[1]);
  }
  return names;
}

function buildConsumerSets() {
  const oasCamel = new Set(Object.keys(OAS_OPERATOR_PREFILL_KEY_ALIASES));
  const oasTitles = new Set();
  for (const aliases of Object.values(OAS_OPERATOR_PREFILL_KEY_ALIASES)) {
    for (const a of aliases) oasTitles.add(a);
  }
  const explorerCamel = new Set([
    "companyName",
    "companyDescription",
    "website",
    "headquarters",
    "yearEstablished",
    "yearsInBusiness",
    "primaryServiceModel",
    "numberOfBrands",
    "brands",
    "chainScalesSupported",
    "chainScale",
    "regions",
    "regionsSupported",
    "specificMarkets",
    "totalProperties",
    "totalRooms",
    "companyLogo",
    "dataConfidenceLevel",
    "sourceType",
    "lastUpdatedDate",
    "activeCountries",
    "activeMarkets",
    "marketPresenceType",
    "serviceModelsSupported",
    "managementStructuresSupported",
    "offeredServices",
    "newBuildOpeningExperience",
    "preOpeningSupportCapability",
    "ownerReportingLevel",
    "revenueManagementCapability",
    "governanceCadence",
    "brandFamiliesOperated",
    "conversionReflagExperience",
    "softBrandLifestyleExperience",
    "fbCapabilityLevel",
    "salesPlatform",
    "minimumKeyCount",
    "brand_signal_audit",
    "brand_signal_reflag",
    "brand_signal_franchise_align",
    "brand_signal_soft_retention",
    "bf_operating_situations",
    "bf_selected_asset_types",
    "bf_selected_situation_types",
    "bf_selected_deal_structures",
    "bf_not_ideal_for",
    "cap_kpi_operating_model",
    "cap_kpi_execution_strength",
    "cap_kpi_transition",
    "cap_kpi_reporting",
    "caseStudiesDetail",
    "brandsPortfolioDetail",
  ]);
  for (let i = 1; i <= 3; i++) {
    for (const p of ["overview_bestat_", "overview_why_"]) {
      explorerCamel.add(`${p}${i}_headline`);
      explorerCamel.add(`${p}${i}_story`);
    }
    explorerCamel.add(`overview_signal_${i}_value`);
  }
  const strategyCamel = new Set([
    "dataConfidenceLevel",
    "companyName",
    "company_name",
    "submission_status",
    "regionsSupported",
    "chainScale",
    "primaryServiceModel",
    "totalProperties",
    "totalRooms",
  ]);
  return { oasCamel, oasTitles, explorerCamel, strategyCamel };
}

function fieldNeededFor(name, consumers) {
  const oasYes =
    consumers.oasCamel.has(name) ||
    consumers.oasTitles.has(name) ||
    name.startsWith("bf_") ||
    name.startsWith("cap_") ||
    name.startsWith("brand_signal_") ||
    ["submission_status", "dataConfidenceLevel", "Data Confidence Level", "Source Type", "Last Updated Date"].includes(
      name
    );
  const explorerYes =
    consumers.explorerCamel.has(name) ||
    name.startsWith("overview_") ||
    name.startsWith("cap_") ||
    name.startsWith("geo_") ||
    name.startsWith("ov_") ||
    name.startsWith("infra_") ||
    name.startsWith("lead_") ||
    name.startsWith("tr_") ||
    ["companyDescription", "website", "headquarters", "primaryServiceModel", "chainScale", "chainScalesSupported", "totalProperties", "totalRooms", "companyLogo", "brands", "numberOfBrands", "Brands Portfolio Detail"].includes(name);
  const strategyYes =
    consumers.strategyCamel.has(name) ||
    ["dataConfidenceLevel", "Data Confidence Level", "company_name", "companyName", "submission_status"].includes(name);
  return { oas: oasYes ? "Yes" : "No", explorer: explorerYes ? "Yes" : "No", strategy: strategyYes ? "Yes" : "No" };
}

function buildIndexes() {
  const inventory = loadJson(INVENTORY_PATH);
  const staticForms = new Set(inventory.map((r) => r.fieldName));

  const buildSheet = loadJson(BUILD_SHEET_PATH).rows || [];
  const nbByAirtable = new Map();
  const nbByForm = new Map();
  for (const r of buildSheet) {
    nbByAirtable.set(`${r.table_name}::${r.airtable_field_name}`, r);
    nbByForm.set(r.form_name, r);
  }
  let phaseB = { masterWriterHardcoded: [], rows: [] };
  try {
    phaseB = loadJson(PHASE_B_PATH);
  } catch {
    /* optional */
  }
  for (const r of phaseB.masterWriterHardcoded || []) {
    nbByAirtable.set(`${r.table_name}::${r.airtable_field_name}`, r);
    nbByForm.set(r.form_name, r);
  }

  const bindings = loadJson(BINDINGS_PATH);
  const formToBinding = new Map();
  const legacyTitleByForm = new Map();
  for (const b of bindings.bindings || []) {
    for (const fk of b.formKeys || []) {
      formToBinding.set(fk, b);
      legacyTitleByForm.set(fk, b.airtableName);
    }
  }

  const legacyFlat = new Set();
  for (const names of Object.values(getUsedThirdPartyOperatorFieldNamesByTable())) {
    for (const n of names) legacyFlat.add(n);
  }

  for (const [legacyTitle, formKey] of Object.entries(BASICS_AIRTABLE_TO_FORM_KEY)) {
    if (!legacyTitleByForm.has(formKey)) legacyTitleByForm.set(formKey, legacyTitle);
  }

  const formLegacyMapped = new Set();
  const markLegacyForm = (formName) => {
    if (!formName) return;
    if (formName.startsWith("exec_") || JSON_PAYLOAD_FORMS.has(formName)) {
      formLegacyMapped.add(formName);
      return;
    }
    const binding = formToBinding.get(formName);
    if (binding && legacyFlat.has(binding.airtableName)) {
      formLegacyMapped.add(formName);
      return;
    }
    const legacyTitle = legacyTitleByForm.get(formName);
    if (legacyTitle && (legacyFlat.has(legacyTitle) || BASICS_PRIMARY_FIELD_NAMES.has(legacyTitle))) {
      formLegacyMapped.add(formName);
    }
  };

  for (const formName of staticForms) markLegacyForm(formName);
  for (const formName of Object.values(BASICS_AIRTABLE_TO_FORM_KEY)) markLegacyForm(formName);

  const oasInjected = extractOasInjectedFields(fs.readFileSync(OAS_INJECT_PATH, "utf8"));

  /** formName -> { table, field } */
  const formToSchema = new Map();
  const schema = loadJson(SCHEMA_PATH);
  const schemaFieldSet = new Set();
  for (const [key, tableName] of Object.entries(TABLE_KEY_TO_NAME)) {
    const block = schema.tables?.[key];
    if (!block?.fields) continue;
    for (const f of block.fields) {
      if (LINK_FIELD_RE.test(f.name)) continue;
      schemaFieldSet.add(`${tableName}::${f.name}`);
      formToSchema.set(f.name, { table: tableName, field: f.name });
    }
  }
  for (const r of buildSheet) {
    formToSchema.set(r.form_name, { table: r.table_name, field: r.airtable_field_name });
  }
  for (const [formName, alias] of Object.entries(FORM_ALIAS_TO_SCHEMA)) {
    formToSchema.set(formName, { table: alias.table, field: alias.field });
  }
  for (const [legacyTitle, formKey] of Object.entries(BASICS_AIRTABLE_TO_FORM_KEY)) {
    const hit = [...schemaFieldSet].find((k) => k.endsWith(`::${formKey}`));
    if (hit) {
      const [table, field] = hit.split("::");
      formToSchema.set(formKey, { table, field });
    } else if (formKey === "companyName") {
      formToSchema.set(formKey, { table: "Operator Setup - Profile & Positioning", field: "company_name" });
    }
  }

  for (const fk of ["dataConfidenceLevel", "sourceType", "lastUpdatedDate"]) {
    staticForms.add(fk);
  }

  return {
    inventory,
    staticForms,
    buildSheet,
    phaseB,
    nbByAirtable,
    nbByForm,
    formToBinding,
    formLegacyMapped,
    legacyFlat,
    oasInjected,
    bindings,
    formToSchema,
    schema,
  };
}

function collectSchemaFields(schema) {
  const rows = [];
  for (const [key, tableName] of Object.entries(TABLE_KEY_TO_NAME)) {
    const block = schema.tables?.[key];
    if (!block?.fields) continue;
    for (const f of block.fields) {
      if (LINK_FIELD_RE.test(f.name)) continue;
      rows.push({
        table: tableName,
        name: f.name,
        type: f.type || "",
        source: "operator-alignment-5b-schema-backup-2026-05-25.json",
        planned: false,
      });
    }
  }
  for (const pc of schema.plannedCreates || []) {
    const table = TABLE_KEY_TO_NAME[pc.table] || pc.table;
    rows.push({
      table,
      name: pc.name,
      type: pc.type || "",
      source: "plannedCreates (schema backup)",
      planned: true,
    });
  }
  for (const [table, meta] of Object.entries(CHILD_TABLES)) {
    for (const f of meta.fields) {
      rows.push({
        table,
        name: f,
        type: "child",
        source: meta.source,
        planned: false,
        childPayload: meta.payload,
      });
    }
  }
  return rows;
}

function formsForSchemaField(table, fieldName, indexes) {
  const forms = new Set();
  if (indexes.staticForms.has(fieldName)) forms.add(fieldName);
  for (const [formName, nb] of indexes.nbByForm) {
    if (nb.table_name === table && nb.airtable_field_name === fieldName) forms.add(formName);
  }
  for (const [formName, loc] of indexes.formToSchema) {
    if (loc.table === table && loc.field === fieldName) forms.add(formName);
  }
  if (fieldName === "company_name") forms.add("companyName");
  for (const r of indexes.phaseB?.masterWriterHardcoded || []) {
    if (r.table_name === table && r.airtable_field_name === fieldName) forms.add(r.form_name);
  }
  if (fieldName === "Data Confidence Level") forms.add("dataConfidenceLevel");
  if (fieldName === "Source Type") forms.add("sourceType");
  if (fieldName === "Last Updated Date") forms.add("lastUpdatedDate");
  if (fieldName === "companyLogo") forms.add("companyLogo");
  if (fieldName === "Brands Portfolio Detail" || fieldName === "brandsPortfolioDetail") {
    forms.add("brandsPortfolioDetail");
  }
  return [...forms];
}

function isExecForm(name) {
  return /^exec_\d+_/.test(name);
}

function resolveRow(sr, indexes, consumers) {
  const { table, name } = sr;
  const isSystem = SYSTEM_FIELDS.has(name);
  const isGranular = GRANULAR_COLUMNS.has(name);
  const isChild = Boolean(sr.childPayload);
  const isPlanned = sr.planned;

  const staticForm = formsForSchemaField(table, name, indexes).join("; ");

  const oasInjected =
    indexes.oasInjected.has(name) ||
    staticForm.split("; ").some((f) => indexes.oasInjected.has(f.trim()))
      ? "Yes"
      : "No";

  const nbKey = `${table}::${name}`;
  const nbMapped = indexes.nbByAirtable.has(nbKey) ? "Yes" : "No";

  const formNames = staticForm.split("; ").map((s) => s.trim()).filter(Boolean);
  const legacyMapped =
    formNames.some((f) => indexes.formLegacyMapped.has(f)) ||
    (name === "company_name" && indexes.formLegacyMapped.has("companyName"))
      ? "Yes"
      : "No";

  let childJson = "No";
  if (isChild) childJson = "Yes";
  if (["companyLogo", "Brands Portfolio Detail", "brandsPortfolioDetail"].includes(name)) childJson = "Yes";
  if (formNames.some((f) => JSON_PAYLOAD_FORMS.has(f))) childJson = "Yes";

  const sources = new Set([sr.source]);
  if (indexes.nbByAirtable.has(nbKey)) sources.add("operator-setup-new-base-build-sheet-rows.json");
  if (legacyMapped === "Yes") sources.add("third-party-operator-airtable-fields-used.js; operator-setup-write-plan.js");
  if (oasInjected === "Yes") sources.add("oas-inject-form-fields.js");
  if (staticForm) sources.add("he-cala-form-inventory.json");
  if (indexes.formToBinding.has(name)) sources.add("third-party-operator-new-two-field-bindings.json");

  const need = fieldNeededFor(name, consumers);
  for (const f of formNames) {
    const n2 = fieldNeededFor(f, consumers);
    if (n2.oas === "Yes") need.oas = "Yes";
    if (n2.explorer === "Yes") need.explorer = "Yes";
    if (n2.strategy === "Yes") need.strategy = "Yes";
  }

  let status;
  let risk;
  let action;

  if (NEEDS_DECISION_FIELDS.has(name)) {
    status = "Needs Decision";
    risk = "Medium";
    action = "Add UI opt-in control or remove unused schema column";
  } else if (isSystem) {
    status = "Fully Covered";
    risk = "Low";
    action = "System/link field — no UI required";
  } else if (isGranular) {
    status = "Fully Covered";
    risk = "Low";
    action = "Writer-derived from aggregate service multis — not missing UI";
  } else if (isChild && childJson === "Yes") {
    status = "JSON / Child Table Only";
    risk = "Low";
    action = "Saved via JSON payload / repeater — verify writer child replace";
  } else if (nbMapped === "Yes" && (staticForm || oasInjected === "Yes")) {
    status = "Fully Covered";
    risk = "Low";
    action = "In new-base writer build sheet with form or OAS inject";
  } else if (nbMapped === "Yes" && !staticForm) {
    status = "New-Base Writer Only";
    risk = "Medium";
    action = "Writer maps column; no static/OAS form — confirm script/backfill source";
  } else if (legacyMapped === "Yes" && nbMapped === "No" && (staticForm || childJson === "Yes")) {
    status = "Legacy Only";
    risk = need.oas === "Yes" || need.explorer === "Yes" ? "High" : "Medium";
    action = "Extend new-base build sheet + writer (Phase B)";
  } else if (childJson === "Yes" && legacyMapped === "Yes" && nbMapped === "No") {
    status = "JSON / Child Table Only";
    risk = "Low";
    action = "Legacy JSON/file payload — extend new-base writer if Explorer/OAS must read from new-base only";
  } else if (staticForm && nbMapped === "No" && legacyMapped === "No") {
    status = "Static Form Only";
    risk = "High";
    action = "Form field with no legacy or new-base path — add mapping or remove from UI";
  } else if (oasInjected === "Yes" && nbMapped === "No") {
    status = "OAS Injected Only";
    risk = "High";
    action = "Add to build sheet with correct table + column name";
  } else if (isPlanned && nbMapped === "No") {
    status = "Schema Only";
    risk = "High";
    action = "plannedCreates — confirm live column; add build sheet + UI";
  } else if ((need.oas === "Yes" || need.explorer === "Yes" || need.strategy === "Yes") && nbMapped === "No" && childJson === "No") {
    status = "Missing Writer Mapping";
    risk = "High";
    action = "Priority Phase B — add to operator-setup-new-base-build-sheet-rows.json";
  } else if (!staticForm && nbMapped === "No" && legacyMapped === "No") {
    status = "Schema Only";
    risk = "Medium";
    action = "Schema column with no intake path — confirm intentional or deprecate";
  } else if (!staticForm && nbMapped === "Yes") {
    status = "New-Base Writer Only";
    risk = "Medium";
    action = "Writer-only column — optional UI";
  } else {
    status = "Missing UI Control";
    risk = "Medium";
    action = "Review whether field should be exposed or is writer/backfill only";
  }

  return {
    staticForm,
    oasInjected,
    nbMapped,
    legacyMapped,
    childJson,
    sources: [...sources].join("; "),
    need,
    status,
    risk,
    action,
  };
}

function getLiveOptions(fieldName, formNames, indexes) {
  for (const f of formNames) {
    const b = indexes.formToBinding.get(f);
    if (b?.selectOptions?.length) return b.selectOptions.join(" | ");
  }
  const b = indexes.formToBinding.get(fieldName);
  if (b?.selectOptions?.length) return b.selectOptions.join(" | ");
  return "";
}

function addOrphanFormRows(rows, indexes, consumers) {
  const coveredForms = new Set();
  for (const r of rows) {
    for (const f of (r.staticForm || "").split(";")) {
      const t = f.trim();
      if (t) coveredForms.add(t);
    }
  }

  const orphans = [];
  for (const formName of indexes.staticForms) {
    if (coveredForms.has(formName)) continue;
    if (isExecForm(formName)) continue;
    const loc = indexes.formToSchema.get(formName);
    if (loc) continue;

    const nb = indexes.nbByForm.get(formName);
    orphans.push({
      table: nb?.table_name || "(no new-base schema match)",
      name: nb?.airtable_field_name || formName,
      type: nb?.airtable_type || "",
      source: "he-cala-form-inventory.json (orphan form)",
      planned: false,
      staticForm: formName,
    });
  }

  for (const ex of orphans) {
    const resolved = resolveRow(ex, indexes, consumers);
    resolved.staticForm = ex.staticForm;
    rows.push({
      table: ex.table,
      name: ex.name,
      type: ex.type,
      liveOptions: getLiveOptions(ex.name, [ex.staticForm], indexes),
      ...resolved,
    });
  }

  rows.push({
    table: "Operator Setup - Leadership Team Members",
    name: "exec_* (repeater)",
    type: "child",
    source: "he-cala-form-inventory.json; operator-setup-new-base-writer.js",
    planned: false,
    childPayload: CHILD_TABLES["Operator Setup - Leadership Team Members"].payload,
    staticForm: "exec_1..6_*",
    oasInjected: "No",
    nbMapped: "Yes",
    legacyMapped: "Yes",
    childJson: "Yes",
    sources:
      "he-cala-form-inventory.json; operator-setup-new-base-writer.js; third-party-operator-airtable-fields-used.js",
    need: fieldNeededFor("exec_1_name", consumers),
    status: "JSON / Child Table Only",
    risk: "Low",
    action: "Leadership repeater → child table replace on save",
  });
}

function main() {
  const indexes = buildIndexes();
  const consumers = buildConsumerSets();
  const schemaRows = collectSchemaFields(indexes.schema);
  const outRows = [];

  for (const sr of schemaRows) {
    const formNames = formsForSchemaField(sr.table, sr.name, indexes);
    const resolved = resolveRow(sr, indexes, consumers);
    outRows.push({
      table: sr.table,
      name: sr.name,
      type: sr.type,
      liveOptions: getLiveOptions(sr.name, formNames, indexes),
      ...resolved,
    });
  }

  addOrphanFormRows(outRows, indexes, consumers);

  const header = [
    "Airtable Table",
    "Airtable Field",
    "Airtable Field Type",
    "Live Options",
    "Static Form Field Name",
    "OAS Injected?",
    "New-Base Writer Mapped?",
    "Legacy Writer Mapped?",
    "Child Table / JSON Payload?",
    "Source File(s)",
    "Needed For OAS?",
    "Needed For Operator Explorer?",
    "Needed For Operator Strategy?",
    "Coverage Status",
    "Risk Level",
    "Recommended Action",
  ];

  const csvLines = [header.map(csvEscape).join(",")];
  for (const r of outRows) {
    csvLines.push(
      [
        r.table,
        r.name,
        r.type,
        r.liveOptions,
        r.staticForm,
        r.oasInjected,
        r.nbMapped,
        r.legacyMapped,
        r.childJson,
        r.sources,
        r.need.oas,
        r.need.explorer,
        r.need.strategy,
        r.status,
        r.risk,
        r.action,
      ]
        .map(csvEscape)
        .join(",")
    );
  }

  const csvPath = path.join(ROOT, "reports/operator-setup-field-coverage-diff.csv");
  fs.mkdirSync(path.dirname(csvPath), { recursive: true });
  fs.writeFileSync(csvPath, csvLines.join("\n") + "\n", "utf8");

  const counts = {};
  for (const r of outRows) counts[r.status] = (counts[r.status] || 0) + 1;

  const highRisk = outRows.filter((r) => r.risk === "High");
  const oasGaps = outRows.filter(
    (r) =>
      r.need.oas === "Yes" &&
      r.nbMapped === "No" &&
      !SYSTEM_FIELDS.has(r.name) &&
      !GRANULAR_COLUMNS.has(r.name) &&
      r.childJson !== "Yes"
  );
  const explorerGaps = outRows.filter(
    (r) =>
      r.need.explorer === "Yes" &&
      r.nbMapped === "No" &&
      r.legacyMapped === "No" &&
      !GRANULAR_COLUMNS.has(r.name) &&
      r.childJson !== "Yes"
  );
  const strategyGaps = outRows.filter(
    (r) => r.need.strategy === "Yes" && r.nbMapped === "No" && !SYSTEM_FIELDS.has(r.name)
  );
  const legacyOnly = outRows.filter((r) => r.status === "Legacy Only");
  const missingWriter = outRows.filter((r) => r.status === "Missing Writer Mapping");
  const staticOnly = outRows.filter((r) => r.status === "Static Form Only");
  const schemaOnly = outRows.filter((r) => r.status === "Schema Only");
  const uiGaps = outRows.filter(
    (r) => r.status === "Missing UI Control" || r.status === "Needs Decision"
  );

  const top10 = [...highRisk]
    .sort((a, b) => {
      const score = (r) =>
        (r.need.oas === "Yes" ? 4 : 0) +
        (r.need.explorer === "Yes" ? 2 : 0) +
        (r.need.strategy === "Yes" ? 1 : 0) +
        (r.status === "Legacy Only" ? 1 : 0);
      return score(b) - score(a);
    })
    .slice(0, 10);

  const md = buildMarkdown({
    counts,
    total: outRows.length,
    staticFormCount: indexes.staticForms.size,
    buildSheetCount: indexes.buildSheet.length,
    top10,
    oasGaps,
    explorerGaps,
    strategyGaps,
    legacyOnly,
    missingWriter,
    staticOnly,
    schemaOnly,
    uiGaps,
  });

  const mdPath = path.join(ROOT, "docs/operator-setup-field-coverage-diff.md");
  fs.writeFileSync(mdPath, md, "utf8");

  console.log("Wrote", csvPath);
  console.log("Wrote", mdPath);
  console.log("Total rows:", outRows.length);
  console.log("Status counts:", counts);
  console.log("High-risk rows:", highRisk.length);
  console.log("Legacy Only:", legacyOnly.length);
  console.log("Static Form Only:", staticOnly.length);
  console.log("OAS-needed, not new-base mapped:", oasGaps.length);
}

function buildMarkdown(ctx) {
  const {
    counts,
    total,
    staticFormCount,
    buildSheetCount,
    top10,
    oasGaps,
    explorerGaps,
    strategyGaps,
    legacyOnly,
    missingWriter,
    staticOnly,
    uiGaps,
  } = ctx;

  const proceedB =
    legacyOnly.length > 50 || oasGaps.length > 0 || missingWriter.length > 30
      ? "**Phase B should proceed** before enabling `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` in production. Close Legacy Only and Missing Writer Mapping rows first (especially OAS inject fields, `dataConfidenceLevel`, profile/footprint, and `bf_*` / `brand_signal_*` columns)."
      : "Phase B can proceed in parallel with Operator Strategy UX, but is still required before retiring the legacy writer.";

  return `# Operator Setup Field Coverage Diff (Phase A)

Audit-only artifact generated by \`scripts/generate-operator-setup-field-coverage-diff.mjs\`.  
**Do not** treat this as permission to change Airtable schema, scoring, or PDF layouts.

Companion CSV: [reports/operator-setup-field-coverage-diff.csv](../reports/operator-setup-field-coverage-diff.csv)

## Executive summary

Operator Setup intake exposes **${staticFormCount}** static HTML form fields (\`scripts/he-cala-form-inventory.json\`), while the committed new-base writer build sheet maps **${buildSheetCount}** form keys to new-base Airtable columns. This audit catalogs **${total}** rows: every new-base schema field (from \`reports/operator-alignment-5b-schema-backup-2026-05-25.json\`), child-table columns, plannedCreates, plus orphan form fields with no schema match.

**${legacyOnly.length}** fields are **Legacy Only** (form + legacy writer path, not in the 103-row build sheet). **${staticOnly.length}** form fields have **no** legacy or new-base persistence path in this audit. Explorer, OAS companies API, and Operator Strategy read **new-base** tables — so Legacy Only and Static Form Only rows explain empty or stale downstream data while \`OPERATOR_SETUP_USE_NEW_BASE_WRITER=0\` remains the default.

${proceedB}

## Summary counts

| Coverage Status | Count |
|-----------------|------:|
${Object.entries(counts)
  .sort((a, b) => b[1] - a[1])
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

| Metric | Value |
|--------|------:|
| Static form fields (inventory) | ${staticFormCount} |
| New-base build sheet rows | ${buildSheetCount} |
| Total CSV audit rows | ${total} |
| Legacy Only | ${legacyOnly.length} |
| Static Form Only (no writer path) | ${staticOnly.length} |
| Missing Writer Mapping | ${missingWriter.length} |
| OAS-needed, not new-base mapped | ${oasGaps.length} |
| Explorer consumer, not new-base mapped | ${explorerGaps.length} |
| Strategy consumer, not new-base mapped | ${strategyGaps.length} |

## Top 10 high-priority gaps

| Table | Field | Status | OAS | Explorer | Strategy | Recommended action |
|-------|-------|--------|-----|----------|----------|-------------------|
${top10
  .map(
    (r) =>
      `| ${r.table} | ${r.name} | ${r.status} | ${r.need.oas} | ${r.need.explorer} | ${r.need.strategy} | ${String(r.action).slice(0, 100)} |`
  )
  .join("\n")}

## Fields needed for OAS (Operator Setup side)

OAS company scoring reads **new-base** via \`loadActiveOperatorCandidatesForAlignment\` + \`buildPrefillObjectFromNewBaseRows\`.

**P0 columns:** \`dataConfidenceLevel\`, \`sourceType\`, \`lastUpdatedDate\`, OAS inject fields on Platform (\`activeCountries\`, \`activeMarkets\`, \`serviceModelsSupported\`, …), Commercial \`bf_*\`, Profile \`brand_signal_*\`, Platform \`cap_kpi_*\`, \`chainScale\`, \`brands\`, \`primaryServiceModel\`, Master \`submission_status\` (Active gate).

**Computed read (not a column):** \`regionsSupported\` is derived from \`geo_*\` totals in \`operator-setup-new-base-read.js\` — form \`regions\` maps to legacy **Regions Supported** only.

**Gap count (OAS-needed, not new-base mapped, excluding child JSON):** ${oasGaps.length}

Deal-side OAS inject (Deal Intake) is **out of scope** for this CSV.

## Fields needed for Operator Explorer

List API: \`company_name\`, \`website\`, \`headquarters\`, \`companyDescription\`, \`primaryServiceModel\`, \`brands\`, \`totalProperties\`, \`totalRooms\`, \`chainScale\`, \`companyLogo\`, case study / diligence counts.

Detail: \`overview_*\`, \`cap_*\`, \`geo_*\`, leadership children, case studies.

**Gap count:** ${explorerGaps.length} rows (consumer Yes, new-base writer No, not child JSON).

## Fields needed for Operator Strategy

Per-company rows from \`GET /api/operator-alignment-snapshot/:dealId/companies\` need Master id, \`companyName\`, \`dataConfidenceLevel\`, and populated profile/footprint for scoring.

**Gap count:** ${strategyGaps.length}

## Legacy-only fields

**${legacyOnly.length}** rows: static form (or JSON payload) persists via legacy 9-table writer (\`operator-setup-write-plan.js\`, \`third-party-operator-airtable-fields-used.js\`) but **not** in \`operator-setup-new-base-build-sheet-rows.json\`. Primary Phase B backlog.

## New-base writer gaps

**${missingWriter.length}** rows flagged **Missing Writer Mapping** (downstream consumer needs data; no build sheet row).

Regenerate build sheet: \`node scripts/generate-operator-setup-build-sheet-rows.mjs\` (requires Airtable meta).

## UI gaps

| Item | Notes |
|------|--------|
| \`dealTermsOptIn\` | **Needs Decision** — schema on Commercial, no form control |
| \`diligenceQaOptIn\` | **Needs Decision** — schema on Governance, no form control |
| Granular governance service columns | **Not missing UI** — derived from aggregate multis |
| \`exec_*\` | **JSON / Child Table Only** — single audit row |
| \`regions\` | Legacy **Regions Supported**; new-base uses \`geo_*\` + computed \`regionsSupported\` |

**${uiGaps.length}** rows: Missing UI Control or Needs Decision.

## Recommended Phase B writer-extension plan

1. Filter CSV: \`Coverage Status\` ∈ {Legacy Only, Missing Writer Mapping, OAS Injected Only, Static Form Only} and \`Risk Level\` = High.
2. Extend \`operator-setup-new-base-build-sheet-rows.json\` (target: cover all Legacy Only profile/platform/commercial/governance fields used by OAS + Explorer list).
3. Confirm \`plannedCreates\` columns in live base; add writer rows.
4. Resolve \`regions\` vs \`geo_*\` / \`specificMarkets\` explicitly in mapping doc.
5. Staging: \`OPERATOR_SETUP_USE_NEW_BASE_WRITER=1\`; save sample operator → list API → OAS companies → Strategy table.
6. Backfill Active masters from HE/CALA inventory or dedicated script.
7. Decide \`dealTermsOptIn\` / \`diligenceQaOptIn\` UI vs schema removal.

## Phase B proceed / no-go

${proceedB}

## Regenerate

\`\`\`bash
node scripts/generate-operator-setup-field-coverage-diff.mjs
\`\`\`

---

*Generated ${new Date().toISOString().slice(0, 10)}. See [operator-side-system-comparison.md](./operator-side-system-comparison.md).*
`;
}

main();
