#!/usr/bin/env node
/**
 * Pre-Phase E: Operator Setup → Explorer field mapping audit (read-only).
 * Outputs:
 *   reports/operator-setup-to-explorer-field-mapping-audit.csv
 *   docs/operator-setup-to-explorer-field-mapping-audit.md
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
const PROFILE_JS_PATH = path.join(ROOT, "public/js/operator-explorer-new-base-profile.js");
const COVERAGE_CSV_PATH = path.join(ROOT, "reports/operator-setup-field-coverage-diff.csv");

const TABLE_KEY_TO_NAME = {
  master: "Operator Setup - Master",
  profile: "Operator Setup - Profile & Positioning",
  platform: "Operator Setup - Platform & Markets",
  commercial: "Operator Setup - Commercial Fit & Terms",
  governance: "Operator Setup - Governance, Delivery & Diligence",
};

const SETUP_TABS_ORDER = [
  "0. Company Profile",
  "1. Positioning & Snapshot",
  "2. Operating Platform",
  "3. Brand & Relationships",
  "4. Markets & Footprint",
  "5. Owner Value & Engagement",
  "6. Infrastructure & Data",
  "7. Risk & Compliance",
  "8. Leadership & Team",
  "9. Best Fit & Preferences",
  "10. Deal Terms",
  "11. Proof & Track Record",
  "12. Diligence",
];

const TAB_ALIASES = {
  "Company Profile": "0. Company Profile",
  "Positioning & Snapshot": "1. Positioning & Snapshot",
  "Operating Platform": "2. Operating Platform",
  "Brand & Relationships": "3. Brand & Relationships",
  "Markets & Footprint": "4. Markets & Footprint",
  "Owner Value & Engagement": "5. Owner Value & Engagement",
  "Infrastructure & Data": "6. Infrastructure & Data",
  "Risk & Compliance": "7. Risk & Compliance",
  "Leadership & Team": "8. Leadership & Team",
  "Best Fit & Preferences": "9. Best Fit & Preferences",
  "Deal Terms": "10. Deal Terms",
  "Proof & Track Record": "11. Proof & Track Record",
  Diligence: "12. Diligence",
};

const CHILD_TABLES = {
  "Operator Setup - Leadership Team Members": {
    fields: ["name", "title", "role", "summary", "bio", "headshot", "display_order"],
    payload: "exec_1..6_*",
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
    payload: "caseStudiesDetail",
  },
  "Operator Setup - Diligence QA": {
    fields: ["category", "question", "answer", "display_order"],
    payload: "ownerDiligenceQa",
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
  "operatorId",
  "submission_status",
  "created_at",
  "updated_at",
  "Operator",
]);

const NEEDS_DECISION_FIELDS = new Set(["dealTermsOptIn", "diligenceQaOptIn"]);

const GRANULAR_COLUMNS = new Set();
for (const cat of OPERATOR_SERVICE_GRANULAR) {
  for (const c of cat.columns) GRANULAR_COLUMNS.add(c.col);
}

/** Phase D snapshot rail sections (form/camel keys). */
const EXPLORER_SECTION_FIELDS = {
  "A. Profile Snapshot": [
    "companyName",
    "company_name",
    "companyDescription",
    "website",
    "headquarters",
    "parentCompany",
    "platform",
    "dataConfidenceLevel",
    "lastUpdatedDate",
    "yearEstablished",
    "yearsInBusiness",
  ],
  "B. Market Presence": [
    "activeCountries",
    "activeMarkets",
    "marketPresenceType",
    "regions",
    "regionsSupported",
    "specificMarkets",
  ],
  "C. Operating Profile": [
    "serviceModelsSupported",
    "chainScalesSupported",
    "managementStructuresSupported",
    "minimumKeyCount",
    "primaryServiceModel",
  ],
  "D. Services & Platform": [
    "offeredServices",
    "revenueManagementCapability",
    "salesPlatform",
    "fbCapabilityLevel",
    "fBCapabilityLevel",
  ],
  "E. Opening / Transition Support": [
    "newBuildOpeningExperience",
    "conversionReflagExperience",
    "preOpeningSupportCapability",
  ],
  "F. Owner Reporting & Governance": [
    "ownerReportingLevel",
    "governanceCadence",
    "sourceType",
    "dataConfidenceLevel",
  ],
  "G. Brand / Portfolio Experience": [
    "brandFamiliesOperated",
    "softBrandLifestyleExperience",
    "brandsPortfolioDetail",
    "similarProjectCaseStudies",
    "brands",
    "numberOfBrands",
  ],
};

const P1_FIELDS = [
  { table: "Operator Setup - Master", field: "company_name", form: "companyName" },
  { table: "Operator Setup - Master", field: "operator_id", form: "operatorId", systemField: true },
  { table: "Operator Setup - Master", field: "Data Confidence Level", form: "dataConfidenceLevel" },
  { table: "Operator Setup - Master", field: "Source Type", form: "sourceType" },
  { table: "Operator Setup - Master", field: "Last Updated Date", form: "lastUpdatedDate" },
  { table: "Operator Setup - Platform & Markets", field: "Active Countries", form: "activeCountries" },
  { table: "Operator Setup - Platform & Markets", field: "Active Markets / Cities", form: "activeMarkets" },
  { table: "Operator Setup - Platform & Markets", field: "Market Presence Type", form: "marketPresenceType" },
  { table: "Operator Setup - Platform & Markets", field: "Service Models Supported", form: "serviceModelsSupported" },
  { table: "Operator Setup - Profile & Positioning", field: "chainScalesSupported", form: "chainScalesSupported" },
  { table: "Operator Setup - Platform & Markets", field: "Management Structures Supported", form: "managementStructuresSupported" },
  { table: "Operator Setup - Platform & Markets", field: "Offered Services", form: "offeredServices" },
  { table: "Operator Setup - Platform & Markets", field: "New-Build Opening Experience", form: "newBuildOpeningExperience" },
  { table: "Operator Setup - Platform & Markets", field: "Pre-Opening Support Capability", form: "preOpeningSupportCapability" },
  { table: "Operator Setup - Governance, Delivery & Diligence", field: "Owner Reporting Level", form: "ownerReportingLevel" },
  { table: "Operator Setup - Platform & Markets", field: "Revenue Management Capability", form: "revenueManagementCapability" },
  { table: "Operator Setup - Platform & Markets", field: "Sales Platform", form: "salesPlatform" },
  { table: "Operator Setup - Platform & Markets", field: "F&B Capability Level", form: "fbCapabilityLevel" },
];

const P2_FIELDS = [
  { form: "companyDescription" },
  { form: "website" },
  { form: "headquarters" },
  { form: "companyLogo" },
  { form: "brandFamiliesOperated" },
  { form: "softBrandLifestyleExperience" },
  { form: "similarProjectCaseStudies" },
  { form: "brandsPortfolioDetail" },
  { form: "governanceCadence" },
  { form: "minimumKeyCount" },
];

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
  while ((m = re.exec(js)) !== null) names.add(m[1]);
  return names;
}

function extractProfileModuleKeys(js) {
  const keys = new Set();
  const re = /"([a-zA-Z0-9_]+)"/g;
  const block = js.match(/NEW_BASE_EXPLORER_PREFILL_KEYS\s*=\s*\[([\s\S]*?)\];/);
  if (block) {
    let m;
    while ((m = re.exec(block[1])) !== null) keys.add(m[1]);
  }
  return keys;
}

function buildConsumerSets() {
  const oasCamel = new Set(Object.keys(OAS_OPERATOR_PREFILL_KEY_ALIASES));
  const oasTitles = new Set();
  for (const aliases of Object.values(OAS_OPERATOR_PREFILL_KEY_ALIASES)) {
    for (const a of aliases) oasTitles.add(a);
  }
  const explorerCamel = new Set();
  for (const keys of Object.values(EXPLORER_SECTION_FIELDS)) {
    for (const k of keys) explorerCamel.add(k);
  }
  for (const k of extractProfileModuleKeys(fs.readFileSync(PROFILE_JS_PATH, "utf8"))) {
    explorerCamel.add(k);
  }
  const legacyExplorerPrefixes = [
    "overview_",
    "cap_",
    "brand_",
    "mkt_",
    "geo_",
    "ov_",
    "infra_",
    "risk_",
    "lead_",
    "bf_",
    "tr_",
    "systems_",
    "exec_",
    "brand_signal_",
  ];
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
  return { oasCamel, oasTitles, explorerCamel, strategyCamel, legacyExplorerPrefixes };
}

function explorerTabForField(name, formNames) {
  const n = String(name);
  const f = formNames[0] || n;
  if (/^overview_/.test(n) || /^overview_/.test(f)) return "Profile & Positioning";
  if (/^cap_/.test(n) || /^cap_/.test(f)) return "Operating Platform";
  if (/^brand_/.test(n) || /^brand_signal_/.test(n) || /^brand_/.test(f)) return "Brand & Relationships";
  if (/^mkt_/.test(n) || /^geo_/.test(n) || ["specificMarkets", "regions", "activeCountries", "activeMarkets", "marketPresenceType"].includes(f))
    return "Markets & Footprint";
  if (/^ov_/.test(n) || /^ov_/.test(f) || /ownerReporting|governanceCadence|reportingFrequency/i.test(f))
    return "Owner Engagement & Reporting";
  if (/^infra_/.test(n) || /^systems_/.test(n)) return "Infrastructure & Data";
  if (/^risk_/.test(n)) return "Risk, Compliance & ESG";
  if (/^lead_/.test(n) || /^exec_/.test(n) || f === "keyLeadership") return "Leadership";
  if (/^bf_/.test(n) || /^bf_/.test(f)) return "Project Fit & Deal Profile";
  if (/^tr_/.test(n) || f === "caseStudiesDetail") return "Proof & Track Record";
  for (const [section, keys] of Object.entries(EXPLORER_SECTION_FIELDS)) {
    if (keys.includes(n) || keys.includes(f)) return section;
  }
  if (["companyDescription", "website", "headquarters", "companyName", "company_name", "primaryServiceModel", "chainScalesSupported", "propertyTypes", "companyTagline"].includes(f))
    return "A. Profile Snapshot";
  return "";
}

function explorerSectionForField(name, formNames) {
  const n = String(name);
  const forms = formNames.length ? formNames : [n];
  for (const f of forms) {
    for (const [section, keys] of Object.entries(EXPLORER_SECTION_FIELDS)) {
      if (keys.includes(f) || keys.includes(n)) return section;
    }
  }
  const tab = explorerTabForField(n, forms);
  if (tab.startsWith("A.")) return tab;
  if (tab && !tab.includes(".")) return `Legacy tab: ${tab}`;
  return "";
}

function displayedInExplorer(name, formNames, resolved, profileKeys) {
  const forms = formNames.length ? formNames : [name];
  const inRail = forms.some((f) => profileKeys.has(f)) || profileKeys.has(name);
  const legacyTab = explorerTabForField(name, forms);
  const hasLegacyTab = legacyTab && !legacyTab.startsWith("A.");

  if (inRail && (resolved.nbMapped === "Yes" || resolved.legacyMapped === "Yes" || resolved.childJson === "Yes"))
    return { display: "Yes", source: "Live (Phase D rail + prefill)" };
  if (inRail && resolved.staticForm) return { display: "Partial", source: "Phase D rail (save path gap)" };
  if (hasLegacyTab && (resolved.legacyMapped === "Yes" || resolved.nbMapped === "Yes"))
    return { display: "Partial", source: `Legacy Explorer tab: ${legacyTab}` };
  if (resolved.need.explorer === "Yes" && resolved.childJson === "Yes")
    return { display: "Partial", source: "Child table / JSON in Explorer" };
  if (resolved.need.explorer === "Yes") return { display: "No", source: "Consumer expects field; not rendered" };
  return { display: "No", source: "Not an Explorer consumer field" };
}

function systemDerivedAdmin(name, resolved, isSystem, isGranular, isChild) {
  if (isSystem) return "Yes — System / link";
  if (isGranular) return "Yes — Writer-derived granular service columns";
  if (isChild) return "Yes — Child table / JSON payload";
  if (resolved.status === "Writer-Derived" || GRANULAR_COLUMNS.has(name)) return "Yes — Writer-derived";
  if (["dataConfidenceLevel", "Source Type", "Last Updated Date", "submission_status"].includes(name))
    return "Yes — Admin / ops metadata";
  if (name.startsWith("geo_") && resolved.nbMapped === "Yes") return "Yes — Aggregate geo grid (writer)";
  return "No";
}

function buildFormToTab(inventory) {
  const m = new Map();
  for (const row of inventory) {
    const tab = TAB_ALIASES[row.tab] || row.tab;
    m.set(row.fieldName, tab);
  }
  m.set("dataConfidenceLevel", "0. Company Profile");
  m.set("sourceType", "0. Company Profile");
  m.set("lastUpdatedDate", "0. Company Profile");
  for (const f of [
    "activeCountries",
    "activeMarkets",
    "marketPresenceType",
    "serviceModelsSupported",
    "managementStructuresSupported",
    "offeredServices",
    "newBuildOpeningExperience",
    "preOpeningSupportCapability",
    "revenueManagementCapability",
    "salesPlatform",
    "fbCapabilityLevel",
    "conversionReflagExperience",
  ])
    m.set(f, "4. Markets & Footprint");
  for (const f of ["ownerReportingLevel", "governanceCadence", "ownerReportingCadence", "reportingFrequency"])
    m.set(f, "5. Owner Value & Engagement");
  return m;
}

// --- Reuse core indexing from coverage diff (condensed) ---
function buildIndexes() {
  const inventory = loadJson(INVENTORY_PATH);
  const staticForms = new Set(inventory.map((r) => r.fieldName));
  const formToTab = buildFormToTab(inventory);

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
    /* */
  }
  for (const r of phaseB.masterWriterHardcoded || []) {
    nbByAirtable.set(`${r.table_name}::${r.airtable_field_name}`, r);
    nbByForm.set(r.form_name, r);
  }
  for (const r of phaseB.rows || []) {
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
  const schema = loadJson(SCHEMA_PATH);

  return {
    inventory,
    staticForms,
    formToTab,
    buildSheet,
    nbByAirtable,
    nbByForm,
    formLegacyMapped,
    legacyFlat,
    oasInjected,
    formToBinding,
    schema,
    phaseB,
  };
}

function fieldNeededFor(name, consumers) {
  const oasYes =
    consumers.oasCamel.has(name) ||
    consumers.oasTitles.has(name) ||
    name.startsWith("bf_") ||
    name.startsWith("cap_") ||
    name.startsWith("brand_signal_") ||
    ["submission_status", "dataConfidenceLevel", "Data Confidence Level", "Source Type", "Last Updated Date"].includes(name);
  const explorerYes =
    consumers.explorerCamel.has(name) ||
    consumers.legacyExplorerPrefixes.some((p) => name.startsWith(p)) ||
    ["companyDescription", "website", "headquarters", "primaryServiceModel", "chainScale", "chainScalesSupported", "totalProperties", "totalRooms", "companyLogo", "brands", "numberOfBrands"].includes(name);
  const strategyYes =
    consumers.strategyCamel.has(name) ||
    ["dataConfidenceLevel", "Data Confidence Level", "company_name", "companyName", "submission_status"].includes(name);
  return { oas: oasYes ? "Yes" : "No", explorer: explorerYes ? "Yes" : "No", strategy: strategyYes ? "Yes" : "No" };
}

function formsForSchemaField(table, fieldName, indexes) {
  const forms = new Set();
  if (indexes.staticForms.has(fieldName)) forms.add(fieldName);
  for (const [formName, nb] of indexes.nbByForm) {
    if (nb.table_name === table && nb.airtable_field_name === fieldName) forms.add(formName);
  }
  if (fieldName === "company_name") forms.add("companyName");
  if (fieldName === "Data Confidence Level") forms.add("dataConfidenceLevel");
  if (fieldName === "Source Type") forms.add("sourceType");
  if (fieldName === "Last Updated Date") forms.add("lastUpdatedDate");
  if (fieldName === "companyLogo") forms.add("companyLogo");
  if (fieldName === "Brands Portfolio Detail") forms.add("brandsPortfolioDetail");
  return [...forms];
}

function resolveRow(sr, indexes, consumers) {
  const { table, name } = sr;
  const isSystem = SYSTEM_FIELDS.has(name);
  const isGranular = GRANULAR_COLUMNS.has(name);
  const isChild = Boolean(sr.childPayload);

  const formNames = formsForSchemaField(table, name, indexes);
  const staticForm = formNames.join("; ");

  const setupTabs = [
    ...new Set(
      formNames.map((f) => indexes.formToTab.get(f)).filter(Boolean)
    ),
  ].join("; ");

  const oasInjected =
    indexes.oasInjected.has(name) || formNames.some((f) => indexes.oasInjected.has(f)) ? "Yes" : "No";

  const nbKey = `${table}::${name}`;
  const nbMapped = indexes.nbByAirtable.has(nbKey) ? "Yes" : "No";

  const legacyMapped =
    formNames.some((f) => indexes.formLegacyMapped.has(f)) ||
    (name === "company_name" && indexes.formLegacyMapped.has("companyName"))
      ? "Yes"
      : "No";

  let childJson = "No";
  if (isChild) childJson = "Yes";
  if (["companyLogo", "Brands Portfolio Detail", "brandsPortfolioDetail"].includes(name)) childJson = "Yes";
  if (formNames.some((f) => JSON_PAYLOAD_FORMS.has(f))) childJson = "Yes";

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

  if (NEEDS_DECISION_FIELDS.has(name) || formNames.some((f) => NEEDS_DECISION_FIELDS.has(f))) {
    status = "Needs Decision";
    risk = "Medium";
    action = "Product decision before UI/writer mapping";
  } else if (isSystem) {
    status = "System Field";
    risk = "Low";
    action = "No operator UI — system/link/metadata";
  } else if (isGranular) {
    status = "Writer-Derived";
    risk = "Low";
    action = "Derived from aggregate service multi-selects";
  } else if (isChild && childJson === "Yes") {
    status = "JSON / Child Table Only";
    risk = "Low";
    action = "Saved via child table or JSON payload";
  } else if (nbMapped === "Yes" && (staticForm || oasInjected === "Yes")) {
    status = "Fully Covered";
    risk = "Low";
    action = "UI + new-base writer (+ Explorer when consumer)";
  } else if (nbMapped === "Yes" && !staticForm && !oasInjected) {
    status = "New-Base Writer Only";
    risk = "Medium";
    action = "Writer/backfill only — add UI if operators must edit";
  } else if (legacyMapped === "Yes" && nbMapped === "No" && (staticForm || childJson === "Yes")) {
    status = "Legacy Only";
    risk = need.oas === "Yes" || need.explorer === "Yes" ? "High" : "Medium";
    action = "Phase B2 — extend new-base writer";
  } else if (staticForm && nbMapped === "No" && legacyMapped === "No") {
    status = "Static Form Only";
    risk = "High";
    action = "Phase Setup UI + B2 writer mapping";
  } else if (oasInjected === "Yes" && nbMapped === "No") {
    status = "OAS Injected Only";
    risk = "High";
    action = "Add build-sheet row + writer";
  } else if (!staticForm && nbMapped === "No" && legacyMapped === "No") {
    status = "Schema Only";
    risk = "Medium";
    action = "Confirm intentional or deprecate";
  } else {
    status = "Setup Only";
    risk = "Medium";
    action = "Review UI vs downstream consumer";
  }

  return {
    staticForm,
    setupTabs,
    oasInjected,
    nbMapped,
    legacyMapped,
    childJson,
    need,
    status,
    risk,
    action,
  };
}

function collectSchemaFields(schema) {
  const rows = [];
  const LINK_FIELD_RE = /^Operator Setup - /;
  for (const [key, tableName] of Object.entries(TABLE_KEY_TO_NAME)) {
    const block = schema.tables?.[key];
    if (!block?.fields) continue;
    for (const f of block.fields) {
      if (LINK_FIELD_RE.test(f.name)) continue;
      rows.push({ table: tableName, name: f.name, type: f.type || "", options: (f.options || []).join(" | ") });
    }
  }
  for (const pc of schema.plannedCreates || []) {
    const table = TABLE_KEY_TO_NAME[pc.table] || pc.table;
    rows.push({ table, name: pc.name, type: pc.type || "", options: "", planned: true });
  }
  for (const [table, meta] of Object.entries(CHILD_TABLES)) {
    for (const f of meta.fields) {
      rows.push({
        table,
        name: f,
        type: "child",
        options: "",
        childPayload: meta.payload,
      });
    }
  }
  return rows;
}

function getLiveOptions(fieldName, formNames, indexes) {
  for (const f of formNames) {
    const b = indexes.formToBinding.get(f);
    if (b?.selectOptions?.length) return b.selectOptions.join(" | ");
  }
  return "";
}

function main() {
  const indexes = buildIndexes();
  const consumers = buildConsumerSets();
  const profileKeys = extractProfileModuleKeys(fs.readFileSync(PROFILE_JS_PATH, "utf8"));
  const schemaRows = collectSchemaFields(indexes.schema);
  const outRows = [];

  for (const sr of schemaRows) {
    const formNames = formsForSchemaField(sr.table, sr.name, indexes);
    const resolved = resolveRow({ ...sr, childPayload: sr.childPayload }, indexes, consumers);
    const explorerSection = explorerSectionForField(sr.name, formNames);
    const explorerTab = explorerTabForField(sr.name, formNames);
    const display = displayedInExplorer(sr.name, formNames, resolved, profileKeys);
    const sys = systemDerivedAdmin(
      sr.name,
      resolved,
      SYSTEM_FIELDS.has(sr.name),
      GRANULAR_COLUMNS.has(sr.name),
      Boolean(sr.childPayload)
    );

    let explorerParts = [];
    if (explorerSection) explorerParts.push(explorerSection);
    if (explorerTab && !explorerTab.startsWith("A.")) explorerParts.push(`Tab: ${explorerTab}`);
    if (display.display === "Yes" && display.source.includes("Phase D")) explorerParts.push("H. Alignment Context (dealId)");

    outRows.push({
      table: sr.table,
      name: sr.name,
      type: sr.type,
      options: sr.options || getLiveOptions(sr.name, formNames, indexes),
      setupTabs: resolved.setupTabs || (formNames[0] ? indexes.formToTab.get(formNames[0]) || "" : ""),
      staticForm: resolved.staticForm,
      oasInjected: resolved.oasInjected,
      nbMapped: resolved.nbMapped,
      legacyMapped: resolved.legacyMapped,
      childJson: resolved.childJson,
      explorerSection: explorerParts.join(" | ") || "—",
      displayedExplorer: display.display,
      displaySource: display.source,
      need: resolved.need,
      sysDerived: sys,
      status: resolved.status,
      risk: resolved.risk,
      action: resolved.action,
    });
  }

  // Orphan inventory forms
  const covered = new Set(outRows.flatMap((r) => r.staticForm.split(";").map((s) => s.trim()).filter(Boolean)));
  for (const row of indexes.inventory) {
    if (covered.has(row.fieldName)) continue;
    if (/^exec_\d+_/.test(row.fieldName)) continue;
    const need = fieldNeededFor(row.fieldName, consumers);
    const nb = indexes.nbByForm.get(row.fieldName);
    const resolved = {
      staticForm: row.fieldName,
      setupTabs: indexes.formToTab.get(row.fieldName) || "",
      oasInjected: indexes.oasInjected.has(row.fieldName) ? "Yes" : "No",
      nbMapped: nb ? "Yes" : "No",
      legacyMapped: indexes.formLegacyMapped.has(row.fieldName) ? "Yes" : "No",
      childJson: JSON_PAYLOAD_FORMS.has(row.fieldName) ? "Yes" : "No",
      need,
      status: "Static Form Only",
      risk: "High",
      action: "Orphan form — map to schema or remove",
    };
    if (nb) resolved.status = resolved.nbMapped === "Yes" ? "Fully Covered" : resolved.status;
    const display = displayedInExplorer(row.fieldName, [row.fieldName], resolved, profileKeys);
    outRows.push({
      table: nb?.table_name || "(orphan)",
      name: nb?.airtable_field_name || row.fieldName,
      type: nb?.airtable_type || "",
      options: getLiveOptions(row.fieldName, [row.fieldName], indexes),
      setupTabs: resolved.setupTabs,
      staticForm: row.fieldName,
      oasInjected: resolved.oasInjected,
      nbMapped: resolved.nbMapped,
      legacyMapped: resolved.legacyMapped,
      childJson: resolved.childJson,
      explorerSection: explorerSectionForField(row.fieldName, [row.fieldName]) || "—",
      displayedExplorer: display.display,
      displaySource: display.source,
      need,
      sysDerived: "No",
      status: resolved.status,
      risk: resolved.risk,
      action: resolved.action,
    });
  }

  const header = [
    "Airtable Table",
    "Airtable Field",
    "Airtable Field Type",
    "Live Options",
    "Operator Setup UI Tab",
    "Operator Setup Form Field Name",
    "OAS Injected?",
    "New-Base Writer Mapped?",
    "Legacy Writer Mapped?",
    "Child Table / JSON Payload?",
    "Operator Explorer Section / Tab",
    "Displayed in Operator Explorer?",
    "Explorer Display Source",
    "Used by OAS?",
    "Used by Operator Strategy?",
    "System / Derived / Admin-only?",
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
        r.options,
        r.setupTabs,
        r.staticForm,
        r.oasInjected,
        r.nbMapped,
        r.legacyMapped,
        r.childJson,
        r.explorerSection,
        r.displayedExplorer,
        r.displaySource,
        r.need.oas,
        r.need.strategy,
        r.sysDerived,
        r.status,
        r.risk,
        r.action,
      ]
        .map(csvEscape)
        .join(",")
    );
  }

  const csvPath = path.join(ROOT, "reports/operator-setup-to-explorer-field-mapping-audit.csv");
  fs.writeFileSync(csvPath, csvLines.join("\n") + "\n", "utf8");

  const counts = {};
  for (const r of outRows) counts[r.status] = (counts[r.status] || 0) + 1;

  const p1Gaps = [];
  for (const p1 of P1_FIELDS) {
    const hit =
      outRows.find((r) => r.table === p1.table && (r.name === p1.field || r.staticForm.includes(p1.form))) ||
      outRows.find((r) => r.staticForm === p1.form);
    if (!hit) {
      p1Gaps.push({ ...p1, issue: "Not found in audit rows" });
      continue;
    }
    const issues = [];
    if (hit.nbMapped !== "Yes" && hit.legacyMapped !== "Yes" && hit.childJson !== "Yes" && !hit.sysDerived.startsWith("Yes — System"))
      issues.push("not saved to new-base/legacy");
    if (hit.displayedExplorer === "No" && hit.need.explorer === "Yes") issues.push("not displayed in Explorer");
    if (!hit.setupTabs && !p1.systemField && hit.oasInjected !== "Yes") issues.push("no Setup UI tab");
    if (issues.length) p1Gaps.push({ ...p1, field: hit.name, status: hit.status, issues: issues.join("; ") });
  }

  const md = buildMarkdown({ outRows, counts, indexes, p1Gaps, profileKeys });
  const mdPath = path.join(ROOT, "docs/operator-setup-to-explorer-field-mapping-audit.md");
  fs.writeFileSync(mdPath, md, "utf8");

  console.log("Wrote", csvPath);
  console.log("Wrote", mdPath);
  console.log("Total rows:", outRows.length);
  console.log("Status counts:", counts);
  console.log("P1 gaps:", p1Gaps.length);
}

function buildTabSummary(outRows, indexes) {
  const byTab = new Map();
  for (const t of SETUP_TABS_ORDER) {
    byTab.set(t, { ui: new Set(), nb: new Set(), legacy: new Set(), none: new Set(), explorer: new Set(), oas: new Set(), strategy: new Set() });
  }
  for (const row of indexes.inventory) {
    const tab = indexes.formToTab.get(row.fieldName) || "—";
    const bucket = byTab.get(tab) || byTab.get("—") || { ui: new Set(), nb: new Set(), legacy: new Set(), none: new Set(), explorer: new Set(), oas: new Set(), strategy: new Set() };
    bucket.ui.add(row.fieldName);
    const nb = indexes.nbByForm.get(row.fieldName);
    if (nb) bucket.nb.add(row.fieldName);
    else if (indexes.formLegacyMapped.has(row.fieldName)) bucket.legacy.add(row.fieldName);
    else bucket.none.add(row.fieldName);
    if (fieldNeededFor(row.fieldName, buildConsumerSets()).explorer === "Yes") bucket.explorer.add(row.fieldName);
  }
  return byTab;
}

function buildExplorerSectionSummary(outRows) {
  const sections = {};
  for (const [section] of Object.entries(EXPLORER_SECTION_FIELDS)) {
    sections[section] = { displayed: [], missing: [], partial: [] };
  }
  sections["H. Alignment Context"] = {
    displayed: ["alignmentBand", "alignmentScoreOptional", "alignmentSignals", "whatNeedsValidation", "keyConsideration"],
    missing: [],
    partial: [],
    note: "API-derived from OAS /companies — not Airtable columns",
  };

  for (const [section, keys] of Object.entries(EXPLORER_SECTION_FIELDS)) {
    for (const key of keys) {
      const hits = outRows.filter(
        (r) => r.name === key || r.staticForm.split(";").map((s) => s.trim()).includes(key)
      );
      const hit = hits[0];
      if (!hit) {
        sections[section].missing.push(key);
        continue;
      }
      if (hit.displayedExplorer === "Yes") sections[section].displayed.push(key);
      else if (hit.displayedExplorer === "Partial") sections[section].partial.push(key);
      else sections[section].missing.push(key);
    }
  }
  return sections;
}

function buildMarkdown(ctx) {
  const { outRows, counts, indexes, p1Gaps, profileKeys } = ctx;
  const total = outRows.length;
  const staticCount = indexes.staticForms.size;
  const buildCount = indexes.buildSheet.length;
  const fullyCovered = counts["Fully Covered"] || 0;
  const legacyOnly = counts["Legacy Only"] || 0;
  const staticOnly = counts["Static Form Only"] || 0;
  const schemaOnly = counts["Schema Only"] || 0;
  const explorerDisplayedYes = outRows.filter((r) => r.displayedExplorer === "Yes").length;
  const explorerDisplayedPartial = outRows.filter((r) => r.displayedExplorer === "Partial").length;
  const oasNeedNoNb = outRows.filter(
    (r) => r.need.oas === "Yes" && r.nbMapped === "No" && !r.sysDerived.startsWith("Yes")
  ).length;
  const explorerNeedNoDisplay = outRows.filter(
    (r) => r.need.explorer === "Yes" && r.displayedExplorer === "No"
  ).length;

  const sectionSummary = buildExplorerSectionSummary(outRows);

  return `# Operator Setup → Operator Explorer field mapping audit (pre-Phase E)

**Audit-only** — generated by \`scripts/generate-operator-setup-to-explorer-field-mapping-audit.mjs\`.  
No code, Airtable schema, scoring, BAS/OCS/OAS PDF, or My Deals UX changes.

**CSV:** [reports/operator-setup-to-explorer-field-mapping-audit.csv](../reports/operator-setup-to-explorer-field-mapping-audit.csv)  
**Prior diff:** [operator-setup-field-coverage-diff.md](./operator-setup-field-coverage-diff.md) | [reports/operator-setup-field-coverage-diff.csv](../reports/operator-setup-field-coverage-diff.csv)  
**Explorer Phase D:** [operator-explorer-new-base-integration.md](./operator-explorer-new-base-integration.md)

---

## Executive summary

| Metric | Value |
|--------|------:|
| Airtable / child audit rows | ${total} |
| Static setup form fields (inventory) | ${staticCount} |
| New-base build-sheet rows | ${buildCount} |
| **Fully Covered** (UI + new-base writer) | ${fullyCovered} |
| **Legacy Only** | ${legacyOnly} |
| **Static Form Only** | ${staticOnly} |
| **Schema Only** | ${schemaOnly} |
| Explorer display **Yes** (live rail / prefill) | ${explorerDisplayedYes} |
| Explorer display **Partial** (legacy tabs / JSON) | ${explorerDisplayedPartial} |
| OAS-needed, not new-base mapped | ${oasNeedNoNb} |
| Explorer consumer, not displayed | ${explorerNeedNoDisplay} |
| **P1 gaps before Phase E** | ${p1Gaps.length} |

**Full consistency across Setup → new-base → Explorer → OAS/Strategy?** **No — Partial.**  
Live \`rec…\` Explorer profiles (Phase D) surface P1 fields when prefill is populated, but **${legacyOnly}** legacy-only and **${staticOnly}** static-form-only rows mean many setup inputs still do not persist to new-base. Phase E (Deal Operator Review Set) should wait until P1 save + display paths are verified in staging.

---

## Audit 1 — Airtable field coverage

See CSV for every row with columns:

- Operator Setup UI tab, form name, OAS inject, new-base / legacy writer, child/JSON
- Explorer section/tab, displayed flag, OAS/Strategy use, system/derived/admin
- Coverage status, risk, recommended action

### Coverage status counts

| Coverage Status | Count |
|-----------------|------:|
${Object.entries(counts)
  .sort((a, b) => b[1] - a[1])
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

---

## Audit 2 — Operator Setup page/tab coverage (13 tabs)

Source: \`scripts/he-cala-form-inventory.json\` (${staticCount} fields) + OAS inject fields on Platform tab.

| Tab | Fields in UI (count) | Saved new-base (approx) | Legacy-only (approx) | Not saved | Used Explorer | Used OAS/Strategy |
|-----|----------------------:|------------------------:|---------------------:|----------:|:-------------:|:-----------------:|
${SETUP_TABS_ORDER.map((tab) => {
  const fields = indexes.inventory.filter((r) => (TAB_ALIASES[r.tab] || r.tab) === tab);
  const nb = fields.filter((f) => indexes.nbByForm.has(f.fieldName)).length;
  const leg = fields.filter((f) => indexes.formLegacyMapped.has(f.fieldName) && !indexes.nbByForm.has(f.fieldName)).length;
  const none = fields.length - nb - leg;
  const ex = fields.filter((f) => fieldNeededFor(f.fieldName, buildConsumerSets()).explorer === "Yes").length;
  const oas = fields.filter((f) => fieldNeededFor(f.fieldName, buildConsumerSets()).oas === "Yes").length;
  return `| ${tab} | ${fields.length} | ${nb} | ${leg} | ${none} | ${ex} | ${oas} |`;
}).join("\n")}

**Recommended actions (tabs):**

- **0–1 Company / Positioning:** Keep P1 identity + admin metadata (data confidence, source, last updated). Close legacy-only narrative fields in Phase B2 if Explorer tabs must read new-base only.
- **2 Operating Platform:** \`cap_*\` / service multis — largely legacy narrative + writer-derived granular columns; align \`offeredServices\` + capability singles with Phase D rail.
- **4 Markets & Footprint:** OAS inject + \`geo_*\` grid — prioritize Active Countries/Markets writer rows; \`regions\` vs \`geo_*\` remains a known split.
- **9–10 Best Fit / Deal Terms:** \`bf_*\` for OAS; \`dealTermsOptIn\` needs product decision.
- **11–12 Proof / Diligence:** JSON/child (\`caseStudiesDetail\`, \`ownerDiligenceQa\`) — verify new-base child replace on save.

---

## Audit 3 — Operator Explorer section coverage (Phase D + legacy tabs)

### Phase D snapshot rail (live \`rec…\` profiles)

| Section | Fields in rail module | Displayed Yes | Partial | Missing in Explorer UI |
|---------|----------------------|--------------:|--------:|-------------------------|
${Object.entries(EXPLORER_SECTION_FIELDS)
  .map(([section, keys]) => {
    const s = sectionSummary[section];
    return `| ${section} | ${keys.length} | ${s.displayed.length} | ${s.partial.length} | ${s.missing.join(", ") || "—"} |`;
  })
  .join("\n")}

| **H. Alignment Context** | OAS API (dealId) | — | — | N/A (not Airtable) |

**Legacy Explorer tabs** (still render when prefill / \`explorerProfileJson\` populated): Profile & Positioning (\`overview_*\`), Operating Platform (\`cap_*\`), Brand (\`brand_*\`), Markets (\`geo_*\`, \`mkt_*\`), Owner Engagement (\`ov_*\`), Infrastructure, Risk, Leadership (child), Best Fit (\`bf_*\`), Proof (case studies). These are **Partial** display — not the Phase D rail.

**Mock/demo:** No-id gold-mock page = **Sample operator profile** banner; \`MOCK_OPERATORS\` API requires \`OPERATOR_EXPLORER_ALLOW_MOCKS=1\`.

---

## Audit 4 — Gap categories

1. **Airtable fields not in Setup UI** — ${schemaOnly} schema-only rows + admin columns without forms.
2. **Airtable fields not in Explorer** — High-value fields with \`Displayed=No\` but Explorer consumer Yes (see CSV filter).
3. **Setup UI not saving new-base** — **${staticOnly}** static-form-only + orphan forms.
4. **Setup UI legacy-only** — **${legacyOnly}** legacy-only rows.
5. **Explorer shows data not reliably in new-base** — Legacy tab narratives + list/detail when \`OPERATOR_SETUP_USE_NEW_BASE_WRITER=0\`.
6. **OAS fields not in Explorer** — Scoring-only fields (e.g. \`bf_*\` inputs) may appear only in OAS book, not Phase D rail — acceptable if intentional.
7. **Strategy fields not in Explorer** — Strategy uses OAS companies API; same prefill spine — \`dataConfidenceLevel\`, \`companyName\`, alignment band.
8. **System/admin** — \`operator_id\`, \`submission_status\`, timestamps, Operator links.
9. **Writer-derived** — Granular governance service columns from aggregate multis.
10. **Needs decision** — \`dealTermsOptIn\`, \`diligenceQaOptIn\`.

---

## Audit 5 — High-priority fields

### Priority 1 — Before Phase E / Review Set

| Field | Setup UI | New-base writer | Explorer | OAS | Strategy | Gap |
|-------|:--------:|:---------------:|:--------:|:---:|:--------:|-----|
${P1_FIELDS.map((p1) => {
  const hit =
    outRows.find((r) => r.table === p1.table && r.name === p1.field) ||
    outRows.find((r) => r.staticForm === p1.form);
  if (!hit)
    return `| ${p1.form || p1.field} | ? | ? | ? | ? | ? | Not in audit |`;
  return `| ${hit.name} | ${hit.setupTabs ? "Yes" : "Partial"} | ${hit.nbMapped} | ${hit.displayedExplorer} | ${hit.need.oas} | ${hit.need.strategy} | ${hit.status} |`;
}).join("\n")}

**P1 blockers (${p1Gaps.length}):**

${p1Gaps.length ? p1Gaps.map((g) => `- **${g.form || g.field}**: ${g.issues || g.issue}`).join("\n") : "- None flagged by automated P1 check — still verify staging saves for Active operators."}

### Priority 2 — Stronger Explorer

companyDescription, website, headquarters, companyLogo, brandFamiliesOperated, softBrandLifestyleExperience, similarProjectCaseStudies, brandsPortfolioDetail, governanceCadence, minimumKeyCount — see CSV (\`Needed For Operator Explorer?=Yes\`).

### Priority 3 — Later

dealTermsOptIn, diligenceQaOptIn, granular service columns (writer-derived), legacy \`overview_*\` / \`cap_*\` narratives, low-value static-only fields.

---

## Audit 6 — Consistency claims

| # | Question | Answer |
|---|----------|--------|
| 1 | All Airtable fields have Setup UI? | **No** — system, schema-only, writer-derived, child JSON. |
| 2 | All Airtable fields have new-base writer? | **No** — ${legacyOnly} legacy-only, ${staticOnly} static-only, child JSON paths. |
| 3 | All high-value fields display in Explorer? | **Partial** — Phase D rail for P1 keys; legacy tabs for narratives; gaps when prefill empty. |
| 4 | All OAS-needed fields in Explorer? | **Partial** — Core footprint in rail; \`bf_*\` / scoring inputs primarily in OAS book. |
| 5 | All Strategy-needed fields in Explorer? | **Partial** — Strategy uses OAS companies; profile shows alignment context with dealId. |
| 6 | Fields intentionally hidden (system/admin/derived)? | **Yes** — see \`System / Derived / Admin-only?\` column. |
| 7 | Must fix before Phase E? | P1 save path to new-base + staging verification; \`companyLogo\` attachment pipeline; close static-only market/footprint fields used by OAS. |
| 8 | Can wait until after Phase E? | P3 narratives, deal/diligence opt-ins, non-blocking Explorer depth tabs. |

---

## Audit 7 — Recommended implementation plan

### Phase D2 — Explorer display
- Ensure P2 fields render in Phase D rail when prefill present.
- Surface \`conversionReflagExperience\` explicitly in section E.
- List cards: optional chips from \`activeCountries\` / chain scale.

### Phase B2 — New-base writer
- Extend build sheet for remaining **Legacy Only** rows consumed by OAS/Explorer (${legacyOnly} rows).
- Map OAS inject fields missing \`nbMapped=Yes\`.
- Resolve \`regions\` vs \`geo_*\` / \`specificMarkets\`.

### Phase Setup UI
- Add controls only where operators/admins must edit (not writer-derived granulars).
- Resolve \`dealTermsOptIn\` / \`diligenceQaOptIn\`.

### Phase E readiness (Deal Operator Review Set)
Proceed when P1 fields are **saved to new-base** (staging with \`OPERATOR_SETUP_USE_NEW_BASE_WRITER=1\`), **visible on live Explorer** profiles, and **returned by OAS companies API** for Strategy.

---

## Recommended next Cursor prompt (P1 gaps)

\`\`\`
Phase B2 + Setup: Close P1 Operator Setup field gaps from reports/operator-setup-to-explorer-field-mapping-audit.csv — add new-base writer rows for any P1 field with Legacy Only / Static Form Only / OAS Injected Only status; verify third-party-operator-setup save in staging; confirm Operator Explorer Phase D rail and OAS companies API return populated values. Do not enable production OPERATOR_SETUP_USE_NEW_BASE_WRITER=1. No schema/scoring/PDF changes.
\`\`\`

---

## Regenerate

\`\`\`bash
node scripts/generate-operator-setup-to-explorer-field-mapping-audit.mjs
node scripts/validate-operator-setup-to-explorer-field-mapping.mjs
\`\`\`

*Generated ${new Date().toISOString().slice(0, 10)}.*
`;
}

main();
