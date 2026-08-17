/**
 * Generate Operator Explorer / My Operator fill template CSV.
 *
 *   node scripts/generate-operator-explorer-fill-template.mjs
 *   node scripts/generate-operator-explorer-fill-template.mjs "Hotel Equities (CALA)" reports/my-template.csv
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const DEFAULT_OPERATOR = "Hotel Equities (CALA)";
const DEFAULT_MASTER_ID = "recWPKu5laVZxsvpn";
const INVENTORY_PATH = path.join(ROOT, "scripts", "he-cala-form-inventory.json");
const BUILD_SHEET_PATH = path.join(ROOT, "api", "lib", "operator-setup-new-base-build-sheet-rows.json");
const BINDINGS_PATH = path.join(ROOT, "api", "lib", "third-party-operator-new-two-field-bindings.json");
const EXPLORER_AUDIT_CSV = path.join(ROOT, "reports", "operator-setup-to-explorer-field-mapping-audit.csv");
const OUT_DEFAULT = path.join(ROOT, "reports", "operator-explorer-fill-template-hotel-equities-cala.csv");

const MULTI_SELECT = new Set([
  "chainScalesSupported",
  "propertyTypes",
  "additionalExperience",
  "activeCountries",
  "activeMarkets",
  "regions",
  "serviceModelsSupported",
  "managementStructuresSupported",
  "brandFamiliesOperated",
  "offeredServices",
  "brands",
  "salesPlatform",
  "bf_selected_asset_types",
  "bf_selected_situation_types",
  "bf_selected_deal_structures",
  "revenueManagementServices",
  "salesMarketingSupport",
  "accountingReporting",
  "procurementServices",
  "hrTrainingServices",
  "technologyServices",
  "designRenovationSupport",
  "developmentServices",
]);

const SINGLE_SELECT_HINTS = [
  "primaryServiceModel",
  "marketPresenceType",
  "companySize",
  "emergencyResponse",
  "businessContinuity",
  "support24x7",
  "sustainabilityPrograms",
  "esgReporting",
  "carbonTracking",
  "ownerReportingLevel",
  "governanceCadence",
  "preferredContactMethod",
  "fbCapabilityLevel",
  "revenueManagementCapability",
  "salesPlatform",
  "newBuildOpeningExperience",
  "conversionReflagExperience",
  "preOpeningSupportCapability",
  "dataConfidenceLevel",
  "sourceType",
  "infra_technology_maturity_level",
  "softBrandLifestyleExperience",
];

function inferValueType(fieldName, binding) {
  if (binding?.fieldType === "multipleSelects") return "multi_select";
  if (binding?.fieldType === "singleSelect") return "single_select";
  if (binding?.fieldType === "checkbox") return "checkbox";
  if (binding?.fieldType === "number") return "number";
  if (binding?.fieldType === "url" || fieldName === "website") return "url";
  if (binding?.fieldType === "multipleAttachments" || fieldName === "companyLogo") return "attachment";
  if (fieldName.endsWith("_json") || fieldName.includes("PortfolioDetail")) return "json_or_long_text";
  if (MULTI_SELECT.has(fieldName)) return "multi_select";
  if (
    SINGLE_SELECT_HINTS.includes(fieldName) ||
    fieldName.startsWith("cap_signal_") ||
    fieldName.startsWith("cap_kpi_") ||
    fieldName.startsWith("brand_signal_") ||
    fieldName.startsWith("infra_signal_") ||
    fieldName.startsWith("tr_signal_") ||
    fieldName.startsWith("mkt_signal_") ||
    fieldName.startsWith("lead_signal_")
  ) {
    return "single_select";
  }
  if (/^exec_\d+_headshot$/.test(fieldName)) return "attachment_url";
  if (/^exec_\d+_/.test(fieldName)) return "text";
  if (/^cs_/.test(fieldName) || fieldName.includes("caseStud")) return "case_study_child";
  if (/^diligence_/.test(fieldName) || fieldName === "ownerDiligenceQa") return "diligence_child";
  if (/^geo_/.test(fieldName)) return "number";
  if (["yearEstablished", "yearsInBusiness", "numberOfBrands", "numberOfMarkets", "totalProperties", "totalRooms", "minimumKeyCount"].includes(fieldName))
    return "number";
  return "text";
}

function csvEscape(cell) {
  const s = cell == null ? "" : String(cell);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function parseCsvLine(line) {
  const cells = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else inQ = !inQ;
      continue;
    }
    if (ch === "," && !inQ) {
      cells.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  cells.push(cur);
  return cells;
}

function loadExplorerAuditByFormKey() {
  const map = new Map();
  if (!fs.existsSync(EXPLORER_AUDIT_CSV)) return map;
  const lines = fs.readFileSync(EXPLORER_AUDIT_CSV, "utf8").split(/\r?\n/).filter(Boolean);
  const header = parseCsvLine(lines[0]);
  const idx = {
    table: header.indexOf("Airtable Table"),
    field: header.indexOf("Airtable Field"),
    form: header.indexOf("Operator Setup Form Field Name"),
    explorerSection: header.indexOf("Operator Explorer Section / Tab"),
    displayed: header.indexOf("Displayed in Operator Explorer?"),
    coverage: header.indexOf("Coverage Status"),
    action: header.indexOf("Recommended Action"),
  };
  for (let i = 1; i < lines.length; i++) {
    const c = parseCsvLine(lines[i]);
    const formKey = (c[idx.form] || "").trim();
    const airtableField = (c[idx.field] || "").trim();
    const key = formKey || airtableField;
    if (!key) continue;
    map.set(key, {
      explorerSection: c[idx.explorerSection] || "",
      displayedInExplorer: c[idx.displayed] || "",
      coverageStatus: c[idx.coverage] || "",
      recommendedAction: c[idx.action] || "",
      airtableTable: c[idx.table] || "",
      airtableField,
    });
  }
  return map;
}

function bindingByFormKey(bindingsDoc) {
  const map = new Map();
  const list = Array.isArray(bindingsDoc)
    ? bindingsDoc
    : bindingsDoc.bindings || bindingsDoc.rows || [];
  for (const row of list) {
    const keys = row.formKeys || row.formKey || [];
    const list = Array.isArray(keys) ? keys : [keys];
    for (const k of list) {
      if (!k) continue;
      map.set(k, row);
    }
  }
  return map;
}

function buildSheetByFormKey(rows) {
  const map = new Map();
  for (const r of rows) {
    if (r.form_name) map.set(r.form_name, r);
  }
  return map;
}

function purposeForField(fieldName, tab, explorer) {
  if (/^exec_\d+_/.test(fieldName)) {
    return "Leadership team member profile shown in Explorer Leadership sections.";
  }
  if (fieldName.startsWith("overview_")) {
    return "Owner-facing narrative on Explorer Overview (What they are best at / Why owners consider).";
  }
  if (fieldName.startsWith("cap_")) {
    return "Capability Snapshot KPI or narrative card on Explorer Operating Platform.";
  }
  if (fieldName.startsWith("brand_")) {
    return "Brand relationships narrative or signal on Explorer Brand section.";
  }
  if (fieldName.startsWith("infra_")) {
    return "Technology & infrastructure story or KPI on Explorer Infrastructure section.";
  }
  if (fieldName.startsWith("mkt_") || fieldName.startsWith("geo_")) {
    return "Markets & footprint metrics or narrative on Explorer Markets section.";
  }
  if (fieldName.startsWith("bf_") || fieldName.startsWith("bestFit")) {
    return "Best-fit / deal preference filters used in alignment context and Explorer fit panels.";
  }
  if (fieldName.startsWith("op_")) {
    return "Operating platform JSON subsection (commercial, reporting, transition tiles).";
  }
  if (fieldName === "brands" || fieldName === "additionalBrands") {
    return "Which brands the operator manages; drives Explorer brand portfolio and logos.";
  }
  if (fieldName === "companyLogo") {
    return "Operator logo in Explorer list and profile hero.";
  }
  if (explorer?.displayedInExplorer === "Yes") {
    return `Populates Operator Explorer: ${explorer.explorerSection || "see audit column"}.`;
  }
  if (explorer?.displayedInExplorer?.startsWith("Partial")) {
    return `Partial Explorer visibility (${explorer.explorerSection || "legacy/JSON path"}).`;
  }
  return `My Operator intake (${tab}); ${explorer?.recommendedAction || "save via canonical new-base writer when mapped"}.`;
}

const operatorName = process.argv[2] || DEFAULT_OPERATOR;
const outPath = process.argv[3] ? path.resolve(process.cwd(), process.argv[3]) : OUT_DEFAULT;

const inventory = JSON.parse(fs.readFileSync(INVENTORY_PATH, "utf8"));
const buildRows = JSON.parse(fs.readFileSync(BUILD_SHEET_PATH, "utf8")).rows || [];
const bindingsRaw = JSON.parse(fs.readFileSync(BINDINGS_PATH, "utf8"));
const bindings = bindingByFormKey(bindingsRaw);
const buildMap = buildSheetByFormKey(buildRows);
const explorerMap = loadExplorerAuditByFormKey();

const header = [
  "operator_master_record_id",
  "operator_name",
  "my_operator_tab",
  "form_field_key",
  "airtable_table",
  "airtable_field_name",
  "value_type",
  "allowed_options",
  "displayed_in_operator_explorer",
  "operator_explorer_section",
  "field_purpose",
  "example_value_for_template",
  "current_value_in_system",
  "suggested_value_verdict",
  "where_to_enter",
  "canonical_writer_mapped",
  "coverage_status",
  "notes",
];

const lines = [header.map(csvEscape).join(",")];

for (const row of inventory) {
  const key = row.fieldName;
  const build = buildMap.get(key);
  const bind = bindings.get(key);
  const explorer = explorerMap.get(key) || explorerMap.get(build?.airtable_field_name || "");
  const valueType = inferValueType(key, bind);
  const options = bind?.selectOptions
    ? Array.isArray(bind.selectOptions)
      ? bind.selectOptions.join(" | ")
      : String(bind.selectOptions)
    : "";
  const example =
    row.suggestedCopyPaste && String(row.suggestedCopyPaste).trim()
      ? String(row.suggestedCopyPaste).trim()
      : row.current && String(row.current).trim() && row.current !== "(empty)"
      ? String(row.current).trim()
      : "";
  const tabLabel = row.tab || "My Operator";
  const where =
    key.startsWith("exec_")
      ? "Leadership & Team (repeat per leader index)"
      : key.includes("caseStud") || key.startsWith("cs_")
      ? "Proof & Track Record / Case Studies"
      : tabLabel;

  lines.push(
    [
      DEFAULT_MASTER_ID,
      operatorName,
      tabLabel,
      key,
      build?.table_name || explorer?.airtableTable || "",
      build?.airtable_field_name || bind?.airtableName || explorer?.airtableField || key,
      valueType,
      options,
      explorer?.displayedInExplorer || "",
      explorer?.explorerSection || "",
      purposeForField(key, tabLabel, explorer),
      example,
      row.rawValue != null ? String(row.rawValue).replace(/\s+/g, " ").slice(0, 500) : "",
      row.verdict || "",
      where,
      build ? "yes" : "no",
      explorer?.coverageStatus || "",
      row.isEmpty ? "currently empty in form inventory" : "",
    ]
      .map(csvEscape)
      .join(",")
  );
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, lines.join("\n") + "\n", "utf8");

console.log(
  JSON.stringify(
    {
      ok: true,
      operatorName,
      masterRecordId: DEFAULT_MASTER_ID,
      rows: inventory.length,
      outPath,
      alsoSee: [
        "scripts/he-cala-part-a.csv",
        "scripts/he-cala-form-inventory.json",
        "reports/operator-setup-to-explorer-field-mapping-audit.csv",
      ],
    },
    null,
    2
  )
);
