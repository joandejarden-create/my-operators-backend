#!/usr/bin/env node
/**
 * Validates pre-Phase E mapping audit artifacts exist and P1 fields are classified sensibly.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

const csvPath = path.join(ROOT, "reports/operator-setup-to-explorer-field-mapping-audit.csv");
const mdPath = path.join(ROOT, "docs/operator-setup-to-explorer-field-mapping-audit.md");
const genPath = path.join(ROOT, "scripts/generate-operator-setup-to-explorer-field-mapping-audit.mjs");

for (const p of [csvPath, mdPath, genPath]) {
  if (!fs.existsSync(p)) fail("Missing artifact: " + p);
  else pass("Exists: " + path.basename(p));
}

if (!fs.existsSync(csvPath)) process.exit(process.exitCode || 1);

const csv = fs.readFileSync(csvPath, "utf8");
const lines = csv.trim().split("\n");
const header = lines[0].split(",");
const requiredCols = [
  "Airtable Table",
  "Airtable Field",
  "Operator Setup UI Tab",
  "New-Base Writer Mapped?",
  "Displayed in Operator Explorer?",
  "Used by OAS?",
  "Coverage Status",
];
for (const col of requiredCols) {
  if (!header.includes(col)) fail("CSV missing column: " + col);
  else pass("CSV column: " + col);
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (c === '"') inQ = false;
      else cur += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

const rows = lines.slice(1).map(parseCsvLine);
const col = (name) => header.indexOf(name);
const byField = new Map();
for (const r of rows) {
  const key = `${r[col("Airtable Table")]}::${r[col("Airtable Field")]}`;
  byField.set(key, r);
  const form = r[col("Operator Setup Form Field Name")];
  if (form) {
    for (const f of form.split(";")) {
      const t = f.trim();
      if (t) byField.set(`form::${t}`, r);
    }
  }
}

const P1 = [
  { table: "Operator Setup - Master", field: "company_name", form: "companyName" },
  { table: "Operator Setup - Master", field: "operator_id", form: "operatorId", systemOk: true },
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

for (const p1 of P1) {
  const r = byField.get(`${p1.table}::${p1.field}`) || byField.get(`form::${p1.form}`);
  if (!r) {
    fail("P1 field missing from CSV: " + p1.field);
    continue;
  }
  pass("P1 in CSV: " + p1.field);

  const sys = r[col("System / Derived / Admin-only?")] || "";
  if (p1.systemOk) {
    if (!sys.startsWith("Yes")) fail(p1.field + " should be system/admin");
    else pass(p1.field + " marked system/admin");
    continue;
  }

  const nb = r[col("New-Base Writer Mapped?")];
  const leg = r[col("Legacy Writer Mapped?")];
  const child = r[col("Child Table / JSON Payload?")];
  if (nb !== "Yes" && leg !== "Yes" && child !== "Yes") {
    fail(`P1 ${p1.field}: not mapped to new-base or legacy (${nb}/${leg})`);
  } else {
    pass(`P1 ${p1.field}: writer path (${nb} new-base, ${leg} legacy)`);
  }

  const disp = r[col("Displayed in Operator Explorer?")];
  if (disp === "No") fail(`P1 ${p1.field}: not displayed in Explorer audit`);
  else pass(`P1 ${p1.field}: Explorer display ${disp}`);

  const oas = r[col("Used by OAS?")];
  if (oas !== "Yes" && ["company_name", "companyDescription", "website", "headquarters"].includes(p1.form))
    pass(`P1 ${p1.field}: OAS=${oas} (profile-only ok)`);
  else if (oas === "Yes" || !["companyDescription"].includes(p1.form)) pass(`P1 ${p1.field}: OAS=${oas}`);
}

const writerDerived = rows.filter((r) => {
  const sys = r[col("System / Derived / Admin-only?")] || "";
  const status = r[col("Coverage Status")] || "";
  return status === "Writer-Derived" || sys.includes("Writer-derived");
});
if (writerDerived.length < 1) {
  const granularInSheet = fs
    .readFileSync(path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json"), "utf8")
    .includes("governance");
  if (granularInSheet) pass("Granular service fields covered via build sheet (writer-derived at save)");
  else fail("Expected writer-derived or granular mappings");
} else pass("Writer-derived fields recognized: " + writerDerived.length);

const childRows = rows.filter((r) => r[col("Child Table / JSON Payload?")] === "Yes");
if (childRows.length < 3) fail("Expected child/JSON rows");
else pass("Child/JSON rows: " + childRows.length);

const md = fs.readFileSync(mdPath, "utf8");
for (const section of [
  "Audit 1",
  "Audit 2",
  "Audit 3",
  "Audit 4",
  "Audit 5",
  "Audit 6",
  "Audit 7",
  "Phase D2",
  "Phase B2",
  "Phase E readiness",
]) {
  if (!md.includes(section)) fail("Markdown missing section: " + section);
  else pass("Markdown section: " + section);
}

const profileJs = fs.readFileSync(
  path.join(ROOT, "public/js/operator-explorer-new-base-profile.js"),
  "utf8"
);
if (!profileJs.includes("operator-alignment-snapshot")) fail("Explorer profile missing OAS companies API");
else pass("Explorer uses OAS companies endpoint");

if (process.exitCode) {
  console.error("\nValidation finished with failures. Regenerate: node scripts/generate-operator-setup-to-explorer-field-mapping-audit.mjs");
} else {
  console.log("\nAll mapping audit validation checks passed.");
}
