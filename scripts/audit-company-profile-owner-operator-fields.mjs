/**
 * Audit empty Owner-Operator extension fields on Company Profile.
 *
 *   node scripts/audit-company-profile-owner-operator-fields.mjs
 *   node scripts/audit-company-profile-owner-operator-fields.mjs --json reports/company-profile-oo-fields-audit.json
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { join } from "path";
import {
  buildCompanyProfileOwnerOperatorBackfillPatch,
  EXTENSION_FIELDS,
} from "../lib/company-profile-owner-operator-backfill.js";
import { MAP_CP_AIRTABLE } from "../lib/company-profile-owner-operator-fields.js";

const TABLE = process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const writeJson = process.argv.includes("--json");
const jsonPathArg = process.argv.find((a) => a.startsWith("--json="));
const jsonPath = jsonPathArg
  ? jsonPathArg.slice("--json=".length)
  : writeJson
    ? join("reports", "company-profile-oo-fields-audit.json")
    : null;

function fieldEmpty(raw) {
  if (raw == null) return true;
  if (Array.isArray(raw)) return raw.length === 0;
  return String(raw).trim() === "";
}

async function fetchAll() {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}?${qs}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
  process.exit(1);
}

const records = await fetchAll();
const summary = { total: records.length, emptyCounts: {}, wouldPatch: 0, rows: [] };

for (const field of EXTENSION_FIELDS) {
  summary.emptyCounts[field] = 0;
}

for (const rec of records) {
  const f = rec.fields || {};
  const name = String(f["Company Name"] || "(no name)").trim();
  const emptyFields = EXTENSION_FIELDS.filter((field) => fieldEmpty(f[field]));
  for (const field of emptyFields) summary.emptyCounts[field] += 1;

  const { patch, reasons } = buildCompanyProfileOwnerOperatorBackfillPatch(f);
  const patchKeys = Object.keys(patch);
  if (patchKeys.length) summary.wouldPatch += 1;

  summary.rows.push({
    id: rec.id,
    companyName: name,
    companyType: String(f[MAP_CP_AIRTABLE.companyType] || "").trim(),
    ecosystemRole: String(f["Company's role in the hotel ecosystem"] || "").trim(),
    emptyFields,
    proposedPatch: patch,
    reasons,
  });
}

console.log("Company Profile Owner-Operator field audit");
console.log("Total companies:", summary.total);
console.log("Would receive at least one backfill field:", summary.wouldPatch);
console.log("\nEmpty field counts:");
for (const [field, count] of Object.entries(summary.emptyCounts)) {
  console.log(`  ${field}: ${count}`);
}

console.log("\n--- Per company (would patch) ---");
for (const row of summary.rows
  .filter((r) => Object.keys(r.proposedPatch).length)
  .sort((a, b) => a.companyName.localeCompare(b.companyName))) {
  console.log(`\n${row.companyName} (${row.id})`);
  console.log("  type:", row.companyType || "(empty)");
  console.log("  ecosystem:", row.ecosystemRole || "(empty)");
  console.log("  empty:", row.emptyFields.join(", ") || "(none)");
  console.log("  patch:", JSON.stringify(row.proposedPatch));
  console.log("  reasons:", row.reasons.join("; "));
}

if (jsonPath) {
  writeFileSync(jsonPath, JSON.stringify(summary, null, 2), "utf8");
  console.log("\nWrote", jsonPath);
}
