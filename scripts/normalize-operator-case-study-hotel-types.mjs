#!/usr/bin/env node
/**
 * Normalize Operator Setup case study `hotel_type` values for Operator Explorer Asset Type filter.
 *
 *   node scripts/normalize-operator-case-study-hotel-types.mjs --dry-run
 *   node scripts/normalize-operator-case-study-hotel-types.mjs --apply
 *
 * Tables: Operator Setup - Case Studies (new-base); optional legacy 3rd Party Operator - Case Studies.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  normalizeOperatorCaseStudyHotelType,
  OPERATOR_EXPLORER_ASSET_TYPE_VALUES,
} from "../lib/operator-explorer/operator-case-study-hotel-type-normalize.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_PATH = path.join(ROOT, "reports", "operator-case-study-hotel-type-normalize.json");

const NEW_BASE_TABLE = "Operator Setup - Case Studies";
const LEGACY_TABLE = "3rd Party Operator - Case Studies";

const args = new Set(process.argv.slice(2));
const DRY_RUN = !args.has("--apply");
const INCLUDE_LEGACY = args.has("--include-legacy");

function enc(v) {
  return encodeURIComponent(String(v));
}

async function fetchAllRecords(baseId, apiKey, tableName, fieldNames) {
  const table = enc(tableName);
  const fields = fieldNames.map((f) => `fields%5B%5D=${enc(f)}`).join("&");
  let offset = null;
  const rows = [];
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100&${fields}`;
    if (offset) url += `&offset=${enc(offset)}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const data = await res.json();
    if (data.error) throw new Error(`${tableName}: ${data.error.message || JSON.stringify(data.error)}`);
    for (const rec of data.records || []) {
      rows.push({ id: rec.id, fields: rec.fields || {}, tableName });
    }
    offset = data.offset || null;
  } while (offset);
  return rows;
}

async function patchRecord(baseId, apiKey, tableName, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${enc(tableName)}/${enc(recordId)}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`${tableName} ${recordId}: ${data.error?.message || res.status}`);
  return data;
}

function hotelTypeFieldForTable(tableName) {
  return tableName === LEGACY_TABLE ? "Hotel Type" : "hotel_type";
}

function planRow(row) {
  const field = hotelTypeFieldForTable(row.tableName);
  const before = String(row.fields[field] || "").trim();
  const { normalized, changed, reason } = normalizeOperatorCaseStudyHotelType(before);
  return {
    recordId: row.id,
    tableName: row.tableName,
    field,
    property_name: row.fields.property_name || row.fields["Property Name"] || "",
    before,
    after: normalized,
    changed: changed && before !== normalized,
    reason,
  };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!baseId || !apiKey) {
    console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
    process.exit(1);
  }

  const tables = [NEW_BASE_TABLE];
  if (INCLUDE_LEGACY) tables.push(LEGACY_TABLE);

  const allRows = [];
  for (const tableName of tables) {
    const field = hotelTypeFieldForTable(tableName);
    const fields =
      tableName === LEGACY_TABLE
        ? [field, "Property Name"]
        : [field, "property_name"];
    const rows = await fetchAllRecords(baseId, apiKey, tableName, fields);
    allRows.push(...rows);
  }

  const plans = allRows.map(planRow);
  const toApply = plans.filter((p) => p.changed);
  const unmapped = plans.filter((p) => p.before && p.reason === "unmapped" && !p.changed);

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: DRY_RUN,
    canonicalValues: OPERATOR_EXPLORER_ASSET_TYPE_VALUES,
    totals: {
      scanned: plans.length,
      withValue: plans.filter((p) => p.before).length,
      toUpdate: toApply.length,
      unmapped: unmapped.length,
    },
    updates: toApply,
    unmapped,
  };

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

  console.log(`Scanned ${report.totals.scanned} case study rows (${report.totals.withValue} with hotel_type).`);
  console.log(`Planned updates: ${report.totals.toUpdate}`);
  if (report.totals.unmapped) console.log(`Unmapped (left unchanged): ${report.totals.unmapped}`);
  console.log(`Report: ${REPORT_PATH}`);

  for (const p of toApply) {
    console.log(`  ${p.tableName} ${p.recordId}: ${JSON.stringify(p.before)} → ${JSON.stringify(p.after)} (${p.reason})`);
  }

  if (DRY_RUN) {
    console.log("\nDry run only. Re-run with --apply to write to Airtable.");
    return;
  }

  for (const p of toApply) {
    await patchRecord(baseId, apiKey, p.tableName, p.recordId, { [p.field]: p.after });
  }
  console.log(`\nApplied ${toApply.length} update(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
