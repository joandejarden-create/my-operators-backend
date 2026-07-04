/**
 * Emit CSV + JSON of every Brand Setup ↔ Airtable mapping claimed in api/brand-library.js
 * (no Airtable API calls). Use for schema diffs and UI demand matrices.
 *
 * Usage: node scripts/export-brand-setup-airtable-inventory.mjs
 * Optional: OUT_DIR=./docs/generated node scripts/export-brand-setup-airtable-inventory.mjs
 */
import { mkdirSync, writeFileSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildBrandSetupAirtableMappingInventory } from "../api/brand-library.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const outDir = process.env.OUT_DIR
  ? path.resolve(process.cwd(), process.env.OUT_DIR)
  : path.join(root, "docs", "generated");

function csvEscape(val) {
  const s = val == null ? "" : String(val);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows) {
  const cols = [
    "airtableTable",
    "brandSetupTab",
    "mappingSource",
    "mappingKind",
    "apiFormKey",
    "airtableColumn",
    "notes"
  ];
  const lines = [cols.join(",")];
  for (const r of rows) {
    lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  }
  return lines.join("\r\n");
}

const inv = buildBrandSetupAirtableMappingInventory();
mkdirSync(outDir, { recursive: true });
const jsonPath = path.join(outDir, "brand-setup-airtable-inventory.json");
const csvPath = path.join(outDir, "brand-setup-airtable-inventory.csv");
writeFileSync(jsonPath, JSON.stringify(inv, null, 2), "utf8");
writeFileSync(csvPath, toCsv(inv.rows), "utf8");
console.log(`Wrote ${inv.rowCount} rows`);
console.log(jsonPath);
console.log(csvPath);
