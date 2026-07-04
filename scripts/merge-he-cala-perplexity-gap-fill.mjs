/**
 * Merge scripts/he-cala-perplexity-gap-fill.json into he-cala-form-inventory.json + .csv
 *
 *   node scripts/merge-he-cala-perplexity-gap-fill.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const GAP = path.join(ROOT, "scripts", "he-cala-perplexity-gap-fill.json");
const INV_JSON = path.join(ROOT, "scripts", "he-cala-form-inventory.json");
const INV_CSV = path.join(ROOT, "scripts", "he-cala-form-inventory.csv");

function csvEscape(cell) {
  const s = cell == null ? "" : String(cell);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function rowToCsv(cols) {
  return cols.map(csvEscape).join(",");
}

const gap = JSON.parse(fs.readFileSync(GAP, "utf8"));
const fields = gap.fields || {};
const inventory = JSON.parse(fs.readFileSync(INV_JSON, "utf8"));

let merged = 0;
let missing = [];

for (const [fieldName, value] of Object.entries(fields)) {
  const row = inventory.find((r) => r.fieldName === fieldName);
  if (!row) {
    missing.push(fieldName);
    continue;
  }
  row.suggestedCopyPaste = value;
  row.verdict = "Change";
  row.isEmpty = false;
  merged += 1;
}

fs.writeFileSync(INV_JSON, JSON.stringify(inventory, null, 2) + "\n", "utf8");

const header = ["tab", "fieldName", "label", "verdict", "currentAnswer", "suggestedCopyPaste"];
const lines = [rowToCsv(header)];
for (const row of inventory) {
  const current = row.isEmpty ? "(empty)" : row.current ?? row.rawValue ?? "";
  lines.push(
    rowToCsv([
      row.tab,
      row.fieldName,
      row.label,
      row.verdict,
      current,
      row.suggestedCopyPaste ?? "",
    ])
  );
}
fs.writeFileSync(INV_CSV, lines.join("\n") + "\n", "utf8");

console.log(
  JSON.stringify(
    {
      mergedIntoInventory: merged,
      missingFromInventory: missing,
      airtableOnlyFieldCount: Object.keys(gap.airtableOnlyFields || {}).length,
      note: "airtableOnlyFields need Platform/Profile patch via writer or direct API — not in 417-row form inventory",
    },
    null,
    2
  )
);
