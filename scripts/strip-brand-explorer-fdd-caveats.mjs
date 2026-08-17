/**
 * Strip FDD diligence caveats from Brand Explorer presentation rows (one brand).
 *
 *   node scripts/strip-brand-explorer-fdd-caveats.mjs --dry-run --brand "Country Inn & Suites by Choice"
 *   node scripts/strip-brand-explorer-fdd-caveats.mjs --brand "Country Inn & Suites by Choice"
 */
import "../load-env.js";
import Airtable from "airtable";
import { sanitizeExternalCopy } from "../lib/external-owner-copy.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const TEXT_FIELDS = [
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Owner Objective",
  "Case Summary Brand Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
];

function parseArgs(argv) {
  const dryRun = argv.includes("--dry-run");
  const i = argv.indexOf("--brand");
  const brand = i >= 0 ? String(argv[i + 1] || "").trim() : "";
  if (!brand) throw new Error("--brand required");
  return { dryRun, brand };
}

const { dryRun, brand } = parseArgs(process.argv);
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const esc = brand.replace(/"/g, '\\"');
const rows = await base(TABLE)
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
  .all();

let updated = 0;
for (const rec of rows) {
  const fields = {};
  let changed = false;
  for (const f of TEXT_FIELDS) {
    const raw = rec.get(f);
    if (raw == null || raw === "") continue;
    const clean = sanitizeExternalCopy(String(raw));
    if (clean !== String(raw).trim()) {
      fields[f] = clean;
      changed = true;
    }
  }
  if (!changed) continue;
  console.log(`${dryRun ? "Would update" : "Updating"} ${rec.get("Slot Key")} (${rec.id})`);
  if (!dryRun) await base(TABLE).update(rec.id, fields);
  updated++;
}
console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
