/**
 * Export Everhome Suites Brand Basics + all presentation rows to JSON.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BRAND = "Everhome Suites";
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const esc = BRAND.replace(/"/g, '\\"');
const basicsRows = await base(BASICS)
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
  .all();

const presRows = await base(TABLE)
  .select({
    filterByFormula: `OR({Brand Name} = "${esc}", {Brand} = "${esc}")`,
    maxRecords: 500,
  })
  .all();

function recToObj(rec) {
  const fields = rec.fields || {};
  const image = fields.Image;
  return {
    id: rec.id,
    slotKey: fields["Slot Key"] || "",
    title: fields.Title || "",
    body: fields.Body || "",
    sort: fields["Sort Order"] ?? null,
    hasImage: Array.isArray(image) && image.length > 0,
    imageCount: Array.isArray(image) ? image.length : 0,
    summaryUrl: fields["Summary URL"] || "",
  };
}

const out = {
  exportedAt: new Date().toISOString(),
  brandName: BRAND,
  basics: basicsRows.map((r) => ({ id: r.id, fields: r.fields })),
  presentation: presRows
    .map(recToObj)
    .sort((a, b) => String(a.slotKey).localeCompare(String(b.slotKey))),
};

const outPath = path.join(ROOT, "fixtures/everhome-airtable-export.json");
fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
console.log(`Wrote ${presRows.length} presentation rows → ${outPath}`);
