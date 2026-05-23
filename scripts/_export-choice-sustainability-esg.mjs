/**
 * Export Sustainability & ESG linked rows for Choice CHI brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures", "choice-sustainability-esg-export.json");

const FIELDS = [
  "Brand Name",
  "Sustainability Programs",
  "ESG Reporting",
  "Carbon Footprint Tracking",
  "Energy Efficiency Initiatives",
  "Waste Reduction Programs",
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const basics = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
const choice = basics.filter((r) =>
  String(r.get("Parent Company") || "").includes("Choice Hotels International")
);

const esgRows = await base("Brand Setup - Sustainability & ESG").select({ maxRecords: 500 }).all();
const byName = new Map();
for (const r of esgRows) {
  const n = r.get("Brand Name");
  if (n) byName.set(String(n).trim(), r);
}

const out = [];
for (const b of choice.sort((a, b) => String(a.get("Brand Name")).localeCompare(String(b.get("Brand Name"))))) {
  const name = b.get("Brand Name");
  const esg = byName.get(String(name).trim());
  const o = {
    basicsId: b.id,
    brandName: name,
    esgRecordId: esg?.id || null,
  };
  if (esg) {
    for (const f of FIELDS) o[f] = esg.get(f) ?? null;
  }
  out.push(o);
}

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(out, null, 2), "utf8");
console.log(`Exported ${out.length} brands (${out.filter((x) => x.esgRecordId).length} with ESG rows) → ${OUT}`);
