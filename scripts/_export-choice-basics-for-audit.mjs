/**
 * Export Choice Hotels International Brand Basics narrative fields for audit.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures", "choice-basics-audit-export.json");

const FIELDS = [
  "Brand Name",
  "Parent Company",
  "Hotel Chain Scale",
  "Brand Model",
  "Hotel Service Model",
  "Year Brand Launched",
  "Brand Development Stage",
  "Brand Tagline",
  "Brand Positioning",
  "Brand Customer Promise",
  "Brand Value Proposition",
  "Brand Pillars",
  "Brand History",
  "Target Guest Segments",
  "Guest Psychographics Description",
  "Key Brand Differentiators",
  "Sustainability Positioning",
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
const choice = rows
  .filter((r) =>
    String(r.get("Parent Company") || "").includes("Choice Hotels International")
  )
  .sort((a, b) => String(a.get("Brand Name")).localeCompare(String(b.get("Brand Name"))));

const out = choice.map((r) => {
  const o = { id: r.id };
  for (const f of FIELDS) {
    const v = r.get(f);
    o[f] = Array.isArray(v) ? v : v ?? null;
  }
  return o;
});

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(out, null, 2), "utf8");
console.log(`Exported ${out.length} Choice CHI brands to ${OUT}`);
