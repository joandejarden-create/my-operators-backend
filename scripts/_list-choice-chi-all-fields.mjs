import "../load-env.js";
import Airtable from "airtable";

const FIELDS = [
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
  "Brand Model",
  "Hotel Service Model",
  "Year Brand Launched",
  "Brand Development Stage",
];
const isE = (v) => v == null || v === "" || (Array.isArray(v) && !v.length);

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
const choice = rows.filter((r) =>
  String(r.get("Parent Company") || "").includes("Choice Hotels International")
);
for (const r of choice.sort((a, b) =>
  String(a.get("Brand Name")).localeCompare(String(b.get("Brand Name")))
)) {
  const miss = FIELDS.filter((f) => isE(r.get(f)));
  if (miss.length) {
    console.log(`${r.get("Brand Name")} (${r.id}): ${miss.join(", ")}`);
  }
}
console.log(`\n${choice.length} Choice Hotels International rows; ${choice.filter((r) => FIELDS.some((f) => isE(r.get(f)))).length} with any gaps.`);
