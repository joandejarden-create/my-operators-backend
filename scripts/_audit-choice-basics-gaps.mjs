/**
 * Quick audit: Choice-related Brand Basics rows and empty text fields.
 * node scripts/_audit-choice-basics-gaps.mjs
 */
import "../load-env.js";
import Airtable from "airtable";

const T = "Brand Setup - Brand Basics";
const TEXT_FIELDS = [
  "Parent Company",
  "Hotel Chain Scale",
  "Brand Model",
  "Hotel Service Model",
  "Year Brand Launched",
  "Brand Development Stage",
  "Brand Positioning",
  "Brand Tagline",
  "Brand Customer Promise",
  "Brand Value Proposition",
  "Brand Pillars",
  "Brand History",
  "Target Guest Segments",
  "Guest Psychographics Description",
  "Key Brand Differentiators",
  "Sustainability Positioning",
  "Brand Architecture",
  "Region Offered",
];

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function isChoiceRelated(rec) {
  const pc = String(rec.get("Parent Company") || "").toLowerCase();
  return pc.includes("alpha brand studios");
}

async function main() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
    process.exit(1);
  }
  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await base(T).select({ maxRecords: 500 }).all();
  const choice = rows.filter(isChoiceRelated);
  console.log(`Choice-related Brand Basics: ${choice.length} of ${rows.length} total\n`);

  let totalMissing = 0;
  for (const r of choice.sort((a, b) =>
    String(a.get("Brand Name")).localeCompare(String(b.get("Brand Name")))
  )) {
    const name = r.get("Brand Name");
    const missing = TEXT_FIELDS.filter((f) => isEmpty(r.get(f)));
    totalMissing += missing.length;
    if (missing.length) {
      console.log(`${name} (${r.id})`);
      console.log(`  missing ${missing.length}: ${missing.join(", ")}`);
    }
  }
  const full = choice.filter((r) => !TEXT_FIELDS.some((f) => isEmpty(r.get(f))));
  console.log(`\nFully filled (tracked fields): ${full.length}`);
  console.log(`Total empty field slots: ${totalMissing}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
