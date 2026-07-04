import "../load-env.js";
import Airtable from "airtable";
import { FOLDER_TO_AIRTABLE_NAME } from "./lib/choice-brand-materials-config.mjs";

const T = "Brand Setup - Brand Basics";
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
];

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const names = [...new Set(Object.values(FOLDER_TO_AIRTABLE_NAME)), "Radisson", "Radisson Blu (Choice)"];

for (const n of names) {
  const esc = n.replace(/"/g, '\\"');
  const r = await base(T)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (!r.length) {
    console.log(`${n}: NOT FOUND`);
    continue;
  }
  const rec = r[0];
  const miss = FIELDS.filter((f) => isEmpty(rec.get(f)));
  console.log(
    `${n} (${rec.id}) PC=${rec.get("Parent Company") || "—"} missing=${miss.length}${
      miss.length ? "\n  " + miss.join(", ") : " (narrative fields full)"
    }`
  );
}
