import "../load-env.js";
import Airtable from "airtable";

const names = [
  "Cambria Hotels",
  "Clarion",
  "Quality Inn",
  "Sleep Inn",
  "Ascend Hotel Collection",
];
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
for (const n of names) {
  const esc = n.replace(/"/g, '\\"');
  const r = await base("Brand Setup - Brand Basics")
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .all();
  const h = r[0]?.get("Brand History");
  console.log(n, h ? `len=${String(h).length}` : "EMPTY", h ? String(h).slice(0, 100) : "");
}
