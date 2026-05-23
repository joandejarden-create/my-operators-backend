import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const names = [
  "Comfort Inn & Suites",
  "Radisson Individual (Choice)",
  "Park Inn by Radisson (Choice)",
];
for (const n of names) {
  const esc = n.replace(/"/g, '\\"');
  const r = await base("Brand Setup - Brand Basics")
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .all();
  const rec = r[0];
  if (!rec) continue;
  const h = rec.get("Brand History");
  const s = rec.get("Sustainability Positioning");
  console.log("\n", n);
  console.log("  History:", h ? `len ${String(h).length}` : "EMPTY");
  console.log("  Sustainability:", s ? `len ${String(s).length}` : "EMPTY");
  if (s) console.log("  ", String(s).slice(0, 200));
}
