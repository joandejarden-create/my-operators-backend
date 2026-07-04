import "../load-env.js";
import Airtable from "airtable";

const names = process.argv.slice(2);
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
for (const n of names) {
  const esc = n.replace(/"/g, '\\"');
  const r = await base("Brand Setup - Brand Basics")
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .all();
  if (!r.length) {
    console.log(n, "NOT FOUND");
    continue;
  }
  console.log("\n===", n, "===");
  console.log("Positioning:", (r[0].get("Brand Positioning") || "").slice(0, 400));
}
