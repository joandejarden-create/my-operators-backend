import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const samples = ["Sleep Inn", "MainStay Suites", "Clarion", "WoodSpring Suites"];
for (const n of samples) {
  const esc = n.replace(/"/g, '\\"');
  const r = await base("Brand Setup - Brand Basics")
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .all();
  const rec = r[0];
  if (!rec) continue;
  console.log("\n", n);
  console.log("  Service Model:", rec.get("Hotel Service Model"));
  console.log("  Year Launched:", rec.get("Year Brand Launched"));
  console.log("  Dev Stage:", rec.get("Brand Development Stage"));
  console.log("  Brand Model:", rec.get("Brand Model"));
}
