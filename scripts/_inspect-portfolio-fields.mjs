import "../load-env.js";
import Airtable from "airtable";

const brandName = process.argv[2] || "Comfort Inn & Suites";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const esc = brandName.replace(/"/g, '\\"');
const rows = await base("Brand Setup - Portfolio & Performance")
  .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
  .all();
for (const r of rows) {
  console.log("id", r.id);
  for (const k of Object.keys(r.fields).sort()) {
    if (/minimum|maximum|property|room/i.test(k)) console.log(JSON.stringify(k), "=", r.fields[k]);
  }
}
