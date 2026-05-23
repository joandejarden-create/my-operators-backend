import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
const opts = new Set();
for (const r of rows) {
  const v = r.get("Target Guest Segments");
  if (Array.isArray(v)) v.forEach((x) => opts.add(x));
}
console.log([...opts].sort().join("\n"));
