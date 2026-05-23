import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
for (const r of rows) {
  const v = r.get("Target Guest Segments");
  if (v && (Array.isArray(v) ? v.length : String(v).length)) {
    console.log(r.get("Brand Name"), JSON.stringify(v));
    break;
  }
}
const withSeg = rows.filter((r) => {
  const v = r.get("Target Guest Segments");
  return Array.isArray(v) && v.length;
});
console.log("count with segments:", withSeg.length);
if (withSeg[0]) console.log("example:", withSeg[0].get("Brand Name"), withSeg[0].get("Target Guest Segments"));
