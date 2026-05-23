import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
for (const r of rows) {
  const s = r.get("Sustainability Positioning");
  if (s && String(s).length > 50) {
    console.log(r.get("Brand Name"), "len", String(s).length);
    console.log(String(s).slice(0, 300));
    break;
  }
}
