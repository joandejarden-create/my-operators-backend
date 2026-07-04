import "../load-env.js";
import Airtable from "airtable";

const q = process.argv[2] || "Everhome";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
for (const r of rows) {
  const n = String(r.get("Brand Name") || "");
  if (n.toLowerCase().includes(q.toLowerCase())) {
    console.log(n, r.id, r.get("Parent Company"));
  }
}
