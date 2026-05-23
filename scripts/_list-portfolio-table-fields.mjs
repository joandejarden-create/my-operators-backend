import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Portfolio & Performance").select({ maxRecords: 3 }).all();
const keys = new Set();
for (const r of rows) Object.keys(r.fields).forEach((k) => keys.add(k));
console.log([...keys].sort().join("\n"));
