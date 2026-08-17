#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
const b = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_ALT);
const rows = await b("Hotel Census")
  .select({
    filterByFormula: "{Affiliation}='Independent'",
    fields: ["name", "Parent Company"],
    maxRecords: 30,
  })
  .all();
const parents = {};
for (const r of rows) {
  const p = String(r.get("Parent Company") || "(blank)");
  parents[p] = (parents[p] || 0) + 1;
}
console.log("Independent sample parents:", parents);
