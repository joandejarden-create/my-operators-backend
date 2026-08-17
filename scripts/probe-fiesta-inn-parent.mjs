#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
const b = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_ALT);
const rows = await b("Hotel Census")
  .select({
    filterByFormula: "FIND('Fiesta Inn', {Affiliation})",
    fields: ["name", "Affiliation", "Parent Company"],
    maxRecords: 100,
  })
  .all();
const parents = {};
for (const r of rows) {
  const p = String(r.get("Parent Company") || "(blank)");
  parents[p] = (parents[p] || 0) + 1;
}
console.log("Fiesta Inn rows:", rows.length);
console.log(parents);
for (const r of rows.filter((x) => String(x.get("Parent Company") || "").includes("Hilton"))) {
  console.log("HILTON:", r.id, r.get("name"), r.get("Parent Company"));
}
