import "../load-env.js";
import Airtable from "airtable";

const names = [
  "Radisson Individual (Choice)",
  "Radisson Individuals (Choice)",
  "Clarion Pointe",
  "Radisson Inn & Suites",
  "Everhome Suites",
  "Clarion",
  "Radisson (Choice)",
  "Ascend Hotel Collection",
  "MainStay Suites",
  "Cambria Hotels",
  "Quality Inn",
  "Comfort Inn & Suites",
];
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const rows = await base("Brand Setup - Brand Basics").select({ maxRecords: 500 }).all();
for (const n of names) {
  const r = rows.find((x) => String(x.get("Brand Name")) === n);
  if (r) console.log(n, "→", r.get("Hotel Chain Scale") || "(empty)");
}
