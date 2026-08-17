import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const rows = await base("Brand Setup - Brand Explorer Presentation")
  .select({
    filterByFormula: `AND({Brand Name} = "Country Inn & Suites by Radisson", OR({Slot Key} = "footprint.openings", FIND("footprint.momentum", {Slot Key}) > 0))`,
    maxRecords: 20,
  })
  .all();

console.log(`\n=== Country Inn footprint (${rows.length} rows) ===\n`);
for (const r of rows.sort((a, b) => (a.get("Sort Order") || 0) - (b.get("Sort Order") || 0))) {
  console.log(`${r.get("Slot Key")} | ${r.id}`);
  console.log(`  title: ${r.get("Title") || "(label)"}`);
  const url = String(r.get("Body") || "").match(/https:\/\/[^\s]+/)?.[0];
  if (url) console.log(`  url: ${url}`);
}
