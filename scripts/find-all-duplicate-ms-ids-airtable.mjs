/**
 * Find duplicate Unique_Webflow_ID / Slug values across all Users rows.
 */
import "../load-env.js";
import Airtable from "airtable";
import { cellToString } from "../lib/airtable-utils.js";

const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const byMs = new Map();
const orphans = [];

await base(USERS_TABLE)
  .select({ pageSize: 100 })
  .eachPage((records, next) => {
    for (const rec of records) {
      const f = rec.fields || {};
      const ms =
        cellToString(f.Unique_Webflow_ID) ||
        cellToString(f["Unique Webflow ID"]) ||
        cellToString(f.Slug) ||
        "";
      const email = cellToString(f.Email) || "";
      const deals = Array.isArray(f.Deals) ? f.Deals.length : 0;
      const summary = { recordId: rec.id, email, ms, deals };

      if (!ms) {
        if (!email && deals === 0) orphans.push(summary);
        next();
        continue;
      }

      if (!byMs.has(ms)) byMs.set(ms, []);
      byMs.get(ms).push(summary);
    }
    next();
  });

const dupes = [...byMs.entries()].filter(([, rows]) => rows.length > 1);
console.log("Users rows with duplicate Memberstack id:", dupes.length);
for (const [ms, rows] of dupes) {
  console.log("\n", ms);
  for (const r of rows) {
    console.log(`  ${r.recordId} | email:${r.email || "-"} | deals:${r.deals}`);
  }
}

console.log("\nEmpty orphan rows (no email, no ms id, no deals):", orphans.length);
for (const r of orphans.slice(0, 10)) {
  console.log(`  ${r.recordId}`);
}
if (orphans.length > 10) console.log(`  … and ${orphans.length - 10} more`);
