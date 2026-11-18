import "dotenv/config";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const id = "recLuxvwwxID7U2B8";
const formula = `RECORD_ID()='${id}'`;

const full = await base("Hotel Census")
  .select({ filterByFormula: formula, pageSize: 1 })
  .all();
console.log(
  JSON.stringify(
    {
      fullCount: full.length,
      keys: full[0] ? Object.keys(full[0].fields).sort() : null,
      name: full[0]?.fields?.name ?? null,
      propertyName: full[0]?.fields?.["Property Name"] ?? null,
      city: full[0]?.fields?.city ?? full[0]?.fields?.City ?? null,
      country: full[0]?.fields?.country ?? full[0]?.fields?.Country ?? null,
    },
    null,
    2
  )
);

const lean = await base("Hotel Census")
  .select({
    filterByFormula: formula,
    fields: ["name", "city", "country", "Latitude", "Longitude", "Market"],
    pageSize: 1,
  })
  .all();
console.log(
  JSON.stringify(
    {
      leanCount: lean.length,
      leanFields: lean[0]?.fields ?? null,
    },
    null,
    2
  )
);

// Is Bethesda among first N of unfiltered lean map select?
let found = false;
let scanned = 0;
await base("Hotel Census")
  .select({
    fields: ["name", "Latitude", "Longitude", "country"],
    pageSize: 100,
  })
  .eachPage((records, next) => {
    scanned += records.length;
    if (records.some((r) => r.id === id)) found = true;
    if (found || scanned >= 2000) return; // stop early via not calling next? must call or break
    if (found) return;
    next();
  });
console.log(JSON.stringify({ scanned, foundInFirstPages: found }));
