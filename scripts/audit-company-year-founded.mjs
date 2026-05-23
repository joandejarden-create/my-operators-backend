/**
 * Audit Company Profile "Year Founded" — list empty vs filled.
 * Usage: node scripts/audit-company-year-founded.mjs
 */
import "../load-env.js";

const TABLE = process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;

async function fetchAll() {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}?${qs}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

function rowFromRecord(r) {
  const name = String(r.fields["Company Name"] || r.fields["Name"] || "(no name)").trim();
  const yf = r.fields["Year Founded"];
  const year = yf == null || String(yf).trim() === "" ? "" : String(yf).trim();
  return {
    id: r.id,
    name,
    year,
    type: String(r.fields["Company Type"] || "").trim(),
    website: String(r.fields["Company Website"] || "").trim(),
    hq: String(r.fields["Headquarters Location"] || r.fields["HQ Location"] || "").trim(),
  };
}

const records = await fetchAll();
const empty = [];
const filled = [];
for (const r of records) {
  const row = rowFromRecord(r);
  if (!row.year) empty.push(row);
  else filled.push(row);
}

console.log("Total:", records.length);
console.log("Empty Year Founded:", empty.length);
console.log("Has Year Founded:", filled.length);
console.log("\n--- EMPTY ---");
for (const r of empty.sort((a, b) => a.name.localeCompare(b.name))) {
  console.log(JSON.stringify(r));
}
console.log("\n--- FILLED ---");
for (const r of filled.sort((a, b) => a.name.localeCompare(b.name))) {
  console.log(JSON.stringify(r));
}
