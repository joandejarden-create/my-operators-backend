/**
 * Quick probe: verify Amenities + Website on a Hotel Census record by name search.
 * Usage: node scripts/probe-brand-presence-hotel-fields.mjs "Zemi"
 */
import "dotenv/config";
import Airtable from "airtable";

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
const search = process.argv[2] || "Zemi";

if (!key || !baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  process.exit(1);
}

const base = new Airtable({ apiKey: key }).base(baseId);
const formula = `SEARCH('${search.replace(/'/g, "\\'")}', {name})`;

const records = await base("Hotel Census")
  .select({
    filterByFormula: formula,
    maxRecords: 3,
    fields: ["name", "Amenities", "Website", "Telephone"]
  })
  .all();

for (const rec of records) {
  console.log(JSON.stringify({
    id: rec.id,
    name: rec.get("name"),
    amenities: rec.get("Amenities") ?? null,
    website: rec.get("Website") ?? null,
    telephone: rec.get("Telephone") ?? null
  }, null, 2));
}

if (!records.length) console.log("No records found for:", search);
