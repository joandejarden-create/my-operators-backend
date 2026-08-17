#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const PROPERTY_ID_FIELD =
  process.env.AIRTABLE_CENSUS_PROPERTY_ID_FIELD || "Property ID";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

const formula = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;
const rows = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: formula,
    fields: ["name", PROPERTY_ID_FIELD, "Website"],
    maxRecords: 20,
  })
  .all();

let withPid = 0;
let blank = 0;
for (const r of rows) {
  const pid = r.get(PROPERTY_ID_FIELD);
  if (pid != null && pid !== "") withPid++;
  else blank++;
  const web = String(r.get("Website") || "");
  const m = web.match(/\/hotels\/([a-z0-9]+)-/i);
  console.log(
    JSON.stringify({
      name: r.get("name"),
      propertyId: pid,
      propertyIdType: typeof pid,
      websiteCode: m ? m[1].toUpperCase() : "",
    })
  );
}
console.log("\nSample stats:", { sampled: rows.length, withPid, blank });
