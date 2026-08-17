#!/usr/bin/env node
import "../load-env.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const base = getPlatformBase();
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", CENSUS_FIELDS.affiliation, "Amenities"],
    filterByFormula: `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`,
    maxRecords: 20,
  })
  .all();
for (const r of recs) {
  console.log(r.id, r.get(CENSUS_FIELDS.name), "|", r.get(CENSUS_FIELDS.website) || "(no web)", "| amen:", (r.get("Amenities") || "").slice(0, 40));
}
console.log("sample count", recs.length);
