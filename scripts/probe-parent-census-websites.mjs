#!/usr/bin/env node
import "../load-env.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const parent = process.argv[2] || "IHG";
const base = getPlatformBase();
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", CENSUS_FIELDS.parentCompany, "Amenities"],
    filterByFormula: `FIND("${parent}", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();
let withWeb = 0;
let withAmen = 0;
for (const r of recs) {
  if (r.get("Website")) withWeb++;
  if (String(r.get("Amenities") || "").trim()) withAmen++;
}
console.log("parent", parent, "total", recs.length, "website", withWeb, "amenities", withAmen);
for (const r of recs.filter((x) => x.get("Website")).slice(0, 5)) {
  console.log(" ", r.get("name"), r.get("Website"));
}
