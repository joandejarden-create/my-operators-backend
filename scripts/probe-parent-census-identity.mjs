#!/usr/bin/env node
import "../load-env.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const parent = process.argv[2] || "IHG";
const extra = ["Website", "Property ID", "Amenities"];
const base = getPlatformBase();
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", CENSUS_FIELDS.parentCompany, CENSUS_FIELDS.city, CENSUS_FIELDS.country, ...extra],
    filterByFormula: `FIND("${parent}", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();
const stats = { web: 0, propId: 0, amen: 0 };
for (const r of recs) {
  if (r.get("Website")) stats.web++;
  if (r.get("Property ID")) stats.propId++;
  if (String(r.get("Amenities") || "").trim()) stats.amen++;
}
console.log(parent, "rows", recs.length, stats);
console.log("sample:");
for (const r of recs.slice(0, 8)) {
  console.log(
    r.get("name"),
    "| web:", r.get("Website") || "-",
    "| pid:", r.get("Property ID") || "-"
  );
}
