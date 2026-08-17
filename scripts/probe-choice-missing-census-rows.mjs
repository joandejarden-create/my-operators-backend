#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const countries = ["Jamaica", "Bolivia", "US Virgin Islands"];
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

for (const country of countries) {
  const recs = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", "Website", "Property ID", CENSUS_FIELDS.city, CENSUS_FIELDS.country],
      filterByFormula: `AND(FIND("Choice", {${CENSUS_FIELDS.parentCompany}}), {${CENSUS_FIELDS.country}}="${country}")`,
    })
    .all();
  console.log(`\n${country} (${recs.length})`);
  for (const r of recs) {
    console.log(`  ${r.id} | ${r.fields.name} | ${r.fields.city} | web=${r.fields.Website || ""} | pid=${r.fields["Property ID"] || ""}`);
  }
}
