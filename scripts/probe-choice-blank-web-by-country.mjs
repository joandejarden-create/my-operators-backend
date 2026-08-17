#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const country = process.argv[2] || "Brazil";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", CENSUS_FIELDS.city],
    filterByFormula: `AND(FIND("Choice", {${CENSUS_FIELDS.parentCompany}}), {${CENSUS_FIELDS.country}}="${country}")`,
  })
  .all();
for (const r of recs.filter((x) => isBlankCensusValue(x.fields.Website))) {
  console.log(r.id, r.fields.name, r.fields[CENSUS_FIELDS.city]);
}
