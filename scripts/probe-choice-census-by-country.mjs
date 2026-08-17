#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", CENSUS_FIELDS.country, CENSUS_FIELDS.parentCompany],
    filterByFormula: `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`,
  })
  .all();

/** @type {Record<string, { total: number; blankWeb: number }>} */
const byCountry = {};
for (const r of recs) {
  const c = r.fields[CENSUS_FIELDS.country] || "?";
  if (!byCountry[c]) byCountry[c] = { total: 0, blankWeb: 0 };
  byCountry[c].total++;
  if (isBlankCensusValue(r.fields.Website)) byCountry[c].blankWeb++;
}

console.log(JSON.stringify(byCountry, null, 2));
