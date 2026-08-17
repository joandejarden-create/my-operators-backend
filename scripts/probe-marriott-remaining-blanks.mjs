#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { censusCountryToSitemapSlug } from "../lib/marriott-brand-directory-extract.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
    fields: [CENSUS_FIELDS.name, CENSUS_FIELDS.country, CENSUS_FIELDS.status, MAP_DIRECTORY_ENRICHMENT.website, CENSUS_PROPERTY_ID_FIELD],
    pageSize: 100,
  })
  .all();

const blank = recs.filter((r) => isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website)));
const noSitemap = blank.filter((r) => !censusCountryToSitemapSlug(r.get(CENSUS_FIELDS.country)));
console.log("Blank website:", blank.length);
console.log("No country sitemap mapping:", noSitemap.length);
console.log("\nNo sitemap country sample:");
for (const r of noSitemap.slice(0, 15)) {
  console.log(" ", r.get(CENSUS_FIELDS.name), "|", r.get(CENSUS_FIELDS.country), "| status:", r.get(CENSUS_FIELDS.status));
}
console.log("\nHas sitemap but still blank sample:");
for (const r of blank.filter((x) => censusCountryToSitemapSlug(x.get(CENSUS_FIELDS.country))).slice(0, 15)) {
  console.log(" ", r.get(CENSUS_FIELDS.name), "|", r.get(CENSUS_FIELDS.country), "| pid:", r.get(CENSUS_PROPERTY_ID_FIELD) || "(blank)");
}
