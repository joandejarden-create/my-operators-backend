#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_ALT);
const recs = await base(HOTEL_CENSUS_TABLE).select({
  filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
  fields: [CENSUS_FIELDS.name, CENSUS_FIELDS.status, MAP_DIRECTORY_ENRICHMENT.website, CENSUS_PROPERTY_ID_FIELD],
  pageSize: 100,
}).all();

function isOpen(r) {
  const s = r.get(CENSUS_FIELDS.status);
  if (Array.isArray(s)) return s.some((x) => /open/i.test(String(x)));
  return /open/i.test(String(s || ""));
}

const openBlank = recs.filter((r) => isOpen(r) && (isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) || isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))));
console.log("Open missing website or PID:", openBlank.length);
for (const r of openBlank) {
  console.log("-", r.get(CENSUS_FIELDS.name), "| web:", r.get(MAP_DIRECTORY_ENRICHMENT.website) || "(blank)", "| pid:", r.get(CENSUS_PROPERTY_ID_FIELD) || "(blank)");
}

const anyBlank = recs.filter((r) => isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) || isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website)));
console.log("\nAll missing website or PID:", anyBlank.length);
