#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
    fields: [
      MAP_DIRECTORY_ENRICHMENT.website,
      CENSUS_PROPERTY_ID_FIELD,
      CENSUS_DESCRIPTION_FIELD,
      CENSUS_AMENITIES_TEXT_FIELD,
      CENSUS_FIELDS.name,
    ],
    pageSize: 100,
  })
  .all();

let withWeb = 0;
let withMarsha = 0;
let withEither = 0;
let blankDesc = 0;
for (const r of recs) {
  const w = String(r.fields[MAP_DIRECTORY_ENRICHMENT.website] || "").trim();
  const m = String(r.fields[CENSUS_PROPERTY_ID_FIELD] || "").trim();
  if (w) withWeb++;
  if (m) withMarsha++;
  if (w || m) withEither++;
  if (!String(r.fields[CENSUS_DESCRIPTION_FIELD] || "").trim()) blankDesc++;
}
console.log({ total: recs.length, withWeb, withMarsha, withEither, blankDesc });
