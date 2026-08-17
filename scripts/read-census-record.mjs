#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";

const id = process.argv[2] || "rec54jkFM0zveGu7P";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const rec = await base(HOTEL_CENSUS_TABLE).find(id);
const f = rec.fields;
console.log(JSON.stringify({
  id: rec.id,
  name: f[CENSUS_FIELDS.name],
  country: f[CENSUS_FIELDS.country],
  website: f[MAP_DIRECTORY_ENRICHMENT.website],
  propertyId: f[CENSUS_PROPERTY_ID_FIELD],
  description: f[CENSUS_DESCRIPTION_FIELD],
  amenities: f[CENSUS_AMENITIES_TEXT_FIELD],
}, null, 2));
