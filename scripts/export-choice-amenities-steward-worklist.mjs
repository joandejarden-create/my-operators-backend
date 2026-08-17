#!/usr/bin/env node
/**
 * Export Choice census rows with Website but blank Amenities (steward worklist).
 */
import "../load-env.js";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";
import { choicePropertyIdFromUrl } from "../lib/choice-hotel-content-fetch.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";

const MAP = {
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  website: "Website",
  name: CENSUS_FIELDS.name,
  country: CENSUS_FIELDS.country,
  city: CENSUS_FIELDS.city,
  parentCompany: CENSUS_FIELDS.parentCompany,
};

const OUT = join("reports", "choice-amenities-steward-worklist.csv");

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [MAP.name, MAP.website, MAP.propertyId, MAP.amenities, MAP.country, MAP.city],
    filterByFormula: `FIND("Choice", {${MAP.parentCompany}})`,
  })
  .all();

const rows = records
  .filter((r) => {
    const url = String(r.fields?.[MAP.website] || "").trim();
    const pid = String(r.fields?.[MAP.propertyId] || choicePropertyIdFromUrl(url) || "").trim();
    return (
      url &&
      /choicehotels\.com/i.test(url) &&
      pid &&
      isBlankCensusValue(r.fields?.[MAP.amenities])
    );
  })
  .map((r) => {
    const pid = (
      r.fields[MAP.propertyId] ||
      choicePropertyIdFromUrl(r.fields[MAP.website]) ||
      r.id
    ).toLowerCase();
    return {
      censusRecordId: r.id,
      censusName: r.fields[MAP.name],
      censusCity: r.fields[MAP.city] || "",
      censusCountry: r.fields[MAP.country] || "",
      propertyId: r.fields[MAP.propertyId] || choicePropertyIdFromUrl(r.fields[MAP.website]),
      propertyUrl: r.fields[MAP.website],
      htmlFile: `reports/choice-amenity-html/${pid}.html`,
      instructions:
        "Open propertyUrl in browser → Ctrl+S Webpage Complete → save as htmlFile → apply-choice-amenities-from-html.mjs (dry-run) then --apply. See reports/choice-amenities-steward-runbook.md",
    };
  });

mkdirSync("reports", { recursive: true });
mkdirSync("reports/choice-amenity-html", { recursive: true });
writeCsv(
  OUT,
  rows,
  [
    "censusRecordId",
    "censusName",
    "censusCity",
    "censusCountry",
    "propertyId",
    "propertyUrl",
    "htmlFile",
    "instructions",
  ]
);

console.log(`Wrote ${rows.length} rows to ${OUT}`);
