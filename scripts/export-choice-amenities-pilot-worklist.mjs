#!/usr/bin/env node
/**
 * Export a small pilot steward worklist (~10–15 CALA Choice rows).
 * Prefer Mexico / Costa Rica; require Property ID + Website; blank Amenities only.
 *
 *   node scripts/export-choice-amenities-pilot-worklist.mjs
 *   node scripts/export-choice-amenities-pilot-worklist.mjs --limit 12
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
import { isCalaCountry } from "../lib/gtm-owner-target/cala-footprint.js";

const MAP = {
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  website: "Website",
  name: CENSUS_FIELDS.name,
  country: CENSUS_FIELDS.country,
  city: CENSUS_FIELDS.city,
  parentCompany: CENSUS_FIELDS.parentCompany,
};

const OUT = join("reports", "choice-amenities-pilot-worklist.csv");

const limitEq = process.argv.find((a) => a.startsWith("--limit="));
const limitIdx = process.argv.indexOf("--limit");
const LIMIT = Math.min(
  15,
  Math.max(
    10,
    Number(
      limitEq?.split("=")[1] ||
        (limitIdx >= 0 && process.argv[limitIdx + 1]) ||
        12
    ) || 12
  )
);

/** Prefer Mexico, then Costa Rica, then other CALA. */
function countryRank(country) {
  const c = String(country || "").trim().toLowerCase();
  if (c === "mexico") return 0;
  if (c === "costa rica") return 1;
  return 2;
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [
      MAP.name,
      MAP.website,
      MAP.propertyId,
      MAP.amenities,
      MAP.country,
      MAP.city,
      MAP.parentCompany,
    ],
    filterByFormula: `FIND("Choice", {${MAP.parentCompany}})`,
  })
  .all();

const eligible = records
  .map((r) => {
    const url = String(r.fields?.[MAP.website] || "").trim();
    const pid = String(
      r.fields?.[MAP.propertyId] || choicePropertyIdFromUrl(url) || ""
    ).trim();
    const country = r.fields?.[MAP.country] || "";
    return {
      rec: r,
      url,
      pid,
      country,
      city: r.fields?.[MAP.city] || "",
      name: r.fields?.[MAP.name] || "",
    };
  })
  .filter(
    (row) =>
      row.url &&
      /choicehotels\.com/i.test(row.url) &&
      row.pid &&
      isBlankCensusValue(row.rec.fields?.[MAP.amenities]) &&
      isCalaCountry(row.country)
  )
  .sort((a, b) => {
    const rank = countryRank(a.country) - countryRank(b.country);
    if (rank !== 0) return rank;
    return String(a.pid).localeCompare(String(b.pid), undefined, { sensitivity: "base" });
  });

const selected = eligible.slice(0, LIMIT);

const rows = selected.map((row) => {
  const pidLower = row.pid.toLowerCase();
  return {
    censusRecordId: row.rec.id,
    censusName: row.name,
    censusCity: row.city,
    censusCountry: row.country,
    propertyId: row.pid,
    propertyUrl: row.url,
    htmlFile: `reports/choice-amenity-html/${pidLower}.html`,
    instructions:
      "Open propertyUrl in Chrome/Edge → wait for amenities → Ctrl+S Webpage Complete → save as htmlFile → node scripts/apply-choice-amenities-from-html.mjs (dry-run) then --apply",
  };
});

mkdirSync("reports", { recursive: true });
mkdirSync("reports/choice-amenity-html", { recursive: true });
writeCsv(OUT, rows, [
  "censusRecordId",
  "censusName",
  "censusCity",
  "censusCountry",
  "propertyId",
  "propertyUrl",
  "htmlFile",
  "instructions",
]);

const byCountry = rows.reduce((acc, r) => {
  acc[r.censusCountry] = (acc[r.censusCountry] || 0) + 1;
  return acc;
}, {});

console.log(`Pilot worklist: ${rows.length} rows → ${OUT}`);
console.log("By country:", byCountry);
console.log(`Eligible pool: ${eligible.length} (capped at ${LIMIT})`);
console.log("Next: node scripts/generate-choice-amenity-pilot-opener.mjs");
