#!/usr/bin/env node
/**
 * Export open-status Accor census rows still missing amenities (steward review).
 *
 *   node scripts/export-accor-open-amenity-blanks.mjs
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE, STATUS_OPEN } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

function isOpen(statusRaw) {
  const arr = Array.isArray(statusRaw) ? statusRaw : [statusRaw];
  return arr.some((x) => String(x || "").toLowerCase() === STATUS_OPEN.toLowerCase());
}

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

const base = getPlatformBase();
const rows = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [
      "name",
      "Website",
      "Property ID",
      "Amenities",
      "status",
      CENSUS_FIELDS.city,
      CENSUS_FIELDS.country,
      CENSUS_FIELDS.market,
      CENSUS_FIELDS.submarket,
    ],
    filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

const openBlank = rows.filter(
  (r) => isBlankCensusValue(r.fields?.Amenities) && isOpen(r.fields?.status)
);

const header = [
  "censusRecordId",
  "name",
  "city",
  "country",
  "market",
  "submarket",
  "website",
  "propertyId",
  "recommendedAction",
].join(",");

const lines = [header];
for (const rec of openBlank) {
  const f = rec.fields || {};
  lines.push(
    [
      rec.id,
      f.name,
      f[CENSUS_FIELDS.city],
      f[CENSUS_FIELDS.country],
      f[CENSUS_FIELDS.market],
      f[CENSUS_FIELDS.submarket],
      f.Website,
      f["Property ID"],
      "paste all.accor.com/hotel/{code}/index.en.shtml when verified",
    ]
      .map(csvEscape)
      .join(",")
  );
}

mkdirSync(REPORTS, { recursive: true });
const csvPath = join(REPORTS, "accor-open-amenity-blanks-steward.csv");
const jsonPath = join(REPORTS, "accor-open-amenity-blanks-steward.json");
writeFileSync(csvPath, lines.join("\n"));
writeFileSync(
  jsonPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      count: openBlank.length,
      rows: openBlank.map((r) => ({
        id: r.id,
        name: r.fields.name,
        city: r.fields[CENSUS_FIELDS.city],
        country: r.fields[CENSUS_FIELDS.country],
      })),
    },
    null,
    2
  )
);

console.log("Open amenity blanks:", openBlank.length);
console.log("CSV:", csvPath);
console.log("JSON:", jsonPath);
