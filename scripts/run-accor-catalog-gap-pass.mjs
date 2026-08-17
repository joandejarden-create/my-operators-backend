#!/usr/bin/env node
/**
 * Gap pass: catalog city queries driven by Accor census rows still missing amenities.
 * Supplements country-level pass (Brazil 300 cap) and zero-catalog countries.
 *
 *   node scripts/run-accor-catalog-gap-pass.mjs
 *   node scripts/run-accor-catalog-gap-pass.mjs --apply --fetch-amenities
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync, appendFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { fetchAccorCatalogHotels, accorCountryNameToCode } from "../lib/accor-catalog-api.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "../lib/hotel-census/match-brand-directory-to-census.js";
import { mapExtractRowToDirectoryMatchRow } from "../lib/hotel-census/plan-brand-census-directory-match.js";
import { normalizeCountry, countriesMatch } from "../lib/independent-census/match-current-census.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const LOG_PATH = join(REPORTS, "accor-steward-verified-applies.csv");

/** Territories not in ACCOR_COUNTRY_CODE_TO_LABEL */
const EXTRA_COUNTRY_CODES = {
  "french guiana": "GF",
  guadeloupe: "GP",
  martinique: "MQ",
};

const EXTRA_CITY_QUERIES = [
  { query: "cayenne", country: "French Guiana", countryCode: "GF" },
  { query: "cartagena", country: "Colombia", countryCode: "CO" },
  { query: "medellin", country: "Colombia", countryCode: "CO" },
  { query: "guayaquil", country: "Ecuador", countryCode: "EC" },
  { query: "tulum", country: "Mexico", countryCode: "MX" },
  { query: "mazatlan", country: "Mexico", countryCode: "MX" },
];

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 500),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 70),
    queryDelayMs: Number(args.find((a) => a.startsWith("--query-delay-ms="))?.split("=")[1] || 180),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function countryToCode(countryName) {
  const key = String(countryName || "").trim().toLowerCase();
  return EXTRA_COUNTRY_CODES[key] || accorCountryNameToCode(countryName);
}

function cityQuerySlug(city) {
  return String(city || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function matchCatalogToCensus(catalogHotels, censusRows, minScore) {
  const pairs = [];
  for (const cat of catalogHotels) {
    const dirMatch = mapExtractRowToDirectoryMatchRow(
      {
        inferredHotelName: cat.name,
        city: cat.city,
        country: cat.country,
        propertyId: cat.propertyId,
        propertyUrl: cat.propertyUrl,
        latitude: cat.latitude,
        longitude: cat.longitude,
        source: "accor_catalog_api",
      },
      { scoringProfile: "accor" }
    );

    for (const censusRow of censusRows) {
      if (!countriesMatch(cat.country, censusRow.country)) continue;
      const scored = scoreDirectoryAgainstCensus(dirMatch, censusRow);
      if (scored.score < minScore) continue;
      if (scored.confidence === "none") continue;
      pairs.push({ cat, censusRow, scored, score: scored.score });
    }
  }

  pairs.sort((a, b) => b.score - a.score);
  const usedCensus = new Set();
  const usedCat = new Set();
  const assigned = [];
  for (const p of pairs) {
    const id = p.cat.propertyId;
    if (usedCensus.has(p.censusRow.recordId) || usedCat.has(id)) continue;
    usedCensus.add(p.censusRow.recordId);
    usedCat.add(id);
    assigned.push(p);
  }
  return assigned;
}

function buildApplyPlan(assigned) {
  const plan = [];
  for (const row of assigned) {
    const f = row.censusRow.fields || {};
    const cat = row.cat;
    const url = cat.propertyUrl || accorCanonicalPropertyUrl(cat.propertyId);
    const applyFields = {};

    if (isBlankCensusValue(f.Website) && url) applyFields.Website = url;
    if (isBlankCensusValue(f["Property ID"]) && cat.propertyId) {
      applyFields["Property ID"] = cat.propertyId;
    }
    if (isBlankCensusValue(f.Telephone) && cat.telephone) applyFields.Telephone = cat.telephone;
    if (isBlankCensusValue(f["Address 1"]) && cat.address1) applyFields["Address 1"] = cat.address1;
    if (isBlankCensusValue(f["Postal Code"]) && cat.postalCode) {
      applyFields["Postal Code"] = cat.postalCode;
    }
    if (isBlankCensusValue(f.Latitude) && cat.latitude != null) applyFields.Latitude = cat.latitude;
    if (isBlankCensusValue(f.Longitude) && cat.longitude != null) {
      applyFields.Longitude = cat.longitude;
    }

    const needsAmenities = isBlankCensusValue(f.Amenities);
    if (!Object.keys(applyFields).length && !needsAmenities) continue;

    plan.push({
      censusRecordId: row.censusRow.recordId,
      censusName: row.censusRow.name,
      propertyId: cat.propertyId,
      propertyUrl: url,
      catalogName: cat.name,
      matchScore: row.score,
      matchConfidence: row.scored.confidence,
      applyFields,
      needsAmenities,
    });
  }
  return plan;
}

const opts = parseArgs();
console.log("=== Accor catalog gap pass (city queries from blanks) ===\n");

const base = getPlatformBase();
const allAccor = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [
      "name",
      "Website",
      "Amenities",
      "Property ID",
      "Telephone",
      "Address 1",
      "Postal Code",
      "Latitude",
      "Longitude",
      CENSUS_FIELDS.city,
      CENSUS_FIELDS.country,
    ],
    filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

const blankRecords = allAccor.filter((r) => isBlankCensusValue(r.fields?.Amenities));
const blankRows = blankRecords.map(mapCensusRowForDirectoryMatch);
console.log("Accor amenity blanks:", blankRows.length);

/** @type {{ query: string, country: string, countryCode: string }[]} */
const queries = [];
const seenQuery = new Set();

for (const rec of blankRecords) {
  const country = String(rec.fields?.country || "").trim();
  const city = String(rec.fields?.city || "").trim();
  if (!country || !city) continue;
  const query = cityQuerySlug(city);
  if (!query || query.length < 3) continue;
  const key = `${country}|${query}`;
  if (seenQuery.has(key)) continue;
  seenQuery.add(key);
  queries.push({
    query,
    country,
    countryCode: countryToCode(country),
  });
}

for (const extra of EXTRA_CITY_QUERIES) {
  const key = `${extra.country}|${extra.query}`;
  if (!seenQuery.has(key)) {
    seenQuery.add(key);
    queries.push(extra);
  }
}

console.log("City catalog queries:", queries.length);

/** @type {Map<string, object>} */
const catalogById = new Map();
let queryCount = 0;
for (const q of queries) {
  queryCount++;
  if (queryCount % 20 === 0) console.log(`  catalog query [${queryCount}/${queries.length}]`);
  const result = await fetchAccorCatalogHotels(q.query, {
    countryCode: q.countryCode || undefined,
    enlargementAllowed: false,
  });
  for (const h of result.hotels || []) {
    if (q.countryCode && h.countryCode && h.countryCode !== q.countryCode) continue;
    if (!q.countryCode && !countriesMatch(h.country, q.country)) continue;
    catalogById.set(h.propertyId, h);
  }
  await sleep(opts.queryDelayMs);
}

const catalogHotels = [...catalogById.values()];
console.log("Unique catalog hotels (deduped):", catalogHotels.length);

const assigned = matchCatalogToCensus(catalogHotels, blankRows, opts.minScore);
console.log("Matched blank census pairs:", assigned.length);

let plan = buildApplyPlan(assigned);

if (opts.fetchAmenities) {
  console.log("\nFetching amenities...\n");
  let n = 0;
  for (const row of plan) {
    if (!row.needsAmenities || row.applyFields.Amenities) continue;
    n++;
    process.stdout.write(` [${n}] ${row.censusName}...`);
    const fetched = await fetchAccorHotelAmenities(row.propertyUrl);
    await sleep(opts.delayMs);
    if (fetched.amenitiesText) {
      row.applyFields.Amenities = fetched.amenitiesText;
      console.log(` ${fetched.amenities.length}`);
    } else console.log(" skip");
  }
}

const withPayload = plan.filter((r) => Object.keys(r.applyFields).length > 0);
mkdirSync(REPORTS, { recursive: true });
const outPath = join(REPORTS, "accor-catalog-gap-pass-plan.json");
writeFileSync(
  outPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      blankCensusRows: blankRows.length,
      queries: queries.length,
      catalogHotels: catalogHotels.length,
      matched: assigned.length,
      ready: withPayload.length,
      plan: withPayload,
    },
    null,
    2
  )
);
console.log("\nReady to apply:", withPayload.length);
console.log("Plan:", outPath);

if (!opts.apply || !withPayload.length) {
  if (!opts.apply) console.log("\nDry-run. Use --apply --fetch-amenities");
  process.exit(0);
}

const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
let updated = 0;
const appliedAt = new Date().toISOString();
for (let i = 0; i < withPayload.length; i += 10) {
  const batch = withPayload
    .slice(i, i + 10)
    .map((r) => ({ id: r.censusRecordId, fields: r.applyFields }));
  await airtable(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
  updated += batch.length;
}

if (!existsSync(LOG_PATH)) {
  appendFileSync(
    LOG_PATH,
    "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,source,amenityCount\n"
  );
}
for (const row of withPayload) {
  const count = row.applyFields.Amenities ? row.applyFields.Amenities.split(";").length : 0;
  appendFileSync(
    LOG_PATH,
    `${appliedAt},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},accor_catalog_gap_pass,${count}\n`
  );
}

console.log("Applied:", updated);
