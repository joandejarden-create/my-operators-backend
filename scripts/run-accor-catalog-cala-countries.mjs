#!/usr/bin/env node
/**
 * Accor Catalog API — country-by-country CALA enrichment (viable alternative to city pulls).
 *
 *   node scripts/run-accor-catalog-cala-countries.mjs
 *   node scripts/run-accor-catalog-cala-countries.mjs --apply --fetch-amenities
 *   node scripts/run-accor-catalog-cala-countries.mjs --countries Brazil,Colombia --apply
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync, appendFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  fetchAccorCatalogHotels,
  accorCountryNameToCode,
} from "../lib/accor-catalog-api.js";
import { ACCOR_COUNTRY_CODE_TO_LABEL } from "../lib/brand-sitemap/cala-url-segments.js";
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
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const LOG_PATH = join(REPORTS, "accor-steward-verified-applies.csv");

const CATALOG_CSV_COLUMNS = [
  "propertyId",
  "name",
  "brand",
  "city",
  "country",
  "address1",
  "postalCode",
  "telephone",
  "email",
  "latitude",
  "longitude",
  "propertyUrl",
];

function parseArgs() {
  const args = process.argv.slice(2);
  const countriesArg = args.find((a) => a.startsWith("--countries="))?.split("=")[1];
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 600),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 70),
    countries: countriesArg
      ? countriesArg.split(",").map((s) => s.trim().toLowerCase())
      : null,
    skipZero: !args.includes("--include-zero"),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * @param {string} countryLabel
 * @param {string} countryCode
 * @param {object[]} hotels
 */
function exportCountryCatalog(countryLabel, countryCode, hotels) {
  const slug = slugify(countryLabel);
  const rows = hotels.map((h) => ({
    propertyId: h.propertyId,
    name: h.name,
    brand: h.brand,
    city: h.city,
    country: h.country,
    address1: h.address1,
    postalCode: h.postalCode,
    telephone: h.telephone,
    email: h.email,
    latitude: h.latitude,
    longitude: h.longitude,
    propertyUrl: h.propertyUrl,
  }));
  const jsonPath = join(REPORTS, `accor-catalog-country-${slug}.json`);
  const csvPath = join(REPORTS, `accor-catalog-country-${slug}.csv`);
  writeFileSync(
    jsonPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        country: countryLabel,
        countryCode,
        count: rows.length,
        catalogCapNote:
          countryCode === "BR" && rows.length >= 300
            ? "Catalog API returns max ~300 hotels per country query; supplement with city/continent browse for Brazil stragglers."
            : null,
        hotels: rows,
      },
      null,
      2
    )
  );
  if (rows.length) writeCsv(csvPath, rows, CATALOG_CSV_COLUMNS);
  return { jsonPath, csvPath };
}

/**
 * @param {object[]} catalogHotels
 * @param {object[]} censusRows
 * @param {number} minScore
 */
function matchCatalogToCensus(catalogHotels, censusRows, minScore) {
  /** @type {object[]} */
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
      const scored = scoreDirectoryAgainstCensus(dirMatch, censusRow);
      if (scored.score < minScore) continue;
      if (scored.confidence === "none") continue;
      pairs.push({ cat, censusRow, scored, score: scored.score });
    }
  }

  pairs.sort((a, b) => b.score - a.score);
  const usedCensus = new Set();
  const usedCat = new Set();
  /** @type {typeof pairs} */
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
  /** @type {object[]} */
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
    if (isBlankCensusValue(f.Telephone) && cat.telephone) {
      applyFields.Telephone = cat.telephone;
    }
    if (isBlankCensusValue(f["Address 1"]) && cat.address1) {
      applyFields["Address 1"] = cat.address1;
    }
    if (isBlankCensusValue(f["Postal Code"]) && cat.postalCode) {
      applyFields["Postal Code"] = cat.postalCode;
    }
    if (isBlankCensusValue(f.Latitude) && cat.latitude != null) {
      applyFields.Latitude = cat.latitude;
    }
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
mkdirSync(REPORTS, { recursive: true });

/** @type {{ countryCode: string, country: string }[]} */
let countries = Object.entries(ACCOR_COUNTRY_CODE_TO_LABEL).map(([countryCode, country]) => ({
  countryCode,
  country,
}));

if (opts.countries) {
  const want = new Set(opts.countries);
  countries = countries.filter(
    (c) => want.has(c.country.toLowerCase()) || want.has(c.countryCode.toLowerCase())
  );
}

console.log("=== Accor CALA catalog — country-by-country ===\n");
console.log("Countries:", countries.length);
console.log("Apply:", opts.apply, "| Fetch amenities:", opts.fetchAmenities);
console.log("");

const base = getPlatformBase();
const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

/** @type {object[]} */
const summary = [];
let totalCatalog = 0;
let totalMatched = 0;
let totalApplied = 0;
let totalAmenityFetches = 0;

for (const { countryCode, country } of countries) {
  console.log(`\n--- ${country} (${countryCode}) ---`);

  const catalog = await fetchAccorCatalogHotels(country, {
    countryCode,
    enlargementAllowed: false,
  });
  await sleep(200);

  console.log("Catalog hotels:", catalog.count);
  totalCatalog += catalog.count;

  if (!catalog.count && opts.skipZero) {
    summary.push({
      country,
      countryCode,
      catalogCount: 0,
      matched: 0,
      applied: 0,
      skipped: "zero_catalog_results",
    });
    continue;
  }

  const paths = exportCountryCatalog(country, countryCode, catalog.hotels);
  console.log("Export:", paths.csvPath || paths.jsonPath);

  const records = await base(HOTEL_CENSUS_TABLE)
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
      filterByFormula: `AND(FIND("Accor", {${CENSUS_FIELDS.parentCompany}}), {${CENSUS_FIELDS.country}} = "${country.replace(/"/g, '\\"')}")`,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  console.log("Accor census rows:", censusRows.length);

  const assigned = matchCatalogToCensus(catalog.hotels, censusRows, opts.minScore);
  console.log("Matched pairs:", assigned.length);
  totalMatched += assigned.length;

  let plan = buildApplyPlan(assigned);

  if (opts.fetchAmenities && plan.length) {
    let n = 0;
    for (const row of plan) {
      if (!row.needsAmenities || row.applyFields.Amenities) continue;
      n++;
      totalAmenityFetches++;
      process.stdout.write(`  amenity [${n}] ${row.censusName}...`);
      const fetched = await fetchAccorHotelAmenities(row.propertyUrl);
      await sleep(opts.delayMs);
      if (fetched.amenitiesText) {
        row.applyFields.Amenities = fetched.amenitiesText;
        console.log(` ${fetched.amenities.length}`);
      } else {
        console.log(" skip");
      }
    }
  }

  const withPayload = plan.filter((r) => Object.keys(r.applyFields).length > 0);
  console.log("Ready to apply:", withPayload.length);

  let applied = 0;
  if (opts.apply && withPayload.length) {
    for (let i = 0; i < withPayload.length; i += 10) {
      const batch = withPayload
        .slice(i, i + 10)
        .map((r) => ({ id: r.censusRecordId, fields: r.applyFields }));
      await airtable(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      applied += batch.length;
    }
    totalApplied += applied;

    if (!existsSync(LOG_PATH)) {
      appendFileSync(
        LOG_PATH,
        "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,source,amenityCount\n"
      );
    }
    const appliedAt = new Date().toISOString();
    for (const row of withPayload) {
      const count = row.applyFields.Amenities
        ? row.applyFields.Amenities.split(";").length
        : 0;
      appendFileSync(
        LOG_PATH,
        `${appliedAt},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},accor_catalog_country,${count}\n`
      );
    }
  }

  summary.push({
    country,
    countryCode,
    catalogCount: catalog.count,
    censusAccorRows: censusRows.length,
    matched: assigned.length,
    readyToApply: withPayload.length,
    applied,
    exportJson: paths.jsonPath,
    exportCsv: paths.csvPath,
  });
}

const summaryPath = join(REPORTS, "accor-catalog-cala-countries-summary.json");
writeFileSync(
  summaryPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      mode: "country_by_country",
      totalCatalogHotels: totalCatalog,
      totalMatchedPairs: totalMatched,
      totalApplied,
      totalAmenityFetches,
      catalogApiCapPerCountry: 300,
      countriesWithZeroResults: summary.filter((s) => s.catalogCount === 0).map((s) => s.country),
      countries: summary,
    },
    null,
    2
  )
);

console.log("\n=== Summary ===");
console.log("Catalog hotels (sum):", totalCatalog);
console.log("Matched pairs:", totalMatched);
console.log("Applied:", totalApplied);
console.log("Summary:", summaryPath);
