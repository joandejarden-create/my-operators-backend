#!/usr/bin/env node
/**
 * Open-status gap pass: catalog city + brand + radius + Brazil bbox for Accor amenity blanks.
 *
 *   node scripts/run-accor-catalog-open-gap-pass.mjs
 *   node scripts/run-accor-catalog-open-gap-pass.mjs --apply --fetch-amenities
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync, appendFileSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  BRAZIL_CATALOG_BBOXES,
  accorCountryNameToCode,
  fetchAccorCatalogByBbox,
  fetchAccorCatalogByRadius,
  fetchAccorCatalogHotels,
} from "../lib/accor-catalog-api.js";
import { extractAccorContinentHotels } from "../lib/accor-continent-directory-extract.js";
import {
  accorCatalogBrandCodeFromCensusName,
  accorDirectoryRowToCatalogHotel,
  buildAccorCatalogApplyPlan,
  mapCensusRowForDirectoryMatch,
  matchAccorCatalogToCensus,
} from "../lib/accor-catalog-gap-match.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE, STATUS_OPEN } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { countriesMatch } from "../lib/independent-census/match-current-census.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT = join(REPORTS, "accor-property-directory-extract.json");
const CONTINENT = join(REPORTS, "accor-continent-directory-extract.json");

const LOG_PATH = join(REPORTS, "accor-steward-verified-applies.csv");

function loadMergedDirectoryCatalogHotels() {
  /** @type {Map<string, object>} */
  const byId = new Map();

  function mergeRow(id, row) {
    const prev = byId.get(id);
    if (!prev) {
      byId.set(id, { ...row });
      return;
    }
    const seoLike = (name) =>
      /^(hotel|resort|economy|an affordable|a modern|low cost|well[- ]located|comfortable)/i.test(
        String(name || "")
      );
    const merged = { ...prev, ...row };
    if (row.city && !prev.city) merged.city = row.city;
    if (row.country && !prev.country) merged.country = row.country;
    if (row.countryCode && !prev.countryCode) merged.countryCode = row.countryCode;
    if (row.latitude != null && prev.latitude == null) merged.latitude = row.latitude;
    if (row.longitude != null && prev.longitude == null) merged.longitude = row.longitude;
    if (row.amenitiesText && !prev.amenitiesText) merged.amenitiesText = row.amenitiesText;
    if (row.inferredHotelName) {
      if (!prev.inferredHotelName || (seoLike(prev.inferredHotelName) && !seoLike(row.inferredHotelName))) {
        merged.inferredHotelName = row.inferredHotelName;
      }
    }
    byId.set(id, merged);
  }

  if (existsSync(EXTRACT)) {
    const data = JSON.parse(readFileSync(EXTRACT, "utf8"));
    for (const row of data.propertyRows || []) {
      const id = String(row.propertyId || "").toUpperCase();
      if (id) mergeRow(id, row);
    }
  }
  if (existsSync(CONTINENT)) {
    const data = JSON.parse(readFileSync(CONTINENT, "utf8"));
    for (const row of data.propertyRows || []) {
      const id = String(row.propertyId || "").toUpperCase();
      if (id) mergeRow(id, row);
    }
  }

  return [...byId.values()].map(accorDirectoryRowToCatalogHotel);
}

function censusNameQuery(name) {
  const stop = new Set(["hotel", "the", "by", "future", "collection", "all", "inclusive", "adults", "only"]);
  const words = String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 2 && !stop.has(w));
  return words.slice(0, 5).join(" ");
}

const EXTRA_COUNTRY_CODES = {
  "french guiana": "GF",
  guadeloupe: "GP",
  martinique: "MQ",
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 500),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 55),
    queryDelayMs: Number(args.find((a) => a.startsWith("--query-delay-ms="))?.split("=")[1] || 180),
    radiusKm: Number(args.find((a) => a.startsWith("--radius-km="))?.split("=")[1] || 20),
    skipBbox: args.includes("--skip-bbox"),
    refreshContinent: args.includes("--refresh-continent"),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function countryToCode(countryName) {
  const key = String(countryName || "").trim().toLowerCase();
  return EXTRA_COUNTRY_CODES[key] || accorCountryNameToCode(countryName);
}

function isOpenStatus(statusRaw) {
  const arr = Array.isArray(statusRaw) ? statusRaw : [statusRaw];
  return arr.some((x) => String(x || "").toLowerCase() === STATUS_OPEN.toLowerCase());
}

function cityQuerySlug(city) {
  return String(city || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/**
 * @param {Map<string, object>} catalogById
 * @param {Map<string, { enlarged?: boolean }>} queryMeta
 * @param {object[]} hotels
 * @param {{ enlarged?: boolean }} meta
 */
function mergeCatalogHotels(catalogById, queryMeta, hotels, meta = {}) {
  for (const h of hotels || []) {
    if (!h?.propertyId) continue;
    catalogById.set(h.propertyId, h);
    if (!queryMeta.has(h.propertyId)) {
      queryMeta.set(h.propertyId, meta);
    }
  }
}

const opts = parseArgs();
console.log("=== Accor catalog OPEN gap pass ===\n");

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
      "status",
      CENSUS_FIELDS.city,
      CENSUS_FIELDS.country,
    ],
    filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

const targetRecords = allAccor.filter(
  (r) => isBlankCensusValue(r.fields?.Amenities) && isOpenStatus(r.fields?.status)
);
const targetRows = targetRecords.map(mapCensusRowForDirectoryMatch);
console.log("Open amenity blanks:", targetRows.length);

/** @type {Map<string, object>} */
const catalogById = new Map();
/** @type {Map<string, { enlarged?: boolean }>} */
const queryMeta = new Map();

let queryCount = 0;

async function runQuery(label, fetchFn) {
  queryCount++;
  if (queryCount % 15 === 0) console.log(`  query [${queryCount}] ${label}`);
  const result = await fetchFn();
  if (!result.ok) {
    console.warn(`  warn: ${label} -> ${result.error}`);
    return;
  }
  mergeCatalogHotels(catalogById, queryMeta, result.hotels, {
    enlarged: Boolean(result.autoEnlarged),
  });
  await sleep(opts.queryDelayMs);
}

if (opts.refreshContinent) {
  console.log("Refreshing continent browse extract...");
  const crawled = await extractAccorContinentHotels({ delayMs: 120 });
  writeFileSync(
    CONTINENT,
    JSON.stringify({ generatedAt: new Date().toISOString(), ...crawled }, null, 2)
  );
  console.log("Continent codes:", crawled.propertyRows?.length || 0);
}

const directoryHotels = loadMergedDirectoryCatalogHotels();
console.log("Directory extract hotels merged:", directoryHotels.length);
mergeCatalogHotels(catalogById, queryMeta, directoryHotels, { enlarged: false });

const seenCity = new Set();
for (const rec of targetRecords) {
  const country = String(rec.fields?.country || "").trim();
  const city = String(rec.fields?.city || "").trim();
  const countryCode = countryToCode(country);
  const query = cityQuerySlug(city);
  if (!query || query.length < 3) continue;

  const cityKey = `${country}|${query}`;
  if (!seenCity.has(cityKey)) {
    seenCity.add(cityKey);
    await runQuery(`city:${query}`, () =>
      fetchAccorCatalogHotels(query, {
        countryCode: countryCode || undefined,
        enlargementAllowed: false,
      })
    );
  }

  const brand = accorCatalogBrandCodeFromCensusName(rec.fields?.name);
  if (brand) {
    const brandKey = `${cityKey}|${brand}`;
    if (!seenCity.has(brandKey)) {
      seenCity.add(brandKey);
      await runQuery(`city+brand:${query}/${brand}`, () =>
        fetchAccorCatalogHotels(query, {
          countryCode: countryCode || undefined,
          brand,
          enlargementAllowed: false,
        })
      );
    }
  }

  const lat = Number(rec.fields?.Latitude);
  const lng = Number(rec.fields?.Longitude);
  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    const geoKey = `geo:${lat.toFixed(3)},${lng.toFixed(3)}`;
    if (!seenCity.has(geoKey)) {
      seenCity.add(geoKey);
      await runQuery(`radius:${geoKey}`, () =>
        fetchAccorCatalogByRadius(lat, lng, opts.radiusKm, {
          countryCode: countryCode || undefined,
        })
      );
    }
  }

  const nameQuery = censusNameQuery(rec.fields?.name);
  if (nameQuery && nameQuery.length >= 8) {
    const nameKey = `name:${nameQuery}`;
    if (!seenCity.has(nameKey)) {
      seenCity.add(nameKey);
      await runQuery(`name:${nameQuery}`, () =>
        fetchAccorCatalogHotels(nameQuery, {
          countryCode: countryCode || undefined,
          enlargementAllowed: false,
        })
      );
    }
  }
}

const needsBrazilBbox = targetRecords.some(
  (r) => String(r.fields?.country || "").toLowerCase() === "brazil"
);
if (needsBrazilBbox && !opts.skipBbox) {
  console.log("\nBrazil bbox regions:", BRAZIL_CATALOG_BBOXES.length);
  for (const box of BRAZIL_CATALOG_BBOXES) {
    await runQuery(`bbox:${box.label}`, () =>
      fetchAccorCatalogByBbox(box.boxBottomLeft, box.boxTopRight, { countryCode: "BR" })
    );
  }
}

const catalogHotels = [...catalogById.values()];
console.log("\nCatalog queries run:", queryCount);
console.log("Unique catalog hotels:", catalogHotels.length);

const assigned = matchAccorCatalogToCensus(catalogHotels, targetRows, opts.minScore, {
  queryMeta,
  allowLowConfidence: true,
});
console.log("Matched open blank pairs:", assigned.length);

let plan = buildAccorCatalogApplyPlan(assigned);

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
    } else {
      console.log(" skip");
    }
  }
}

const withPayload = plan.filter((r) => Object.keys(r.applyFields).length > 0);
mkdirSync(REPORTS, { recursive: true });
const outPath = join(REPORTS, "accor-catalog-open-gap-pass-plan.json");
writeFileSync(
  outPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      openBlankRows: targetRows.length,
      catalogQueries: queryCount,
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
if (withPayload.length) {
  for (const row of withPayload.slice(0, 12)) {
    console.log(
      `  ${row.censusName} -> ${row.propertyId} (${row.catalogName}) score=${row.matchScore}`
    );
  }
  if (withPayload.length > 12) console.log(`  ... +${withPayload.length - 12} more`);
}

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
    `${appliedAt},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},accor_catalog_open_gap_pass,${count}\n`
  );
}

console.log("Applied:", updated);
