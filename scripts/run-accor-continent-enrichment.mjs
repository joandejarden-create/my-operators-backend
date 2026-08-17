#!/usr/bin/env node
/**
 * Merge continent browse + existing extract, hydrate new codes, match + apply.
 *
 *   node scripts/run-accor-continent-enrichment.mjs
 *   node scripts/run-accor-continent-enrichment.mjs --apply --fetch-amenities --delay-ms=1000
 */
import "../load-env.js";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { extractAccorContinentHotels } from "../lib/accor-continent-directory-extract.js";
import {
  parseAccorHotelMetadataFromHtml,
  ACCOR_FETCH_HEADERS,
} from "../lib/accor-brand-directory-extract.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";
import { normalizeCountry } from "../lib/independent-census/match-current-census.js";
import { accorCountryCodeIsCala } from "../lib/brand-sitemap/cala-url-segments.js";
import { planBrandCensusDirectoryMatch } from "../lib/hotel-census/plan-brand-census-directory-match.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT = join(REPORTS, "accor-property-directory-extract.json");
const CONTINENT = join(REPORTS, "accor-continent-directory-extract.json");

const apply = process.argv.includes("--apply");
const fetchAmenities = process.argv.includes("--fetch-amenities");
const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 1000);
const hydrateDelay = Number(process.argv.find((a) => a.startsWith("--hydrate-delay-ms="))?.split("=")[1] || 80);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function loadExistingRows() {
  if (!existsSync(EXTRACT)) return [];
  const data = JSON.parse(readFileSync(EXTRACT, "utf8"));
  return Array.isArray(data.propertyRows) ? data.propertyRows : [];
}

function mergeDirectoryRows(existing, continentRows) {
  /** @type {Map<string, object>} */
  const byId = new Map();
  for (const row of existing) {
    const id = String(row.propertyId || "").toUpperCase();
    if (id) byId.set(id, row);
  }
  for (const row of continentRows) {
    const id = String(row.propertyId || "").toUpperCase();
    if (!id) continue;
    const prev = byId.get(id);
    if (!prev) {
      byId.set(id, {
        ...row,
        calaFilterStatus: row.calaFilterStatus || "uncertain",
      });
      continue;
    }
    // Prefer continent browse name when existing name is empty or SEO-like
    const seoLike = /^(hotel|resort|economy|well|cozy|comfortable|optimal)/i.test(
      String(prev.inferredHotelName || "")
    );
    if ((!prev.inferredHotelName || seoLike) && row.inferredHotelName) {
      prev.inferredHotelName = row.inferredHotelName;
    }
    if (!prev.city && row.city) prev.city = row.city;
    if (!prev.country && row.country) prev.country = row.country;
    byId.set(id, prev);
  }
  return [...byId.values()];
}

async function hydrateMissingMetadata(rows) {
  /** @type {object[]} */
  const out = [];
  let n = 0;
  for (const row of rows) {
    const id = String(row.propertyId || "").toUpperCase();
    if (!id) continue;
    if (
      row.calaFilterStatus === "included" &&
      row.latitude != null &&
      row.longitude != null &&
      row.country
    ) {
      out.push(row);
      continue;
    }

    n++;
    if (n % 25 === 0) console.log(`  hydrate [${n}]`);
    const url = accorCanonicalPropertyUrl(id);
    try {
      const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
      if (!res.ok) {
        out.push(row);
        continue;
      }
      const meta = parseAccorHotelMetadataFromHtml(await res.text());
      if (!meta?.name) {
        out.push(row);
        continue;
      }
      const isCala = accorCountryCodeIsCala(meta.countryCode);
      if (!isCala) continue;
      out.push({
        ...row,
        propertyUrl: url,
        inferredHotelName: row.inferredHotelName || meta.name,
        city: meta.city || row.city,
        country: meta.country || row.country,
        countryCode: meta.countryCode,
        countryNorm: normalizeCountry(meta.country),
        latitude: meta.latitude,
        longitude: meta.longitude,
        amenities: meta.amenities || [],
        amenitiesText: meta.amenitiesText || "",
        calaFilterStatus: "included",
      });
    } catch {
      out.push(row);
    }
    if (hydrateDelay > 0) await sleep(hydrateDelay);
  }
  return out.filter((r) => r.calaFilterStatus === "included");
}

console.log("=== Accor continent enrichment ===\n");

let continentRows = [];
if (existsSync(CONTINENT)) {
  const saved = JSON.parse(readFileSync(CONTINENT, "utf8"));
  continentRows = saved.propertyRows || [];
  console.log("Loaded continent extract:", continentRows.length);
} else {
  console.log("Crawling continent browse pages...");
  const crawled = await extractAccorContinentHotels({ delayMs: 120 });
  continentRows = crawled.propertyRows;
  writeFileSync(CONTINENT, JSON.stringify({ generatedAt: new Date().toISOString(), ...crawled }, null, 2));
  console.log("Continent codes:", continentRows.length);
}

const existing = loadExistingRows();
console.log("Existing extract rows:", existing.length);
const merged = mergeDirectoryRows(existing, continentRows);
console.log("Merged directory rows:", merged.length);

const newCodes = merged.filter((r) => {
  const id = String(r.propertyId || "").toUpperCase();
  return id && !existing.some((e) => String(e.propertyId || "").toUpperCase() === id);
});
console.log("New codes from continent merge:", newCodes.length);

console.log("\nHydrating metadata for rows missing geo...");
const hydrated = await hydrateMissingMetadata(merged);
console.log("CALA rows after hydrate:", hydrated.length);

mkdirSync(REPORTS, { recursive: true });
writeFileSync(
  EXTRACT,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      mergedFromContinent: true,
      propertyRows: hydrated,
      summary: { totalPropertyUrls: hydrated.length, calaIncluded: hydrated.length },
    },
    null,
    2
  )
);

const plan = await planBrandCensusDirectoryMatch({
  parentFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
  directoryRows: hydrated,
  minScore: 50,
  requireCountryMatch: true,
  minApplyConfidence: "medium",
  includeAmenitiesFromCache: true,
  scoringProfile: "accor",
});

const applyRows = [...plan.planRows];
console.log("\nAuto-apply ready:", applyRows.length);
console.log("Steward (low):", plan.stewardReviewCount);
console.log("Skipped:", plan.skippedCount);

if (fetchAmenities) {
  console.log("\nFetching amenities for apply rows missing cache...");
  let n = 0;
  for (const row of applyRows) {
    if (!isBlankCensusValue(row.applyFields?.Amenities)) continue;
    const url = row.propertyUrl || accorCanonicalPropertyUrl(row.propertyId);
    if (!url) continue;
    n++;
    const fetched = await fetchAccorHotelAmenities(url);
    await sleep(delayMs);
    if (fetched.amenitiesText) row.applyFields.Amenities = fetched.amenitiesText;
  }
  console.log("Amenity fetch pass done:", n);
}

const outPlan = join(REPORTS, "accor-continent-enrichment-plan.json");
writeFileSync(outPlan, JSON.stringify({ generatedAt: new Date().toISOString(), ready: applyRows.length, plan: applyRows }, null, 2));
console.log("Plan:", outPlan);

if (!apply || !applyRows.length) {
  console.log(apply ? "\nNothing to apply." : "\nDry-run. Use --apply [--fetch-amenities]");
  process.exit(0);
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
let updated = 0;
for (let i = 0; i < applyRows.length; i += 10) {
  const batch = applyRows
    .slice(i, i + 10)
    .filter((r) => Object.keys(r.applyFields || {}).length)
    .map((r) => ({ id: r.censusRecordId, fields: r.applyFields }));
  if (!batch.length) continue;
  await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
  updated += batch.length;
}
console.log("Applied:", updated);
