#!/usr/bin/env node
/**
 * Discover Accor hotels via Catalog API (destination query) and apply fill-blank
 * census fields: Website, Property ID, Amenities, Telephone, Address 1, Postal Code, lat/lng.
 *
 * Replaces un-scrapable booking city search pages — same data the booking funnel uses.
 *
 *   node scripts/apply-accor-catalog-destination-batch.mjs --destination bogota
 *   node scripts/apply-accor-catalog-destination-batch.mjs --destination lavras --apply --fetch-amenities
 *   node scripts/apply-accor-catalog-destination-batch.mjs --booking-url "https://all.accor.com/booking/en/ibis/hotel/B544?..."
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync, appendFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { fetchAccorCatalogHotels, accorCountryNameToCode } from "../lib/accor-catalog-api.js";
import { accorPropertyIdFromBookingUrl } from "../lib/accor-booking-url.js";
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const LOG_PATH = join(REPORTS, "accor-steward-verified-applies.csv");

function parseArgs() {
  const args = process.argv.slice(2);
  const destIdx = args.indexOf("--destination");
  const urlIdx = args.indexOf("--booking-url");
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 700),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 70),
    destination: destIdx >= 0 ? args[destIdx + 1] : "",
    bookingUrls: urlIdx >= 0 ? [args[urlIdx + 1]] : [],
    country: args.find((a) => a.startsWith("--country="))?.split("=")[1] || "",
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} code
 */
async function catalogHotelById(code) {
  const id = String(code || "").toUpperCase();
  if (!id) return null;
  // Catalog accepts hotel id as q for some codes
  for (const q of [id, id.toLowerCase()]) {
    const res = await fetchAccorCatalogHotels(q, { enlargementAllowed: true });
    const hit = res.hotels.find((h) => h.propertyId === id);
    if (hit) return hit;
  }
  return {
    propertyId: id,
    name: "",
    propertyUrl: accorCanonicalPropertyUrl(id),
    source: "booking_url_code",
  };
}

const opts = parseArgs();
const destinations = [];
if (opts.destination) destinations.push(opts.destination);
for (const url of opts.bookingUrls) {
  const code = accorPropertyIdFromBookingUrl(url);
  if (code) destinations.push(code);
}

// Also accept extra booking URLs from CLI repeated flags
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith("http") && arg.includes("accor.com/booking")) {
    const code = accorPropertyIdFromBookingUrl(arg);
    if (code) destinations.push(code);
  }
}

if (!destinations.length) {
  console.error("Usage: --destination bogota | --booking-url <url> [--apply --fetch-amenities]");
  process.exit(1);
}

console.log("=== Accor catalog destination batch ===\n");

/** @type {Map<string, object>} */
const catalogById = new Map();
function looksLikePropertyCode(token) {
  const s = String(token || "").trim().toUpperCase();
  if (!/^[0-9A-Z]{3,6}$/.test(s)) return false;
  // Avoid treating city names (e.g. BOGOTA) as hotel codes — Accor IDs include a digit.
  return /\d/.test(s);
}

for (const dest of [...new Set(destinations)]) {
  const isCode = looksLikePropertyCode(dest);
  const result = isCode
    ? { hotels: [await catalogHotelById(dest)].filter(Boolean) }
    : await fetchAccorCatalogHotels(dest, {
        countryCode: accorCountryNameToCode(opts.country) || undefined,
      });
  console.log(
    isCode ? `Code ${dest.toUpperCase()}:` : `Destination "${dest}":`,
    result.hotels?.length ?? 0,
    "hotels"
  );
  for (const h of result.hotels || []) {
    catalogById.set(h.propertyId, h);
  }
  if (!isCode) await sleep(200);
}

const catalogHotels = [...catalogById.values()];
console.log("Unique catalog hotels:", catalogHotels.length);

const base = getPlatformBase();
let formula = `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`;
if (opts.country) {
  formula = `AND(${formula}, {${CENSUS_FIELDS.country}} = "${opts.country.replace(/"/g, '\\"')}")`;
}

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
    filterByFormula: formula,
    pageSize: 100,
  })
  .all();

const censusRows = records.map(mapCensusRowForDirectoryMatch);

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
    if (scored.score < opts.minScore) continue;
    if (scored.confidence === "none") continue;
    pairs.push({ cat, censusRow, scored, score: scored.score });
  }
}

pairs.sort((a, b) => b.score - a.score);
const usedCensus = new Set();
const usedCat = new Set();
/** @type {object[]} */
const assigned = [];
for (const p of pairs) {
  const id = p.cat.propertyId;
  if (usedCensus.has(p.censusRow.recordId) || usedCat.has(id)) continue;
  usedCensus.add(p.censusRow.recordId);
  usedCat.add(id);
  assigned.push(p);
}

console.log("Matched census pairs:", assigned.length);

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
    catalogContact: {
      telephone: cat.telephone,
      address1: cat.address1,
      postalCode: cat.postalCode,
    },
  });
}

if (opts.fetchAmenities) {
  console.log("\nFetching amenities from canonical pages...\n");
  let n = 0;
  for (const row of plan) {
    if (!row.needsAmenities || row.applyFields.Amenities) continue;
    n++;
    process.stdout.write(` [${n}] ${row.censusName}...`);
    const fetched = await fetchAccorHotelAmenities(row.propertyUrl);
    await sleep(opts.delayMs);
    if (fetched.amenitiesText) {
      row.applyFields.Amenities = fetched.amenitiesText;
      console.log(` ok (${fetched.amenities.length})`);
    } else {
      console.log(" skip");
    }
  }
}

const withPayload = plan.filter((r) => Object.keys(r.applyFields).length > 0);
mkdirSync(REPORTS, { recursive: true });
const outPath = join(REPORTS, "accor-catalog-destination-batch-plan.json");
writeFileSync(
  outPath,
  JSON.stringify({ generatedAt: new Date().toISOString(), ready: withPayload.length, plan: withPayload }, null, 2)
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
  const count = row.applyFields.Amenities
    ? row.applyFields.Amenities.split(";").length
    : 0;
  appendFileSync(
    LOG_PATH,
    `${appliedAt},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},accor_catalog_api,${count}\n`
  );
}

console.log("Applied:", updated);
