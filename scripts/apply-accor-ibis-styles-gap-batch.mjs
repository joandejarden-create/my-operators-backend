#!/usr/bin/env node
/**
 * Match blank ibis Styles census rows to continent-browse directory (clean names)
 * and apply Website / Property ID / Amenities (fill-blank only).
 *
 *   node scripts/apply-accor-ibis-styles-gap-batch.mjs
 *   node scripts/apply-accor-ibis-styles-gap-batch.mjs --apply --fetch-amenities --delay-ms=800
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync, appendFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "../lib/hotel-census/match-brand-directory-to-census.js";
import { mapExtractRowToDirectoryMatchRow } from "../lib/hotel-census/plan-brand-census-directory-match.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";
import {
  parseAccorHotelMetadataFromHtml,
  ACCOR_FETCH_HEADERS,
} from "../lib/accor-brand-directory-extract.js";
import { accorCountryCodeIsCala } from "../lib/brand-sitemap/cala-url-segments.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const CONTINENT = join(REPORTS, "accor-continent-directory-extract.json");
const EXTRACT = join(REPORTS, "accor-property-directory-extract.json");
const LOG_PATH = join(REPORTS, "accor-steward-verified-applies.csv");

const CONF_RANK = { high: 3, medium: 2, low: 1, none: 0 };

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 800),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 70),
    minConfidence: args.find((a) => a.startsWith("--min-confidence="))?.split("=")[1] || "medium",
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isIbisStylesRow(name) {
  return /ibis\s*styles/i.test(String(name || ""));
}

function loadDirectoryRows() {
  /** @type {Map<string, object>} */
  const byId = new Map();

  if (existsSync(EXTRACT)) {
    const data = JSON.parse(readFileSync(EXTRACT, "utf8"));
    for (const row of data.propertyRows || []) {
      const id = String(row.propertyId || "").toUpperCase();
      if (!id) continue;
      byId.set(id, row);
    }
  }

  if (existsSync(CONTINENT)) {
    const data = JSON.parse(readFileSync(CONTINENT, "utf8"));
    for (const row of data.propertyRows || []) {
      const id = String(row.propertyId || "").toUpperCase();
      if (!id) continue;
      const prev = byId.get(id);
      const browseName = String(row.inferredHotelName || "");
      const seoLike = /^(hotel|budget|cheap|cool|great|excellent|affordable)/i.test(
        String(prev?.inferredHotelName || "")
      );
      if (!prev || (browseName && (!prev.inferredHotelName || seoLike))) {
        byId.set(id, { ...prev, ...row, inferredHotelName: browseName || prev?.inferredHotelName });
      }
    }
  }

  return [...byId.values()].filter(
    (r) =>
      isIbisStylesRow(r.inferredHotelName) &&
      (r.calaFilterStatus === "included" || r.calaRelevant)
  );
}

async function hydrateRow(row) {
  const id = String(row.propertyId || "").toUpperCase();
  if (!id) return row;
  if (row.amenitiesText && row.latitude != null) return row;

  const url = accorCanonicalPropertyUrl(id);
  try {
    const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
    if (!res.ok) return row;
    const meta = parseAccorHotelMetadataFromHtml(await res.text());
    if (!meta) return row;
    if (meta.countryCode && !accorCountryCodeIsCala(meta.countryCode)) return null;
    return {
      ...row,
      propertyUrl: url,
      inferredHotelName: row.inferredHotelName || meta.name,
      city: meta.city || row.city,
      country: meta.country || row.country,
      latitude: meta.latitude,
      longitude: meta.longitude,
      amenitiesText: meta.amenitiesText || row.amenitiesText || "",
      calaFilterStatus: "included",
    };
  } catch {
    return row;
  }
}

const opts = parseArgs();
const minRank = CONF_RANK[opts.minConfidence] ?? 2;

const base = getPlatformBase();
const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", "Amenities", "Property ID", CENSUS_FIELDS.city, CENSUS_FIELDS.country],
    filterByFormula: `AND(FIND("Accor", {${CENSUS_FIELDS.parentCompany}}), FIND("ibis Styles", {name}))`,
    pageSize: 100,
  })
  .all();

const blankCensus = records.filter((r) => isBlankCensusValue(r.fields?.Amenities));
console.log("=== ibis Styles gap batch ===\n");
console.log("Blank ibis Styles census rows:", blankCensus.length);

let directoryRows = loadDirectoryRows();
console.log("Directory ibis Styles rows (browse-enriched):", directoryRows.length);

const dirMatches = directoryRows.map((d) => ({
  raw: d,
  match: mapExtractRowToDirectoryMatchRow(d, { scoringProfile: "accor" }),
}));

/** @type {object[]} */
const pairs = [];
for (const rec of blankCensus) {
  const censusRow = mapCensusRowForDirectoryMatch(rec);
  for (const { raw, match } of dirMatches) {
    const scored = scoreDirectoryAgainstCensus(match, censusRow);
    if (scored.score < opts.minScore) continue;
    const rank = CONF_RANK[scored.confidence] ?? 0;
    if (rank < minRank) continue;
    pairs.push({
      censusRecordId: rec.id,
      censusName: rec.fields?.name,
      censusRow,
      raw,
      scored,
      score: scored.score,
    });
  }
}

pairs.sort((a, b) => b.score - a.score);
const usedCensus = new Set();
const usedDir = new Set();
/** @type {typeof pairs} */
const assigned = [];
for (const p of pairs) {
  const dirKey = String(p.raw.propertyId || "").toUpperCase();
  if (usedCensus.has(p.censusRecordId) || usedDir.has(dirKey)) continue;
  usedCensus.add(p.censusRecordId);
  usedDir.add(dirKey);
  assigned.push(p);
}

console.log("Matched pairs (medium+, score>=" + opts.minScore + "):", assigned.length);

/** @type {object[]} */
const plan = [];
let hydrateN = 0;
for (const row of assigned) {
  const f = row.censusRow.fields || {};
  hydrateN++;
  if (hydrateN % 5 === 0) console.log(`  hydrate metadata [${hydrateN}/${assigned.length}]`);
  const hydrated = await hydrateRow(row.raw);
  if (!hydrated) continue;
  await sleep(60);

  const propertyId = String(hydrated.propertyId || "").toUpperCase();
  const url = accorCanonicalPropertyUrl(propertyId) || hydrated.propertyUrl;
  const applyFields = {};

  if (isBlankCensusValue(f.Website) && url) applyFields.Website = url;
  if (isBlankCensusValue(f["Property ID"]) && propertyId) applyFields["Property ID"] = propertyId;
  if (isBlankCensusValue(f.Amenities) && hydrated.amenitiesText) {
    applyFields.Amenities = hydrated.amenitiesText;
  }

  plan.push({
    censusRecordId: row.censusRecordId,
    censusName: row.censusName,
    propertyId,
    propertyUrl: url,
    matchScore: row.score,
    matchConfidence: row.scored.confidence,
    directoryName: hydrated.inferredHotelName,
    applyFields,
    needsAmenityFetch: isBlankCensusValue(f.Amenities) && !applyFields.Amenities,
  });
}

if (opts.fetchAmenities) {
  console.log("\nFetching missing amenities...\n");
  let n = 0;
  for (const row of plan) {
    if (!row.needsAmenityFetch) continue;
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
const outPath = join(REPORTS, "accor-ibis-styles-gap-batch-plan.json");
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
    `${appliedAt},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},ibis_styles_gap_batch,${count}\n`
  );
}

console.log("Applied:", updated);
