#!/usr/bin/env node
/**
 * Apply fill-blank Website / Property ID / Amenities for high-confidence ibis Styles
 * steward matches from the saved Accor expansion plan.
 *
 *   node scripts/apply-accor-ibis-styles-batch.mjs
 *   node scripts/apply-accor-ibis-styles-batch.mjs --apply --fetch-amenities --delay-ms=800
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync, appendFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const PLAN_PATH = join(REPORTS, "accor-census-match-expansion-plan.json");
const LOG_PATH = join(REPORTS, "accor-steward-verified-applies.csv");

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 800),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 85),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isIbisStylesName(name) {
  return /ibis\s*styles/i.test(String(name || ""));
}

const opts = parseArgs();

if (!existsSync(PLAN_PATH)) {
  throw new Error(`Missing ${PLAN_PATH}. Run match expansion first.`);
}

const saved = JSON.parse(readFileSync(PLAN_PATH, "utf8"));
const candidates = [...(saved.planRows || []), ...(saved.stewardRows || [])].filter(
  (r) =>
    isIbisStylesName(r.censusName) &&
    r.matchConfidence === "high" &&
    r.matchScore >= opts.minScore &&
    r.propertyId &&
    r.propertyUrl
);

const base = getPlatformBase();
const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", "Amenities", "Property ID"],
    filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

const censusById = new Map(records.map((r) => [r.id, r]));

/** @type {object[]} */
const plan = [];
for (const row of candidates) {
  const rec = censusById.get(row.censusRecordId);
  if (!rec) continue;

  const f = rec.fields || {};
  const url = accorCanonicalPropertyUrl(row.propertyId) || row.propertyUrl;
  const applyFields = {};

  if (isBlankCensusValue(f.Website) && url) applyFields.Website = url;
  if (isBlankCensusValue(f["Property ID"]) && row.propertyId) {
    applyFields["Property ID"] = String(row.propertyId).toUpperCase();
  }

  const needsAmenities = isBlankCensusValue(f.Amenities);
  if (!Object.keys(applyFields).length && !needsAmenities) continue;

  plan.push({
    censusRecordId: row.censusRecordId,
    censusName: row.censusName,
    propertyId: String(row.propertyId).toUpperCase(),
    propertyUrl: url,
    matchScore: row.matchScore,
    matchReason: row.matchReason,
    applyFields,
    needsAmenities,
  });
}

// Dedupe by census id (first wins — plan rows before steward in merged list; re-sort by score)
const byId = new Map();
for (const row of [...plan].sort((a, b) => b.matchScore - a.matchScore)) {
  if (!byId.has(row.censusRecordId)) byId.set(row.censusRecordId, row);
}
const deduped = [...byId.values()];

console.log("=== Accor ibis Styles batch ===\n");
console.log("High-confidence candidates:", candidates.length);
console.log("Rows needing fill-blank:", deduped.length);

if (opts.fetchAmenities) {
  console.log("\nFetching amenities...\n");
  let n = 0;
  for (const row of deduped) {
    if (!row.needsAmenities || row.applyFields.Amenities) continue;
    n++;
    process.stdout.write(` [${n}] ${row.censusName}...`);
    const fetched = await fetchAccorHotelAmenities(row.propertyUrl);
    await sleep(opts.delayMs);
    if (fetched.amenitiesText) {
      row.applyFields.Amenities = fetched.amenitiesText;
      console.log(` ${fetched.amenities.length} amenities`);
    } else {
      console.log(" (no amenities parsed)");
    }
  }
}

const withPayload = deduped.filter((r) => Object.keys(r.applyFields).length > 0);
mkdirSync(REPORTS, { recursive: true });
const outPath = join(REPORTS, "accor-ibis-styles-batch-plan.json");
writeFileSync(
  outPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      ready: withPayload.length,
      plan: withPayload,
    },
    null,
    2
  )
);
console.log("\nPlan with apply payload:", withPayload.length);
console.log("Written:", outPath);

if (!opts.apply) {
  console.log("\nDry-run. Use --apply --fetch-amenities");
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
    `${appliedAt},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},ibis_styles_batch,${count}\n`
  );
}

console.log("Applied:", updated);
