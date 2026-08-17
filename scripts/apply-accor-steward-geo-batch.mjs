#!/usr/bin/env node
/**
 * Re-score Accor steward-review rows with improved Accor matching and apply fill-blank.
 * Works from saved expansion plan (no full directory extract required).
 *
 *   node scripts/apply-accor-steward-geo-batch.mjs
 *   node scripts/apply-accor-steward-geo-batch.mjs --apply --fetch-amenities --delay-ms=1200
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "../lib/hotel-census/match-brand-directory-to-census.js";
import { mapExtractRowToDirectoryMatchRow } from "../lib/hotel-census/plan-brand-census-directory-match.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const PLAN_PATH = join(REPORTS, "accor-census-match-expansion-plan.json");

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 1200),
    minConfidence: args.find((a) => a.startsWith("--min-confidence="))?.split("=")[1] || "medium",
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const CONF_RANK = { high: 3, medium: 2, low: 1, none: 0 };
const opts = parseArgs();
const minRank = CONF_RANK[opts.minConfidence] ?? 2;

if (!existsSync(PLAN_PATH)) {
  throw new Error(`Missing ${PLAN_PATH}. Run match expansion first.`);
}

const saved = JSON.parse(readFileSync(PLAN_PATH, "utf8"));
const candidates = [...(saved.planRows || []), ...(saved.stewardRows || [])];

const base = getPlatformBase();
const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", "Amenities", "Property ID", CENSUS_FIELDS.city, CENSUS_FIELDS.country],
    filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

const censusById = new Map(records.map((r) => [r.id, mapCensusRowForDirectoryMatch(r)]));

/** @type {object[]} */
const ready = [];
for (const row of candidates) {
  const censusRow = censusById.get(row.censusRecordId);
  if (!censusRow) continue;

  const dirMatch = mapExtractRowToDirectoryMatchRow(
    {
      inferredHotelName: row.directoryHotelName,
      city: row.directoryCity,
      country: row.directoryCountry,
      propertyUrl: row.propertyUrl,
      propertyId: row.propertyId,
      amenitiesText: row.applyFields?.Amenities || "",
      source: "accor_sitemap",
    },
    { scoringProfile: "accor" }
  );

  const scored = scoreDirectoryAgainstCensus(dirMatch, censusRow);
  let confidence = scored.confidence;
  let reason = scored.reason;

  // Steward plan captured geo distance from full extract; reuse when directory coords absent.
  if (
    confidence === "none" &&
    row.distanceMeters != null &&
    row.distanceMeters <= 100 &&
    scored.nameSim >= 0.35 &&
    row.matchScore >= 55
  ) {
    confidence = "medium";
    reason = "Accor geo-anchored match (steward distance ≤100m)";
  }

  const rank = CONF_RANK[confidence] ?? 0;
  if (rank < minRank) continue;

  const f = censusRow.fields || {};
  const applyFields = {};
  const url = accorCanonicalPropertyUrl(row.propertyId) || row.propertyUrl;

  if (isBlankCensusValue(f.Website) && url) applyFields.Website = url;
  if (isBlankCensusValue(f["Property ID"]) && row.propertyId) {
    applyFields["Property ID"] = String(row.propertyId).toUpperCase();
  }

  if (!Object.keys(applyFields).length && isBlankCensusValue(f.Amenities)) {
    // amenities-only path handled below
  }

  const needsAmenities = isBlankCensusValue(f.Amenities);
  const hasApply = Object.keys(applyFields).length > 0;
  if (!hasApply && !needsAmenities) continue;

  ready.push({
    censusRecordId: row.censusRecordId,
    censusName: row.censusName,
    propertyId: row.propertyId,
    propertyUrl: url,
    oldScore: row.matchScore,
    newScore: scored.score,
    newConfidence: confidence,
    newReason: reason,
    nameSim: scored.nameSim,
    distanceMeters: scored.distanceMeters,
    applyFields,
    needsAmenities,
  });
}

// Dedupe by census id (prefer higher new score)
const byCensus = new Map();
for (const r of ready.sort((a, b) => b.newScore - a.newScore)) {
  if (!byCensus.has(r.censusRecordId)) byCensus.set(r.censusRecordId, r);
}
const plan = [...byCensus.values()];

console.log("=== Accor steward geo-batch (re-scored) ===\n");
console.log("Candidates:", candidates.length);
console.log("Ready to apply:", plan.length);

if (opts.fetchAmenities) {
  console.log("\nFetching amenities...\n");
  let n = 0;
  for (const row of plan) {
    if (!row.needsAmenities || row.applyFields.Amenities) continue;
    n++;
    console.log(` [${n}] ${row.censusName}`);
    const fetched = await fetchAccorHotelAmenities(row.propertyUrl);
    await sleep(opts.delayMs);
    if (fetched.amenitiesText) {
      row.applyFields.Amenities = fetched.amenitiesText;
    }
  }
}

mkdirSync(REPORTS, { recursive: true });
const out = join(REPORTS, "accor-steward-geo-batch-plan.json");
writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), ready: plan.length, plan }, null, 2));
console.log("Plan:", out);

if (!opts.apply) {
  console.log("\nDry-run. Use --apply [--fetch-amenities]");
  process.exit(0);
}

const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
let updated = 0;
for (let i = 0; i < plan.length; i += 10) {
  const batch = plan
    .slice(i, i + 10)
    .filter((r) => Object.keys(r.applyFields).length)
    .map((r) => ({ id: r.censusRecordId, fields: r.applyFields }));
  if (!batch.length) continue;
  await airtable(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
  updated += batch.length;
}
console.log("Applied:", updated);
