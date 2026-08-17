#!/usr/bin/env node
/**
 * Apply Accor census rows from improved geo/brand matching + live amenity fetch.
 * Uses steward-review rows OR re-plans with Accor scoring profile.
 *
 *   node scripts/run-accor-census-geo-auto-apply.mjs
 *   node scripts/run-accor-census-geo-auto-apply.mjs --apply --fetch-amenities --delay-ms=1200
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { planBrandCensusDirectoryMatch } from "../lib/hotel-census/plan-brand-census-directory-match.js";
import { loadAccorDirectoryRows } from "../lib/hotel-census/plan-accor-census-sitemap-match.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const PLAN_PATH = join(REPORTS, "accor-census-match-expansion-plan.json");

function loadAccorDirectoryFromExpansionPlan() {
  if (!existsSync(PLAN_PATH)) return [];
  const data = JSON.parse(readFileSync(PLAN_PATH, "utf8"));
  /** @type {Map<string, object>} */
  const byId = new Map();
  for (const row of [...(data.planRows || []), ...(data.stewardRows || [])]) {
    const id = String(row.propertyId || "").toUpperCase();
    if (!id || byId.has(id)) continue;
    byId.set(id, {
      propertyUrl: row.propertyUrl,
      propertyId: id,
      inferredHotelName: row.directoryHotelName,
      city: row.directoryCity,
      country: row.directoryCountry,
      amenitiesText: row.applyFields?.Amenities || "",
      source: "accor_sitemap",
      calaFilterStatus: "included",
    });
  }
  return [...byId.values()];
}

function loadAccorDirectoryRowsWithFallback() {
  const fromExtract = loadAccorDirectoryRows();
  if (fromExtract.length) return fromExtract;
  const fromPlan = loadAccorDirectoryFromExpansionPlan();
  if (fromPlan.length) {
    console.log("Using expansion-plan directory fallback:", fromPlan.length, "rows");
  }
  return fromPlan;
}

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 1200),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 50),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const opts = parseArgs();
const directoryRows = loadAccorDirectoryRowsWithFallback();
if (!directoryRows.length) {
  throw new Error("No Accor directory rows with metadata. Wait for extract or restore accor-property-directory-extract.json");
}

console.log("=== Accor geo-auto apply (improved scoring) ===\n");
console.log("Directory rows:", directoryRows.length);

const plan = await planBrandCensusDirectoryMatch({
  parentFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
  directoryRows,
  minScore: opts.minScore,
  requireCountryMatch: true,
  minApplyConfidence: "medium",
  includeAmenitiesFromCache: true,
  scoringProfile: "accor",
});

const autoRows = [...plan.planRows];
const geoSteward = plan.stewardRows.filter(
  (r) =>
    r.matchConfidence === "medium" ||
    (r.matchScore >= 58 &&
      r.distanceMeters != null &&
      r.distanceMeters <= 100 &&
      /accor geo-anchored|brand token/i.test(r.matchReason || ""))
);

console.log("Auto-apply ready:", autoRows.length);
console.log("Geo-medium steward:", geoSteward.length);
console.log("Still manual review:", plan.stewardRows.length - geoSteward.length);

/** @type {object[]} */
const amenityPlans = [];
if (opts.fetchAmenities) {
  console.log("\nFetching amenities for rows missing cache...\n");
  let n = 0;
  for (const row of [...autoRows, ...geoSteward]) {
    if (!isBlankCensusValue(row.applyFields?.Amenities)) continue;
    const url =
      row.propertyUrl ||
      accorCanonicalPropertyUrl(row.propertyId) ||
      row.applyFields?.Website;
    if (!url) continue;
    n++;
    console.log(` [${n}] ${row.censusName}`);
    const fetched = await fetchAccorHotelAmenities(url);
    await sleep(opts.delayMs);
    if (fetched.amenitiesText) {
      row.applyFields = { ...row.applyFields, Amenities: fetched.amenitiesText };
      amenityPlans.push({
        censusRecordId: row.censusRecordId,
        censusName: row.censusName,
        propertyUrl: url,
        amenityCount: fetched.amenities.length,
      });
    }
  }
  console.log("Amenities fetched:", amenityPlans.length);
}

const allApply = [...autoRows, ...geoSteward].filter((r) => Object.keys(r.applyFields || {}).length > 0);

mkdirSync(REPORTS, { recursive: true });
const reportPath = join(REPORTS, "accor-geo-auto-apply-plan.json");
writeFileSync(
  reportPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      ready: allApply.length,
      autoRows: autoRows.length,
      geoSteward: geoSteward.length,
      plan: allApply,
      amenityPlans,
    },
    null,
    2
  )
);
console.log("Plan:", reportPath);

if (!opts.apply) {
  console.log("\nDry-run. Use --apply [--fetch-amenities] to write.");
  process.exit(0);
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
let updated = 0;
for (let i = 0; i < allApply.length; i += 10) {
  const batch = allApply.slice(i, i + 10).map((r) => ({
    id: r.censusRecordId,
    fields: r.applyFields,
  }));
  await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
  updated += batch.length;
}
console.log("Applied:", updated);
