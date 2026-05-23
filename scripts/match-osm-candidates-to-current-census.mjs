/**
 * Phase 2C — Match OSM dry-run candidates to Hotel Census (READ-ONLY).
 *
 * Loads local OSM JSON report + reads Hotel Census (allowed fields only).
 * Writes local match reports only. No Airtable writes.
 *
 * Usage:
 *   node scripts/match-osm-candidates-to-current-census.mjs --input reports/independent-census-osm-dry-run-osm-dominican-republic-2026-05-20.json
 *
 * Does NOT support --apply.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join, dirname, basename } from "path";
import { fileURLToPath } from "url";
import {
  loadHotelCensusReadOnly,
  matchAllCandidates,
  normalizeCountry,
} from "../lib/independent-census/match-current-census.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");

const CSV_COLUMNS = [
  "sourceRecordId",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "rawLatitude",
  "rawLongitude",
  "osmTourismTag",
  "candidateDedupeKey",
  "matchConfidence",
  "matchScore",
  "matchReason",
  "matchedCensusRecordId",
  "matchedCensusName",
  "matchedCensusCity",
  "matchedCensusCountry",
  "distanceMeters",
  "recommendedAction",
];

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      "--apply is not supported. Phase 2C is read-only matching; no Airtable writes."
    );
  }

  let input = "";
  for (let i = 2; i < process.argv.length; i++) {
    const a = process.argv[i];
    if (a === "--input" && process.argv[i + 1]) {
      input = process.argv[++i].replace(/^"|"$/g, "");
    } else if (a.startsWith("--input=")) {
      input = a.slice("--input=".length).replace(/^"|"$/g, "");
    }
  }

  if (!input) {
    throw new Error(
      "Missing --input path to OSM dry-run JSON (e.g. reports/independent-census-osm-dry-run-osm-dominican-republic-2026-05-20.json)"
    );
  }

  const inputPath = join(process.cwd(), input);
  return { inputPath };
}

function loadOsmReport(inputPath) {
  if (!existsSync(inputPath)) {
    throw new Error(`OSM report not found: ${inputPath}`);
  }
  const data = JSON.parse(readFileSync(inputPath, "utf8"));
  if (!Array.isArray(data.candidates)) {
    throw new Error("Invalid OSM report: missing candidates array");
  }
  const batchId = data.batchId || basename(inputPath, ".json").replace(/^independent-census-osm-dry-run-/, "");
  return { data, batchId, candidates: data.candidates };
}

async function main() {
  const { inputPath } = parseArgs();
  const { data: osmReport, batchId, candidates } = loadOsmReport(inputPath);

  const jsonOut = join(REPORTS_DIR, `independent-census-osm-current-match-${batchId}.json`);
  const csvOut = join(REPORTS_DIR, `independent-census-osm-current-match-${batchId}.csv`);

  console.log("=== OSM candidates ↔ Hotel Census match (Phase 2C, read-only) ===\n");
  console.log(`Input:  ${inputPath}`);
  console.log(`Batch:  ${batchId}`);
  console.log(`OSM candidates: ${candidates.length}\n`);

  const geoCountry = osmReport.geography?.country || candidates[0]?.rawCountry || "";
  const countryFilter = geoCountry ? normalizeCountry(geoCountry) : "";

  console.log("Loading Hotel Census (read-only, non-STR fields only)…");
  const censusData = await loadHotelCensusReadOnly(
    countryFilter ? { countryFilter: geoCountry } : {}
  );
  console.log(
    `  Loaded ${censusData.totalLoaded} census rows; matching pool: ${censusData.rows.length}` +
      (countryFilter ? ` (country filter: ${geoCountry})` : "")
  );
  console.log(`  Fields read: ${censusData.fieldsLoaded.join(", ")}\n`);

  console.log("Matching…");
  const { rows, summary } = matchAllCandidates(candidates, censusData);

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "2C-read-only-match",
    batchId,
    inputReport: inputPath,
    osmCandidateCount: candidates.length,
    censusRowsLoaded: censusData.totalLoaded,
    censusMatchingPoolSize: censusData.rows.length,
    censusCountryFilter: geoCountry || null,
    censusFieldsRead: censusData.fieldsLoaded,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandAliasWrites: false,
    stagingTableWrites: false,
    strFieldsUsed: false,
    summary,
    reportFiles: { json: jsonOut, csv: csvOut },
    matches: rows,
  };

  writeJson(jsonOut, report);
  writeCsv(csvOut, rows, CSV_COLUMNS);

  console.log("--- Match confidence ---");
  console.log(`  high:   ${summary.high}`);
  console.log(`  medium: ${summary.medium}`);
  console.log(`  low:    ${summary.low}`);
  console.log(`  none:   ${summary.none}`);
  console.log("\n--- Recommended action ---");
  console.log(`  likely_existing:            ${summary.likely_existing}`);
  console.log(`  possible_duplicate_review:  ${summary.possible_duplicate_review}`);
  console.log(`  likely_new_candidate:       ${summary.likely_new_candidate}`);
  console.log(`  needs_research:             ${summary.needs_research}`);
  console.log(`  skip_missing_name:          ${summary.skip_missing_name}`);
  if (summary.averageDistanceMetersHighMatches != null) {
    console.log(`\n  Avg distance (high matches): ${summary.averageDistanceMetersHighMatches}m`);
  }
  if (summary.topDuplicateRiskNames?.length) {
    console.log("\n  Top duplicate-risk names:", summary.topDuplicateRiskNames.slice(0, 5));
  }
  console.log("\nReport files:");
  console.log(`  ${jsonOut}`);
  console.log(`  ${csvOut}`);
  console.log("\n✓ Read-only complete. No Airtable writes. Hotel Census not modified.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
