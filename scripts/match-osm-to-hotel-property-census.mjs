/**
 * Match OSM dry-run candidates → Hotel Property Census (READ-ONLY).
 *
 * Production SoT only. Does NOT use legacy Hotel Census.
 *
 * Usage:
 *   node scripts/match-osm-to-hotel-property-census.mjs \
 *     --input reports/independent-census-osm-dry-run-….json
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join, dirname, basename } from "path";
import { fileURLToPath } from "url";
import {
  loadHotelPropertyCensusReadOnly,
  matchAllCandidatesToHotelPropertyCensus,
  HPC_MATCH_VERSION,
} from "../lib/independent-census/match-hotel-property-census.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";
import { normalizeCountry } from "../lib/independent-census/match-current-census.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error("--apply not supported. Read-only Hotel Property Census match.");
  }
  let input = "";
  let country = "";
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1]) input = argv[++i];
    else if (a.startsWith("--input=")) input = a.slice("--input=".length);
    else if (a === "--country" && argv[i + 1]) country = argv[++i];
    else if (a.startsWith("--country=")) country = a.slice("--country=".length);
  }
  if (!input) throw new Error("Missing --input OSM dry-run JSON");
  return {
    inputPath: join(process.cwd(), input.replace(/^"|"$/g, "")),
    country: country.replace(/^"|"$/g, ""),
  };
}

function osmIdentityKey(sourceRecordId) {
  const id = String(sourceRecordId || "")
    .replace(/\//g, "_")
    .toLowerCase();
  return id ? `osm_do_${id}` : "";
}

async function main() {
  const { inputPath, country } = parseArgs();
  if (!existsSync(inputPath)) throw new Error(`Not found: ${inputPath}`);
  const osm = JSON.parse(readFileSync(inputPath, "utf8"));
  if (!Array.isArray(osm.candidates)) throw new Error("Missing candidates[]");

  const batchId =
    osm.batchId ||
    basename(inputPath, ".json").replace(/^independent-census-osm-dry-run-/, "");
  const countryFilter =
    country || osm.geography?.country || "Dominican Republic";

  console.log("=== OSM ↔ Hotel Property Census match (read-only) ===\n");
  console.log(`Version:     ${HPC_MATCH_VERSION}`);
  console.log(`Input:       ${inputPath}`);
  console.log(`Batch:       ${batchId}`);
  console.log(`Candidates:  ${osm.candidates.length}`);
  console.log(`Country:     ${countryFilter}`);
  console.log(`Legacy Hotel Census: FORBIDDEN (not used)\n`);

  console.log("Loading Hotel Property Census…");
  const census = await loadHotelPropertyCensusReadOnly({
    countryFilter,
  });
  console.log(
    `  Table: ${census.table} (${census.tableId})`
  );
  console.log(
    `  Loaded ${census.totalLoaded} total; country pool (${normalizeCountry(countryFilter) || countryFilter}): ${census.rows.length}`
  );

  const { rows, summary } = matchAllCandidatesToHotelPropertyCensus(
    osm.candidates,
    census,
    { identityKeyFn: (c) => osmIdentityKey(c.sourceRecordId) }
  );

  const jsonOut = join(
    REPORTS_DIR,
    `independent-census-hpc-match-${batchId}.json`
  );
  const csvOut = join(
    REPORTS_DIR,
    `independent-census-hpc-match-${batchId}.csv`
  );

  const report = {
    generatedAt: new Date().toISOString(),
    version: HPC_MATCH_VERSION,
    batchId,
    inputReport: inputPath,
    dedupe_source_of_truth: "Hotel Property Census",
    legacy_hotel_census_used: false,
    osmCandidateCount: osm.candidates.length,
    productionCensusTotal: census.totalLoaded,
    productionCensusMatchingPoolSize: census.rows.length,
    productionCensusCountryFilter: countryFilter,
    productionCensusTableId: census.tableId,
    dryRun: true,
    airtableWrites: false,
    summary,
    matches: rows,
  };

  writeJson(jsonOut, report);
  writeCsv(csvOut, rows, [
    "sourceRecordId",
    "rawHotelName",
    "rawCity",
    "rawCountry",
    "rawWebsite",
    "proposedIdentityKey",
    "matchConfidence",
    "matchScore",
    "matchReason",
    "recommendedAction",
    "matchedCensusRecordId",
    "matchedCensusName",
    "matchedCensusCity",
    "matchedCensusCountry",
    "matchedIdentityKey",
    "matchedCurrentBrand",
    "matchedAffiliationStatus",
    "distanceMeters",
    "identityKeyCollision",
  ]);

  console.log("\n--- Match confidence ---");
  console.log(`  high:   ${summary.high}`);
  console.log(`  medium: ${summary.medium}`);
  console.log(`  low:    ${summary.low}`);
  console.log(`  none:   ${summary.none}`);
  console.log("\n--- Recommended action ---");
  console.log(`  likely_existing:            ${summary.likely_existing}`);
  console.log(`  possible_duplicate_review:  ${summary.possible_duplicate_review}`);
  console.log(`  likely_new_candidate:       ${summary.likely_new_candidate}`);
  console.log(`  needs_research:             ${summary.needs_research}`);
  console.log(`  identity_key_collisions:    ${summary.identity_key_collisions}`);
  console.log(`\n  wrote: ${jsonOut}`);
  console.log("✓ Read-only. Legacy Hotel Census was not read.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
