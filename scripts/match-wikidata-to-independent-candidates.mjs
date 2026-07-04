/**
 * Phase 2E — Match Wikidata dry-run candidates to staging OSM candidates (READ-ONLY).
 *
 * Reads Wikidata local JSON + Independent Hotel Source Candidates from Airtable.
 * No writes. Rejects --apply.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join, basename } from "path";
import { SOURCE_TYPES } from "../lib/independent-census/fields.js";
import {
  loadStagingCandidatesReadOnly,
  matchWikidataToStaging,
} from "../lib/independent-census/match-staging-candidates.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

const CSV_COLUMNS = [
  "wikidataQid",
  "wikidataName",
  "wikidataCity",
  "wikidataCountry",
  "wikidataLatitude",
  "wikidataLongitude",
  "wikidataWebsite",
  "matchedStagingRecordId",
  "matchedStagingSourceRecordId",
  "matchedStagingName",
  "matchedStagingCity",
  "matchConfidence",
  "matchScore",
  "matchReason",
  "distanceMeters",
  "nameSimilarity",
  "recommendedAction",
];

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error("--apply is not supported. Read-only match only.");
  }

  let input = "";
  let sourceBatchId = "osm-dominican-republic-hotel-focused-2026-05-20";
  let country = "Dominican Republic";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1]) input = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--input=")) input = a.slice("--input=".length).replace(/^"|"$/g, "");
    else if (a === "--source-batch-id" && argv[i + 1])
      sourceBatchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--source-batch-id="))
      sourceBatchId = a.slice("--source-batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--country" && argv[i + 1]) country = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country=")) country = a.slice("--country=".length).replace(/^"|"$/g, "");
  }

  if (!input) {
    throw new Error("Missing --input (Wikidata dry-run JSON path)");
  }

  return {
    inputPath: join(process.cwd(), input),
    sourceBatchId,
    country,
  };
}

function loadWikidataReport(inputPath) {
  if (!existsSync(inputPath)) throw new Error(`Wikidata report not found: ${inputPath}`);
  const data = JSON.parse(readFileSync(inputPath, "utf8"));
  if (!Array.isArray(data.candidates)) {
    throw new Error("Invalid Wikidata report: missing candidates array");
  }
  const batchId =
    data.batchId ||
    basename(inputPath, ".json").replace(/^independent-census-wikidata-dry-run-/, "");
  return { data, candidates: data.candidates, batchId };
}

async function main() {
  const { inputPath, sourceBatchId, country } = parseArgs();
  const { data, candidates, batchId } = loadWikidataReport(inputPath);

  const jsonOut = join(REPORTS_DIR, `independent-census-wikidata-candidate-match-${batchId}.json`);
  const csvOut = join(REPORTS_DIR, `independent-census-wikidata-candidate-match-${batchId}.csv`);

  console.log("=== Wikidata ↔ staging candidates match (Phase 2E, read-only) ===\n");
  console.log(`Wikidata input:  ${inputPath}`);
  console.log(`Wikidata batch:  ${batchId}`);
  console.log(`Wikidata rows:   ${candidates.length}`);
  console.log(`Staging batch:   ${sourceBatchId} (${SOURCE_TYPES.OSM})\n`);

  console.log("Loading Independent Hotel Source Candidates (read-only)…");
  const staging = await loadStagingCandidatesReadOnly({
    importBatchId: sourceBatchId,
    sourceType: SOURCE_TYPES.OSM,
    countryFilter: country,
  });
  console.log(
    `  Loaded ${staging.totalLoaded} staging rows; pool: ${staging.matchingPoolSize}\n`
  );

  console.log("Matching…");
  const { rows, summary } = matchWikidataToStaging(candidates, staging);

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "2E-read-only-staging-match",
    wikidataBatchId: batchId,
    stagingBatchId: sourceBatchId,
    stagingSourceType: SOURCE_TYPES.OSM,
    countryFilter: country,
    wikidataCandidateCount: candidates.length,
    stagingRowsLoaded: staging.totalLoaded,
    stagingMatchingPoolSize: staging.matchingPoolSize,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    stagingTableUpdates: false,
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
  console.log(`  likely_same_property:    ${summary.likely_same_property}`);
  console.log(`  possible_same_property:  ${summary.possible_same_property}`);
  console.log(`  likely_new_wikidata:     ${summary.likely_new_wikidata}`);
  console.log(`  needs_research:          ${summary.needs_research}`);
  if (summary.averageDistanceMetersHigh != null) {
    console.log(`\n  Avg distance (high): ${summary.averageDistanceMetersHigh}m`);
  }
  console.log("\nReport files:");
  console.log(`  ${jsonOut}`);
  console.log(`  ${csvOut}`);
  console.log("\n✓ Read-only complete. Staging records not modified.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
