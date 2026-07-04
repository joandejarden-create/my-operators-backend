/**
 * Phase 4E — Coverage benchmark vs legacy Hotel Census (READ-ONLY).
 *
 * Compares Hotel Census, Candidates, Evidence, and Verified tables.
 * No writes. No STR/CoStar fields exported.
 */
import "../load-env.js";
import { join } from "path";
import {
  loadHotelCensusReadOnly,
} from "../lib/independent-census/match-current-census.js";
import {
  loadAllCandidates,
  loadAllEvidence,
  loadAllVerified,
  buildCoverageBenchmark,
  benchmarkToCsvRows,
  BENCHMARK_CSV_COLUMNS,
} from "../lib/independent-census/coverage-benchmark.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let country = "Dominican Republic";
  let batchId = "coverage-dr-2026-05-20";
  let candidateBatchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--country" && argv[i + 1]) country = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country="))
      country = a.slice("--country=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-batch-id" && argv[i + 1])
      candidateBatchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-batch-id="))
      candidateBatchId = a.slice("--candidate-batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4E is report-only.");
    }
  }

  return { country, batchId, candidateBatchId };
}

async function main() {
  const { country, batchId, candidateBatchId } = parseArgs();
  const jsonPath = join(REPORTS_DIR, `independent-census-coverage-benchmark-${batchId}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-coverage-benchmark-${batchId}.csv`);

  console.log("=== Independent census coverage benchmark (Phase 4E, read-only) ===\n");
  console.log(`Focus country: ${country}`);
  console.log(`Report batch:  ${batchId}`);
  if (candidateBatchId) console.log(`Candidate batch filter: ${candidateBatchId}`);
  console.log("");

  console.log("Loading legacy Hotel Census (safe fields only)…");
  const censusData = await loadHotelCensusReadOnly();
  console.log(`  ${censusData.totalLoaded} rows; fields: ${censusData.fieldsLoaded.join(", ")}`);

  console.log("Loading Independent Hotel Source Candidates…");
  const allCandidates = await loadAllCandidates(
    candidateBatchId ? { importBatchId: candidateBatchId } : {}
  );
  console.log(`  ${allCandidates.totalLoaded} rows loaded`);

  console.log("Loading Independent Hotel Source Evidence…");
  const allEvidence = await loadAllEvidence();
  console.log(
    `  ${allEvidence.totalLoaded} evidence rows; ${allEvidence.evidenceSupportedCandidateIds.size} candidates with evidence`
  );

  console.log("Loading Verified Independent Hotel Census…");
  const allVerified = await loadAllVerified();
  console.log(`  ${allVerified.totalLoaded} verified rows\n`);

  console.log("Building benchmark metrics…");
  const benchmark = buildCoverageBenchmark({
    censusData,
    allCandidates,
    allEvidence,
    allVerified,
    focusCountry: country,
    candidateBatchId: candidateBatchId || null,
  });

  writeJson(jsonPath, benchmark);
  writeCsv(csvPath, benchmarkToCsvRows(benchmark), BENCHMARK_CSV_COLUMNS);

  const t = benchmark.totals;
  const dr = benchmark.dominicanRepublicSection;
  const foc = benchmark.overlap.focusCountry;

  console.log("--- Totals (all tables) ---");
  console.log(`  Legacy Hotel Census:              ${t.legacyHotelCensus}`);
  console.log(`  Independent candidates:         ${t.independentCandidates}`);
  console.log(`  Evidence rows:                    ${t.evidenceRows}`);
  console.log(`  Evidence-supported candidates:  ${t.evidenceSupportedCandidates}`);
  console.log(`  Verified records:               ${t.verifiedRecords}`);

  console.log(`\n--- Focus: ${country} ---`);
  console.log(`  Legacy records:                   ${dr.legacyRecordCount}`);
  console.log(`  OSM/staging candidates:           ${dr.independentCandidateCount}`);
  console.log(`  Wikidata evidence-supported:      ${dr.evidenceSupportedCandidateCount}`);
  console.log(`  Verified records:                 ${dr.verifiedRecordCount}`);

  console.log("\n--- Overlap (focus country) ---");
  console.log(`  Likely matched to legacy:         ${foc.likelyLegacyMatches}`);
  console.log(`  Legacy matched by candidates:     ${foc.legacyMatchedByCandidates}`);
  console.log(`  Legacy matched by verified:       ${foc.legacyMatchedByVerified}`);
  console.log(`  Likely independent-only:          ${foc.likelyIndependentOnlyCandidates}`);
  console.log(`  Likely legacy-only:               ${foc.likelyLegacyOnlyRecords}`);
  console.log(`  Duplicate-risk clusters:          ${foc.duplicateRiskClusterCount}`);
  console.log(`  Candidate coverage % vs legacy:   ${foc.candidateCoveragePct ?? "n/a"}%`);
  console.log(`  Verified coverage % vs legacy:    ${foc.verifiedCoveragePct ?? "n/a"}%`);

  console.log("\n--- DR recommended next validation sources ---");
  console.log(`  ${dr.recommendedNextValidationSources.join(", ")}`);

  console.log("\n--- Missing fields (focus scope) ---");
  console.log(JSON.stringify(benchmark.missingFieldsBySource, null, 2));

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ No Airtable writes. Hotel Census read-only. Verified / Candidates / Evidence untouched."
  );
  console.log("✓ No STR/CoStar fields loaded or exported.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
