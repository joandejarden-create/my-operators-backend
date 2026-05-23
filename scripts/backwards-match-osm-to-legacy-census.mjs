/**
 * Backwards-match: OSM candidates → read-only legacy Hotel Census → Verified (OSM fields only).
 *
 * Default: dry-run. Loads OSM rows from Phase 4O retention JSON (no Candidate table reads).
 * Requires --apply --approved-by and INDEPENDENT_CENSUS_PIPELINE_ENABLED=true to write Verified.
 */
import "../load-env.js";
import { join } from "path";
import {
  runBackwardsCensusMatch,
  runVerifiedIndexCheck,
  backwardsMatchRowToCsv,
  BACKWARDS_MATCH_CSV_COLUMNS,
  PROMOTION_RECOMMENDATION,
} from "../lib/independent-census/backwards-census-match.js";
import { isIndependentCensusPipelineEnabled } from "../lib/independent-census/platform-base.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let candidateRetentionReport = "";
  let country = "";
  let countries = "";
  let allCountries = false;
  let batchId = "";
  let minConfidence = "high";
  let maxPromotions = null;
  let approvedBy = "";
  let includeRetention = "";
  let excludeRetention = "";
  let apply = false;
  let allowMissingVerifiedIndex = false;
  let verifiedIndexCheckOnly = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--candidate-retention-report" && argv[i + 1])
      candidateRetentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      candidateRetentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--country" && argv[i + 1])
      country = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country="))
      country = a.slice("--country=".length).replace(/^"|"$/g, "");
    else if (a === "--countries" && argv[i + 1])
      countries = argv[++i];
    else if (a.startsWith("--countries="))
      countries = a.slice("--countries=".length);
    else if (a === "--all-countries") allCountries = true;
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--min-confidence" && argv[i + 1])
      minConfidence = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--min-confidence="))
      minConfidence = a.slice("--min-confidence=".length).toLowerCase();
    else if (a === "--max-promotions" && argv[i + 1])
      maxPromotions = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-promotions="))
      maxPromotions = parseInt(a.slice("--max-promotions=".length), 10);
    else if (a === "--approved-by" && argv[i + 1])
      approvedBy = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--approved-by="))
      approvedBy = a.slice("--approved-by=".length).replace(/^"|"$/g, "");
    else if (a === "--include-retention" && argv[i + 1])
      includeRetention = argv[++i];
    else if (a.startsWith("--include-retention="))
      includeRetention = a.slice("--include-retention=".length);
    else if (a === "--exclude-retention" && argv[i + 1])
      excludeRetention = argv[++i];
    else if (a.startsWith("--exclude-retention="))
      excludeRetention = a.slice("--exclude-retention=".length);
    else if (a === "--apply") apply = true;
    else if (a === "--dry-run") apply = false;
    else if (a === "--allow-missing-verified-index")
      allowMissingVerifiedIndex = true;
    else if (a === "--verified-index-check-only") verifiedIndexCheckOnly = true;
  }

  if (!verifiedIndexCheckOnly && !candidateRetentionReport) {
    throw new Error("Required: --candidate-retention-report");
  }
  if (!batchId) throw new Error("Required: --batch-id");
  if (apply && !approvedBy) {
    throw new Error("--approved-by is required when using --apply");
  }
  if (apply && !isIndependentCensusPipelineEnabled()) {
    throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
  }

  return {
    retentionReportPath: join(process.cwd(), candidateRetentionReport),
    country,
    countries,
    allCountries,
    batchId,
    minConfidence,
    maxPromotions,
    approvedBy,
    includeRetention,
    excludeRetention,
    apply,
    allowMissingVerifiedIndex,
    verifiedIndexCheckOnly,
  };
}

function printSummary(result, jsonPath, csvPath) {
  console.log("\n--- Backwards-match summary ---");
  console.log(`Mode:                      ${result.mode}`);
  console.log(`Candidate source:          ${result.candidateSource}`);
  if (result.allCountries) console.log(`Countries:                 all`);
  else if (result.countryFilter?.length)
    console.log(`Countries:                 ${result.countryFilter.join(", ")}`);
  console.log(`OSM candidates loaded:     ${result.osmCandidatesLoaded}`);
  console.log(`OSM rows in retention:     ${result.osmRowsInRetentionReport}`);
  console.log(`Legacy census loaded:      ${result.legacyCensusRecordsLoaded}`);
  console.log(`Legacy census match pool:  ${result.legacyCensusRecordsInPool}`);
  console.log(`Match high:                ${result.confidenceCounts.high}`);
  console.log(`Match medium:              ${result.confidenceCounts.medium}`);
  console.log(`Match low:                 ${result.confidenceCounts.low}`);
  console.log(`Match none:                ${result.confidenceCounts.none}`);
  console.log(`Promotion eligible:        ${result.promotionEligibleCount}`);
  console.log(`Already verified:          ${result.alreadyVerifiedCount}`);
  console.log(`Duplicate/hold:            ${result.holdDuplicateCount}`);
  console.log(`Would promote (dry-run):   ${result.wouldPromoteCount}`);
  if (result.apply) {
    console.log(`Verified written:          ${result.writtenCount}`);
    console.log(`Skipped duplicate:         ${result.skippedDuplicateCount}`);
  }

  if (result.estimatedVerifiedByCountry?.length) {
    console.log("\n--- Estimated new Verified by country (eligible) ---");
    for (const row of result.estimatedVerifiedByCountry.slice(0, 12)) {
      console.log(`  ${row.country}: ${row.estimatedNewVerified}`);
    }
    if (result.estimatedVerifiedByCountry.length > 12) {
      console.log(
        `  … and ${result.estimatedVerifiedByCountry.length - 12} more countries`
      );
    }
  }

  if (result.ineligibleSummary?.byRecommendation?.length) {
    console.log("\n--- Top ineligible promotion reasons ---");
    for (const r of result.ineligibleSummary.byRecommendation.slice(0, 8)) {
      console.log(`  ${r.reason}: ${r.count}`);
    }
  }

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);

  const promotable = result.reportRows.filter(
    (r) =>
      r.promotionRecommendation ===
      PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW
  );
  if (promotable.length) {
    console.log("\n--- promote_after_review (sample) ---");
    for (const r of promotable.slice(0, 15)) {
      console.log(
        `  ${r.osmName} | ${r.osmCity} | ${r.matchedLegacyName} | score ${r.matchScore} | ${r.osmCandidateRecordId}`
      );
    }
    if (promotable.length > 15) {
      console.log(`  … and ${promotable.length - 15} more`);
    }
  }
}

async function main() {
  const args = parseArgs();

  console.log("=== Backwards-match OSM → legacy census → Verified ===\n");
  console.log(`Batch:              ${args.batchId}`);
  console.log(`Mode:               ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Retention report:   ${args.retentionReportPath}`);
  if (args.allCountries) console.log(`Scope:              all countries`);
  else if (args.countries) console.log(`Countries:          ${args.countries}`);
  else if (args.country) console.log(`Country:            ${args.country}`);
  console.log(`Min confidence:     ${args.minConfidence}`);
  if (args.includeRetention)
    console.log(`Include retention:  ${args.includeRetention}`);
  if (args.excludeRetention)
    console.log(`Exclude retention:  ${args.excludeRetention}`);
  if (args.maxPromotions != null) console.log(`Max promotions:     ${args.maxPromotions}`);
  if (args.approvedBy) console.log(`Approved by:        ${args.approvedBy}`);
  console.log(
    "OSM source: Phase 4O retention JSON only (no Candidate table API reads)."
  );
  if (args.verifiedIndexCheckOnly) {
    console.log("Mode: Verified dedupe index check only (no OSM matching).\n");
  } else {
    console.log(
      "Legacy Hotel Census: read-only benchmark only; Verified populated from OSM only.\n"
    );
  }

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-backwards-match-legacy-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-backwards-match-legacy-${args.batchId}.csv`
  );

  if (args.verifiedIndexCheckOnly) {
    const result = await runVerifiedIndexCheck({
      batchId: args.batchId,
      allowMissingVerifiedIndex: args.allowMissingVerifiedIndex,
    });
    writeJson(jsonPath, {
      generatedAt: new Date().toISOString(),
      ...result,
      reportFiles: { json: jsonPath },
    });
    console.log("\n--- Verified index check ---");
    console.log(`Load failed: ${result.verifiedIndexLoadFailed}`);
    if (result.verifiedIndexMeta?.success) {
      console.log(`Verified records: ${result.verifiedIndexMeta.verifiedRecordsLoaded}`);
      console.log(`Dedupe keys:      ${result.verifiedIndexMeta.dedupeKeysIndexed}`);
      console.log(`Candidate links:  ${result.verifiedIndexMeta.candidateLinksIndexed}`);
      console.log(`Duration (ms):    ${result.verifiedIndexMeta.loadDurationMs}`);
      console.log(`Retries:          ${result.verifiedIndexMeta.retryCount}`);
    } else {
      console.log(`Error: ${result.verifiedIndexMeta?.error || "unknown"}`);
    }
    console.log(`\nReport: ${jsonPath}`);
    if (result.verifiedIndexLoadFailed) process.exit(1);
    return;
  }

  const result = await runBackwardsCensusMatch({
    retentionReportPath: args.retentionReportPath,
    countryFilter: args.country,
    countriesStr: args.countries,
    allCountries: args.allCountries,
    batchId: args.batchId,
    minConfidence: args.minConfidence,
    maxPromotions: args.maxPromotions,
    approvedBy: args.approvedBy,
    includeRetention: args.includeRetention,
    excludeRetention: args.excludeRetention,
    apply: args.apply,
    allowMissingVerifiedIndex: args.allowMissingVerifiedIndex,
  });

  writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "backwards-match-legacy-census",
    ...result,
    reportFiles: { json: jsonPath, csv: csvPath },
  });
  writeCsv(
    csvPath,
    result.reportRows.map(backwardsMatchRowToCsv),
    BACKWARDS_MATCH_CSV_COLUMNS
  );

  printSummary(result, jsonPath, csvPath);

  console.log(
    "\n✓ Hotel Census read-only. Brand Setup, Brand Alias, Candidates, Evidence untouched."
  );
  if (!args.apply) {
    console.log("✓ No Verified writes (dry-run). Review report before --apply.");
  }
  if (result.verifiedIndexLoadFailed) {
    console.log(
      "⚠ Verified index did not load — alreadyVerified counts may be understated."
    );
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
