/**
 * Phase 4O — Independent Hotel Source Candidates coverage, dedupe, retention (read-only).
 */
import "../load-env.js";
import { join } from "path";
import {
  loadAllCandidatesForCoverage,
  loadVerifiedByCountry,
  analyzeCandidateCoverage,
  mergeVerifiedCoverageGap,
  COVERAGE_CSV_COLUMNS,
  coverageRowToCsv,
  RETENTION,
} from "../lib/independent-census/candidate-coverage-dedupe.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error("--apply is not supported. Phase 4O is read-only reporting.");
  }

  let batchId = "candidate-coverage-dedupe-2026-05-20";
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  const reportSlug = batchId.includes("candidate-coverage")
    ? "candidate-coverage-dedupe-2026-05-20"
    : `candidate-coverage-dedupe-${batchId}`;

  return {
    batchId,
    jsonPath: join(REPORTS_DIR, `independent-census-${reportSlug}.json`),
    csvPath: join(REPORTS_DIR, `independent-census-${reportSlug}.csv`),
  };
}

async function main() {
  const args = parseArgs();

  console.log("=== Independent census candidate coverage (Phase 4O, read-only) ===\n");
  console.log(`Report batch: ${args.batchId}\n`);

  console.log("Loading all Independent Hotel Source Candidates…");
  const { totalLoaded, rows } = await loadAllCandidatesForCoverage();
  console.log(`  Loaded: ${totalLoaded}\n`);

  console.log("Loading Verified Independent Hotel Census (country counts)…");
  const verified = await loadVerifiedByCountry();
  console.log(`  Verified rows: ${verified.total}\n`);

  console.log("Analyzing coverage, dedupe clusters, retention…");
  const analysis = analyzeCandidateCoverage(rows);
  const verifiedGaps = mergeVerifiedCoverageGap(analysis.byCountry, verified.byCountry);

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4O-candidate-coverage-dedupe",
    batchId: args.batchId,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    evidenceWrites: false,
    verifiedWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    summary: {
      totalCandidates: analysis.totalCandidates,
      usefulForValidationEstimate: analysis.usefulForValidationEstimate,
      bySourceType: analysis.bySourceType,
      byCountryTop: Object.entries(analysis.byCountry)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 20)
        .map(([country, count]) => ({ country, count })),
      byQualityTier: analysis.byQualityTier,
      byRecommendedAction: analysis.byRecommendedAction,
      missingTotals: analysis.missingTotals,
      duplicateRiskClusterCount: analysis.duplicateRiskClusterCount,
      duplicateRiskRecordCount: analysis.duplicateRiskRecordCount,
      byRetention: analysis.byRetention,
      brandDirectoryCount: analysis.brandDirectoryCount,
      osmCount: analysis.osmCount,
      osmOverlappingBrandDirectoryCount: analysis.osmOverlappingBrandDirectoryCount,
      brandDirectoryByParent: analysis.brandDirectoryByParent,
      brandDirectoryByBrand: analysis.brandDirectoryByBrand,
      excessiveCountries: analysis.excessiveCountries,
      lowVerifiedCoverageCountries: verifiedGaps.slice(0, 25),
      importBatchCount: Object.keys(analysis.byBatch).length,
    },
    byBatch: analysis.byBatch,
    duplicateClustersSample: analysis.duplicateClustersSample,
    verifiedByCountry: verified.byCountry,
    reportFiles: { json: args.jsonPath, csv: args.csvPath },
    candidateRows: analysis.rows,
  };

  writeJson(args.jsonPath, report);
  writeCsv(args.csvPath, analysis.rows.map(coverageRowToCsv), COVERAGE_CSV_COLUMNS);

  console.log("--- Summary ---");
  console.log(`  Total candidates:        ${analysis.totalCandidates}`);
  console.log(`  Useful for validation:   ${analysis.usefulForValidationEstimate} (est.)`);
  console.log("\n  By source type:");
  Object.entries(analysis.bySourceType)
    .sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`    ${k}: ${v}`));

  console.log("\n  Top countries:");
  Object.entries(analysis.byCountry)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .forEach(([k, v]) => console.log(`    ${k}: ${v}`));

  console.log("\n  Duplicate-risk:");
  console.log(`    Clusters (size≥2):     ${analysis.duplicateRiskClusterCount}`);
  console.log(`    Records in clusters:  ${analysis.duplicateRiskRecordCount}`);

  console.log("\n  Retention recommendations:");
  console.log(`    keep_high_priority:      ${analysis.byRetention[RETENTION.KEEP_HIGH] || 0}`);
  console.log(`    keep_for_matching:       ${analysis.byRetention[RETENTION.KEEP_MATCHING] || 0}`);
  console.log(`    enrich_next:             ${analysis.byRetention[RETENTION.ENRICH_NEXT] || 0}`);
  console.log(`    duplicate_review:        ${analysis.byRetention[RETENTION.DUPLICATE_REVIEW] || 0}`);
  console.log(`    low_priority_hold:       ${analysis.byRetention[RETENTION.LOW_HOLD] || 0}`);
  console.log(`    possible_archive_later:  ${analysis.byRetention[RETENTION.ARCHIVE_LATER] || 0}`);

  console.log("\n  Brand-directory:");
  console.log(`    Total:                   ${analysis.brandDirectoryCount}`);
  console.log(`    OSM may enrich Choice:   ${analysis.osmOverlappingBrandDirectoryCount}`);

  if (analysis.excessiveCountries.length) {
    console.log("\n  Excessive volume countries (≥5000):");
    analysis.excessiveCountries.forEach((x) =>
      console.log(`    ${x.country}: ${x.count}`)
    );
  }

  console.log("\nReport files:");
  console.log(`  ${args.jsonPath}`);
  console.log(`  ${args.csvPath}`);
  console.log("\n✓ Read-only complete. No Airtable writes.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
