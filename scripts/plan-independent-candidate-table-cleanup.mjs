/**
 * Report-only plan for Independent Hotel Source Candidates table cleanup.
 * No archive, delete, or Airtable writes.
 */
import "../load-env.js";
import { join } from "path";
import {
  runCandidateCleanupPlan,
  cleanupPlanRowToCsv,
  CLEANUP_PLAN_CSV_COLUMNS,
} from "../lib/independent-census/candidate-cleanup-plan.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let input = "";
  let backwardsMatchReport = "";
  let batchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1])
      input = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--input="))
      input = a.slice("--input=".length).replace(/^"|"$/g, "");
    else if (a === "--backwards-match-report" && argv[i + 1])
      backwardsMatchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--backwards-match-report="))
      backwardsMatchReport = a
        .slice("--backwards-match-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  if (!input) throw new Error("Required: --input (coverage dedupe report)");
  if (!batchId) throw new Error("Required: --batch-id");

  return {
    coverageReportPath: join(process.cwd(), input),
    backwardsMatchReportPath: backwardsMatchReport
      ? join(process.cwd(), backwardsMatchReport)
      : "",
    batchId,
  };
}

function printSummary(result, jsonPath, csvPath) {
  console.log("\n--- Candidate cleanup plan (report only) ---");
  console.log(`Total candidates:              ${result.totalCandidates}`);
  console.log(`Keep in Airtable (all keep-*): ${result.keepInAirtableCount}`);
  console.log(`Archive later (combined):      ${result.archiveLaterCount}`);
  console.log(`  export_to_raw_store:         ${result.exportToRawStoreCount}`);
  console.log(`  low_priority_archive_later:    ${result.lowPriorityArchiveLaterCount}`);
  console.log(
    `Estimated record reduction:    ${result.estimatedAirtableRecordReduction} (${result.estimatedReductionPct}% of candidates)`
  );
  console.log(`Backwards-match rows used:     ${result.backwardsMatchRowsUsed}`);

  console.log("\nBy classification:");
  for (const [cls, count] of Object.entries(result.byClassification).sort(
    (a, b) => b[1] - a[1]
  )) {
    console.log(`  ${cls}: ${count}`);
  }

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
}

async function main() {
  const args = parseArgs();

  console.log("=== Independent candidate cleanup plan ===\n");
  console.log(`Batch:                 ${args.batchId}`);
  console.log(`Coverage report:       ${args.coverageReportPath}`);
  if (args.backwardsMatchReportPath) {
    console.log(`Backwards-match report: ${args.backwardsMatchReportPath}`);
  }
  console.log("No archive, delete, or Airtable writes.\n");

  const result = runCandidateCleanupPlan({
    coverageReportPath: args.coverageReportPath,
    backwardsMatchReportPath: args.backwardsMatchReportPath,
    batchId: args.batchId,
  });

  const reportSlug = args.batchId.startsWith("candidate-cleanup-plan")
    ? args.batchId
    : `candidate-cleanup-plan-${args.batchId}`;
  const jsonPath = join(REPORTS_DIR, `independent-census-${reportSlug}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-${reportSlug}.csv`);

  writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "candidate-cleanup-plan",
    ...result,
    reportFiles: { json: jsonPath, csv: csvPath },
  });
  writeCsv(
    csvPath,
    result.planRows.map(cleanupPlanRowToCsv),
    CLEANUP_PLAN_CSV_COLUMNS
  );

  printSummary(result, jsonPath, csvPath);

  console.log(
    "\n✓ Report only. Export raw OSM to /data/independent-census/raw/ before any archive."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
