/**
 * Export export_to_raw_store_then_archive candidates to local raw store (no Airtable archive).
 */
import "../load-env.js";
import { join } from "path";
import {
  runCandidateRawStoreExport,
  rawStoreRowToCsv,
  RAW_STORE_CSV_COLUMNS,
  ensureParentDir,
  defaultRawStorePaths,
} from "../lib/independent-census/candidate-raw-store-export.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

function parseArgs() {
  let cleanupPlan =
    "reports/independent-census-candidate-cleanup-plan-2026-05-20.json";
  let coverageReport =
    "reports/independent-census-candidate-coverage-dedupe-2026-05-20.json";
  let dateSlug = "2026-05-20";
  let batchId = "candidate-raw-store-export-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--cleanup-plan" && argv[i + 1])
      cleanupPlan = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--cleanup-plan="))
      cleanupPlan = a.slice("--cleanup-plan=".length).replace(/^"|"$/g, "");
    else if (a === "--coverage-report" && argv[i + 1])
      coverageReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--coverage-report="))
      coverageReport = a
        .slice("--coverage-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--date-slug" && argv[i + 1]) dateSlug = argv[++i];
    else if (a.startsWith("--date-slug="))
      dateSlug = a.slice("--date-slug=".length);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i];
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length);
  }

  const root = process.cwd();
  return {
    cleanupPlanPath: join(root, cleanupPlan),
    coverageReportPath: join(root, coverageReport),
    dateSlug,
    batchId,
    paths: defaultRawStorePaths(root, dateSlug),
  };
}

async function main() {
  const args = parseArgs();
  console.log("=== Candidate raw-store export (local files only) ===\n");
  console.log(`Cleanup plan:    ${args.cleanupPlanPath}`);
  console.log(`Coverage report: ${args.coverageReportPath}\n`);

  const result = runCandidateRawStoreExport({
    cleanupPlanPath: args.cleanupPlanPath,
    coverageReportPath: args.coverageReportPath,
    batchId: args.batchId,
  });

  const { json: jsonOut, csv: csvOut, reportJson } = args.paths;
  ensureParentDir(jsonOut);
  ensureParentDir(csvOut);
  ensureParentDir(reportJson);

  writeJson(jsonOut, {
    generatedAt: new Date().toISOString(),
    phase: "candidate-raw-store-export",
    exportFormat: "airtable-candidate-snapshot",
    safeFieldsOnly: true,
    ...result,
    dataFiles: { json: jsonOut, csv: csvOut },
  });
  writeCsv(csvOut, result.exportRows.map(rawStoreRowToCsv), RAW_STORE_CSV_COLUMNS);

  const { exportRows, ...summary } = result;
  writeJson(reportJson, {
    generatedAt: new Date().toISOString(),
    phase: "candidate-raw-store-export-summary",
    ...summary,
    reportFiles: { summary: reportJson, dataJson: jsonOut, dataCsv: csvOut },
  });

  console.log(`Export target (plan):  ${result.exportTargetCount}`);
  console.log(`Rows written:          ${result.exportRowCount}`);
  console.log(`Missing coverage:      ${result.missingCoverageRows}`);
  console.log("\nBy country (top):");
  for (const [co, count] of Object.entries(result.byCountry)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)) {
    console.log(`  ${co}: ${count}`);
  }
  console.log(`\nData JSON: ${jsonOut}`);
  console.log(`Data CSV:  ${csvOut}`);
  console.log(`Report:    ${reportJson}`);
  console.log("\n✓ No Airtable archive/delete. Export confirmation required before archive.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
