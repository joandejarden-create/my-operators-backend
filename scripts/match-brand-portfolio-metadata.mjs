/**
 * Match official brand/portfolio metadata to Verified, Candidates, Choice targets.
 */
import "../load-env.js";
import { join } from "path";
import {
  runBrandPortfolioMetadataMatch,
  MATCH_CSV_COLUMNS,
} from "../lib/independent-census/match-brand-portfolio-metadata.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let metadataExtractReport = "";
  let choiceTargetList =
    "reports/independent-census-choice-target-list-choice-targets-2026-05-20.json";
  let retentionReport =
    "reports/independent-census-candidate-coverage-dedupe-2026-05-20.json";
  let batchId = "choice-metadata-match-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--metadata-extract-report" && argv[i + 1])
      metadataExtractReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--metadata-extract-report="))
      metadataExtractReport = a
        .slice("--metadata-extract-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--choice-target-list" && argv[i + 1])
      choiceTargetList = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--choice-target-list="))
      choiceTargetList = a.slice("--choice-target-list=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-retention-report" && argv[i + 1])
      retentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      retentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  if (!metadataExtractReport) throw new Error("Required: --metadata-extract-report");

  return {
    metadataExtractReportPath: join(process.cwd(), metadataExtractReport),
    choiceTargetListPath: join(process.cwd(), choiceTargetList),
    retentionReportPath: join(process.cwd(), retentionReport),
    batchId,
  };
}

async function main() {
  const args = parseArgs();
  const result = await runBrandPortfolioMetadataMatch(args);

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-brand-portfolio-metadata-match-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-brand-portfolio-metadata-match-${args.batchId}.csv`
  );

  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "brand-portfolio-metadata-match",
    ...result,
  });
  await writeCsv(csvPath, result.matchRows, MATCH_CSV_COLUMNS);

  console.log("--- Match summary ---");
  console.log(`Extracted rows:     ${result.extractedRowsMatched}`);
  console.log(`Choice targets:     ${result.choiceTargetCount}`);
  console.log("Buckets:", result.bucketCounts);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
