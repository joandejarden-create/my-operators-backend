/**
 * Phase Choice-B — Match Choice targets to OSM candidates (retention JSON, read-only).
 */
import "../load-env.js";
import { join } from "path";
import {
  runChoiceTargetOsmMatch,
  choiceOsmMatchRowToCsv,
  CHOICE_OSM_MATCH_CSV_COLUMNS,
} from "../lib/independent-census/match-choice-targets-to-osm.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let targetList = "";
  let candidateRetentionReport = "";
  let batchId = "";
  let includeRetention = "";
  let excludeRetention = "";
  let includeDuplicateReview = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--target-list" && argv[i + 1])
      targetList = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--target-list="))
      targetList = a.slice("--target-list=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-retention-report" && argv[i + 1])
      candidateRetentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      candidateRetentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--include-retention" && argv[i + 1])
      includeRetention = argv[++i];
    else if (a.startsWith("--include-retention="))
      includeRetention = a.slice("--include-retention=".length);
    else if (a === "--exclude-retention" && argv[i + 1])
      excludeRetention = argv[++i];
    else if (a.startsWith("--exclude-retention="))
      excludeRetention = a.slice("--exclude-retention=".length);
    else if (a === "--include-duplicate-review") includeDuplicateReview = true;
  }

  if (!targetList) throw new Error("Required: --target-list");
  if (!candidateRetentionReport) {
    throw new Error("Required: --candidate-retention-report");
  }
  if (!batchId) throw new Error("Required: --batch-id");

  return {
    targetListPath: join(process.cwd(), targetList),
    retentionReportPath: join(process.cwd(), candidateRetentionReport),
    batchId,
    includeRetention: includeRetention || undefined,
    excludeRetention: excludeRetention || undefined,
    includeDuplicateReview,
  };
}

function printSummary(result, jsonPath, csvPath) {
  const c = result.confidenceCounts;
  console.log("\n--- Choice target ↔ OSM match ---");
  console.log(`Choice targets:            ${result.choiceTargetCount}`);
  console.log(`OSM candidates loaded:     ${result.osmCandidatesLoaded}`);
  console.log(`High / medium / low / none: ${c.high || 0} / ${c.medium || 0} / ${c.low || 0} / ${c.none || 0}`);
  console.log(`No OSM match:              ${result.noOsmMatchCount}`);
  console.log(`Already verified:          ${result.alreadyVerifiedCount}`);
  console.log(`Duplicate risk:            ${result.duplicateRiskCount}`);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
}

async function main() {
  const args = parseArgs();
  const result = await runChoiceTargetOsmMatch(args);

  const base = `independent-census-choice-target-osm-match-${args.batchId}`;
  const jsonPath = join(REPORTS_DIR, `${base}.json`);
  const csvPath = join(REPORTS_DIR, `${base}.csv`);

  const csvRows = result.matchRows.map(choiceOsmMatchRowToCsv);
  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "Choice-B-osm-match",
    ...result,
  });
  await writeCsv(csvPath, csvRows, CHOICE_OSM_MATCH_CSV_COLUMNS);

  printSummary(result, jsonPath, csvPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
