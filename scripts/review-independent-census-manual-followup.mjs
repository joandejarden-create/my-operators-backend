/**
 * Phase 4D — Manual follow-up report for review_before_promote rows (REPORT ONLY).
 *
 * No Airtable writes. No Verified promotion.
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  selectManualFollowupRows,
  buildManualFollowupRow,
  summarizeManualFollowup,
} from "../lib/independent-census/manual-followup-review.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

const CSV_COLUMNS = [
  "Candidate Airtable Record ID",
  "Candidate Hotel Name",
  "Wikidata Hotel Name",
  "Candidate City",
  "Candidate Country",
  "Candidate Latitude",
  "Candidate Longitude",
  "Candidate Website",
  "Wikidata Website",
  "Wikidata QID",
  "Match Score",
  "Match Reason",
  "Manual Review Reason",
  "Review Priority",
  "Suggested Next Action",
  "Human Notes",
  "Ready For Future Promotion",
];

function parseArgs() {
  let reviewReport = "";
  let batchId = "osm-wikidata-dr-manual-followup-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--review-report" && argv[i + 1])
      reviewReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--review-report="))
      reviewReport = a.slice("--review-report=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4D is report-only.");
    }
  }

  if (!reviewReport) {
    throw new Error("Missing --review-report");
  }

  return {
    reviewReportPath: join(process.cwd(), reviewReport),
    batchId,
  };
}

function loadReviewReport(path) {
  if (!existsSync(path)) throw new Error(`Review report not found: ${path}`);
  const data = JSON.parse(readFileSync(path, "utf8"));
  if (!Array.isArray(data.reviewRows)) {
    throw new Error("Invalid review report: missing reviewRows");
  }
  return data;
}

function toCsvRow(r) {
  return {
    "Candidate Airtable Record ID": r.candidateAirtableRecordId,
    "Candidate Hotel Name": r.candidateHotelName,
    "Wikidata Hotel Name": r.wikidataHotelName,
    "Candidate City": r.candidateCity,
    "Candidate Country": r.candidateCountry,
    "Candidate Latitude": r.candidateLatitude ?? "",
    "Candidate Longitude": r.candidateLongitude ?? "",
    "Candidate Website": r.candidateWebsite,
    "Wikidata Website": r.wikidataWebsite,
    "Wikidata QID": r.wikidataQid,
    "Match Score": r.matchScore,
    "Match Reason": r.matchReason,
    "Manual Review Reason": r.manualReviewReason,
    "Review Priority": r.reviewPriority,
    "Suggested Next Action": r.suggestedNextAction,
    "Human Notes": r.humanNotes,
    "Ready For Future Promotion": r.readyForFuturePromotion,
  };
}

async function main() {
  const { reviewReportPath, batchId } = parseArgs();
  const reviewData = loadReviewReport(reviewReportPath);

  const { selected, skipped } = selectManualFollowupRows(reviewData.reviewRows);
  const followupRows = selected.map(buildManualFollowupRow);
  const summary = summarizeManualFollowup(followupRows);

  const reportSlug = batchId.replace(/-manual-followup(?=-|$)/, "");
  const jsonPath = join(REPORTS_DIR, `independent-census-manual-followup-${reportSlug}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-manual-followup-${reportSlug}.csv`);

  console.log("=== Independent census manual follow-up (Phase 4D, report-only) ===\n");
  console.log(`Review report: ${reviewReportPath}`);
  console.log(`Follow-up batch: ${batchId}`);
  console.log(`Phase 4B rows total: ${reviewData.reviewRows.length}`);
  console.log(`Skipped (not review_before_promote + needs_manual_research): ${skipped.length}`);
  console.log(`Manual follow-up rows: ${followupRows.length}\n`);

  console.log("--- Review priority ---");
  console.log(`  high:   ${summary.priorityHigh}`);
  console.log(`  medium: ${summary.priorityMedium}`);
  console.log(`  low:    ${summary.priorityLow}`);

  console.log("\n--- Manual review reason (primary) ---");
  console.log(JSON.stringify(summary.byPrimaryManualReviewReason, null, 2));
  console.log("\n--- Manual review reason (all flags) ---");
  console.log(JSON.stringify(summary.byAllManualReviewReasons, null, 2));
  console.log("\n--- Suggested next action ---");
  console.log(JSON.stringify(summary.bySuggestedNextAction, null, 2));

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4D-manual-followup",
    batchId,
    reviewReportPath,
    evidenceBatchId: reviewData.evidenceBatchId,
    candidateBatchId: reviewData.candidateBatchId,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    skippedRows: skipped.length,
    summary,
    reportFiles: { json: jsonPath, csv: csvPath },
    followupRows,
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, followupRows.map(toCsvRow), CSV_COLUMNS);

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ No Airtable writes. Hotel Census and Verified Independent Hotel Census untouched."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
