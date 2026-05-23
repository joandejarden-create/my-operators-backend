/**
 * Promote to Verified Independent Hotel Census.
 *
 * Sources: --review-report (Phase 4C/4W) or --apply-plan (balanced backwards-match).
 * Default: dry-run. Requires --apply, --approved-by, INDEPENDENT_CENSUS_PIPELINE_ENABLED=true.
 * Does NOT write Hotel Census, Brand Alias, or update Candidates/Evidence.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { VERIFIED_TABLE, VERIFIED_FIELDS } from "../lib/independent-census/fields.js";
import {
  getIndependentCensusBase,
  isIndependentCensusPipelineEnabled,
} from "../lib/independent-census/platform-base.js";
import {
  selectPromotableReviewRows,
  loadExistingVerifiedDedupeIndex,
  filterDuplicates,
  createVerifiedRecords,
} from "../lib/independent-census/promote-verified.js";
import {
  runPromoteFromApplyPlan,
  promoteApplyPlanRowToCsv,
  PROMOTE_APPLY_PLAN_CSV_COLUMNS,
} from "../lib/independent-census/promote-from-apply-plan.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let reviewReport = "";
  let applyPlan = "";
  let batchId = "";
  let approvedBy = "";
  let approvalNote = "";
  let candidateRecordId = "";
  let maxRecords = null;
  let apply = false;
  let allowReviewBeforePromote = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--review-report" && argv[i + 1])
      reviewReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--review-report="))
      reviewReport = a.slice("--review-report=".length).replace(/^"|"$/g, "");
    else if (a === "--apply-plan" && argv[i + 1])
      applyPlan = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--apply-plan="))
      applyPlan = a.slice("--apply-plan=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--approved-by" && argv[i + 1])
      approvedBy = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--approved-by="))
      approvedBy = a.slice("--approved-by=".length).replace(/^"|"$/g, "");
    else if (a === "--approval-note" && argv[i + 1])
      approvalNote = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--approval-note="))
      approvalNote = a.slice("--approval-note=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-record-id" && argv[i + 1])
      candidateRecordId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-record-id="))
      candidateRecordId = a
        .slice("--candidate-record-id=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--max-records" && argv[i + 1])
      maxRecords = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-records="))
      maxRecords = parseInt(a.slice("--max-records=".length), 10);
    else if (a === "--allow-review-before-promote") allowReviewBeforePromote = true;
    else if (a === "--apply") apply = true;
  }

  if (!reviewReport && !applyPlan) {
    throw new Error("Provide --apply-plan or --review-report");
  }
  if (reviewReport && applyPlan) {
    throw new Error("Use only one of --apply-plan or --review-report");
  }
  if (!batchId) throw new Error("Missing --batch-id");
  if (allowReviewBeforePromote && !candidateRecordId) {
    throw new Error(
      "--candidate-record-id is required when using --allow-review-before-promote"
    );
  }
  if (apply && !approvedBy) {
    throw new Error("--approved-by is required when using --apply");
  }

  return {
    reviewReportPath: reviewReport ? join(process.cwd(), reviewReport) : "",
    applyPlanPath: applyPlan ? join(process.cwd(), applyPlan) : "",
    batchId,
    approvedBy,
    approvalNote,
    candidateRecordId,
    allowReviewBeforePromote,
    maxRecords,
    apply,
    useApplyPlan: !!applyPlan,
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

async function runApplyPlanPromotion(args) {
  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-verified-promote-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-verified-promote-${args.batchId}.csv`
  );

  console.log("=== Verified promotion (balanced apply plan) ===\n");
  console.log(`Mode:           ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Apply plan:     ${args.applyPlanPath}`);
  console.log(`Promote batch:  ${args.batchId}`);
  console.log(`Approved by:    ${args.approvedBy || "(dry-run)"}\n`);

  if (args.apply && !isIndependentCensusPipelineEnabled()) {
    throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
  }

  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const result = await runPromoteFromApplyPlan({
    applyPlanPath: args.applyPlanPath,
    batchId: args.batchId,
    approvedBy: args.approvedBy,
    apply: args.apply,
    base,
  });

  console.log(`Apply plan rows:          ${result.applyPlanCount}`);
  console.log(`Selected for promotion:   ${result.selectedForPromotion}`);
  console.log(`Would write / written:    ${args.apply ? result.writtenCount : result.wouldWriteCount}`);
  console.log(`Skipped duplicate:        ${result.skippedDuplicateCount}`);
  console.log(`Skipped missing fields:   ${result.skippedMissingRequired}`);

  if (result.verifiedIndexMeta?.success) {
    console.log(`\nVerified index (pre-apply): ${result.verifiedIndexMeta.verifiedRecordsLoaded} records`);
    if (args.apply) {
      console.log(
        `Verified index (est. post): ${result.verifiedRecordsAfterApplyEstimate}`
      );
    }
  }

  console.log("\nBy country:");
  for (const [co, count] of Object.entries(result.writtenByCountry).sort(
    (a, b) => b[1] - a[1]
  )) {
    console.log(`  ${co}: ${count}`);
  }

  const status = args.apply ? "written" : "would_write";
  const csvRows = [
    ...result.toWritePreview.map((r) => promoteApplyPlanRowToCsv(r, status)),
    ...result.skippedDuplicate.map((s) =>
      promoteApplyPlanRowToCsv(
        {
          candidateAirtableRecordId: s.planRow?.candidateRecordId,
          verifiedHotelName: s.planRow?.osmName,
          verifiedDedupeKey: s.key || s.planRow?.verifiedDedupeKey,
          osmCountry: s.planRow?.osmCountry,
          matchedLegacyRecordId: s.planRow?.matchedLegacyRecordId,
          matchScore: s.planRow?.matchScore,
        },
        `skipped_${s.reason}`
      )
    ),
  ];

  writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "balanced-global-verified-apply",
    ...result,
    reportFiles: { json: jsonPath, csv: csvPath },
  });
  writeCsv(csvPath, csvRows, PROMOTE_APPLY_PLAN_CSV_COLUMNS);

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log("\n✓ Verified table only. Hotel Census, Candidates, Evidence untouched.");

  if (!args.apply && result.wouldWriteCount !== result.applyPlanCount) {
    console.warn(
      `\n⚠ Would-write (${result.wouldWriteCount}) differs from plan (${result.applyPlanCount}). Review skipped duplicates.`
    );
  }
}

async function runReviewReportPromotion(args) {
  const reviewData = loadReviewReport(args.reviewReportPath);
  const approvedAt = new Date().toISOString();

  const selectOptions = {
    maxRecords: args.maxRecords,
    allowReviewBeforePromote: args.allowReviewBeforePromote,
    candidateRecordId: args.candidateRecordId,
    requirePropertyIdMatchOnOsmWebsite: args.allowReviewBeforePromote,
  };

  const { selected, skipped } = selectPromotableReviewRows(
    reviewData.reviewRows,
    selectOptions
  );

  const prepared = selected.map((row) => ({
    ...row,
    _approvedBy: args.approvedBy || "(dry-run)",
    _batchId: args.batchId,
    _approvedAt: approvedAt,
    _approvalNote: args.approvalNote,
  }));

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-verified-promote-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-verified-promote-${args.batchId}.csv`
  );

  const phaseLabel = args.allowReviewBeforePromote
    ? "4W-verified-promote"
    : "4C-verified-promote";

  console.log(
    `=== Verified Independent Hotel Census promotion (${phaseLabel}) ===\n`
  );
  console.log(`Mode:           ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Review report:  ${args.reviewReportPath}`);
  console.log(`Promote batch:  ${args.batchId}`);
  console.log(`Approved by:    ${args.approvedBy || "(not set — dry-run)"}`);
  if (args.candidateRecordId) {
    console.log(`Candidate filter: ${args.candidateRecordId}`);
  }
  if (args.allowReviewBeforePromote) {
    console.log(`Override:       allow review_before_promote (Phase 4W)`);
  }
  if (args.approvalNote) {
    const preview =
      args.approvalNote.length > 80
        ? `${args.approvalNote.slice(0, 80)}…`
        : args.approvalNote;
    console.log(`Approval note:  ${preview}`);
  }
  console.log(`Max records:    ${args.maxRecords ?? "all"}\n`);

  console.log(`Review rows total:        ${reviewData.reviewRows.length}`);
  console.log(`Selected for promotion:   ${selected.length}`);
  console.log(`Skipped not promotable:   ${skipped.notPromoteAfterReview.length}`);
  console.log(`Skipped not eligible_for_review: ${skipped.notEligibleForReview.length}`);
  console.log(`Skipped candidate mismatch: ${skipped.candidateRecordMismatch.length}`);
  console.log(`Skipped property ID mismatch: ${skipped.propertyIdMismatch.length}`);
  console.log(`Skipped missing required:  ${skipped.missingRequiredFields.length}`);
  console.log(`Skipped max-records cap:   ${skipped.overMaxRecords.length}`);

  let duplicateSkipped = [];
  let written = [];
  let writtenCount = 0;
  let toWritePreview = [];

  if (args.apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
    }
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    console.log("\nLoading existing Verified records for dedupe…");
    const index = await loadExistingVerifiedDedupeIndex(base, VERIFIED_TABLE);
    console.log(`  Existing dedupe keys: ${index.dedupeKeys.size}`);
    console.log(`  Existing candidate links: ${index.candidateLinks.size}`);

    const { toWrite, skippedDuplicate } = filterDuplicates(prepared, index);
    duplicateSkipped = skippedDuplicate;
    toWritePreview = toWrite;

    console.log(`  After dedupe, to write: ${toWrite.length}`);
    console.log(`  Skipped duplicate: ${duplicateSkipped.length}`);

    console.log("\nWriting to Verified Independent Hotel Census only…");
    const result = await createVerifiedRecords(
      base,
      VERIFIED_TABLE,
      toWrite,
      index.dedupeKeys
    );
    written = result.created;
    writtenCount = result.writtenCount;
    console.log(`  Written: ${writtenCount}`);
  } else {
    const { loadVerifiedIndexWithPolicy } = await import(
      "../lib/independent-census/verified-dedupe-index.js"
    );
    const loaded = await loadVerifiedIndexWithPolicy(getIndependentCensusBase(), {
      apply: false,
      allowMissingVerifiedIndex: false,
    });
    const { toWrite, skippedDuplicate } = filterDuplicates(prepared, loaded.index);
    duplicateSkipped = skippedDuplicate;
    toWritePreview = toWrite;
    console.log(`\nWould write after dedupe: ${toWrite.length}`);
    console.log(`Would skip duplicate: ${duplicateSkipped.length}`);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: phaseLabel,
    mode: args.apply ? "apply" : "dry-run",
    promotionBatchId: args.batchId,
    reviewReportPath: args.reviewReportPath,
    evidenceBatchId: reviewData.evidenceBatchId,
    candidateBatchId: reviewData.candidateBatchId,
    approvedBy: args.approvedBy || null,
    approvedAt: args.apply ? approvedAt : null,
    approvalNote: args.approvalNote || null,
    candidateRecordId: args.candidateRecordId || null,
    allowReviewBeforePromote: args.allowReviewBeforePromote,
    counts: {
      reviewRowsTotal: reviewData.reviewRows.length,
      selectedForPromotion: selected.length,
      skippedNotPromotable: skipped.notPromoteAfterReview.length,
      skippedNotEligible: skipped.notEligibleForReview.length,
      skippedCandidateMismatch: skipped.candidateRecordMismatch.length,
      skippedPropertyIdMismatch: skipped.propertyIdMismatch.length,
      skippedMissingRequired: skipped.missingRequiredFields.length,
      skippedMaxRecords: skipped.overMaxRecords.length,
      skippedDuplicate: duplicateSkipped.length,
      written: writtenCount,
    },
    dryRun: !args.apply,
    airtableWrites: args.apply,
    tablesWritten: args.apply ? [VERIFIED_TABLE] : [],
    hotelCensusWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    writtenRecords: written,
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  const csvRows = toWritePreview.map((r) => ({
    verifiedHotelName: r.verifiedHotelName,
    verifiedDedupeKey: r.verifiedDedupeKey,
    choicePropertyId: r.choicePropertyId || "",
    wikidataQid: r.wikidataQid || "",
    candidateAirtableRecordId: r.candidateAirtableRecordId,
    wouldWrite: args.apply ? "written" : "dry-run",
  }));

  writeJson(jsonPath, report);
  writeCsv(csvPath, csvRows, [
    "verifiedHotelName",
    "verifiedDedupeKey",
    "choicePropertyId",
    "wikidataQid",
    "candidateAirtableRecordId",
    "wouldWrite",
  ]);

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log("\n✓ Hotel Census untouched. Candidates/Evidence not modified.");
}

async function main() {
  const args = parseArgs();
  if (args.useApplyPlan) {
    await runApplyPlanPromotion(args);
  } else {
    await runReviewReportPromotion(args);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
