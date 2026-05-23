/**
 * Parent-company batch validation — end-to-end dry-run or gated apply.
 *
 * Default: dry-run (local reports only).
 * Evidence: --apply --source-policy-approved
 * Verified promotion: --apply --approved-by [--include-review-before-promote]
 */
import "../load-env.js";
import { join } from "path";
import {
  runParentCompanyValidationBatch,
  validationRowToCsv,
  VALIDATION_CSV_COLUMNS,
} from "../lib/independent-census/parent-company-validation-batch.js";
import {
  isIndependentCensusPipelineEnabled,
} from "../lib/independent-census/platform-base.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

function parseArgs() {
  let parentCompany = "";
  let batchId = "";
  let approvedBy = "";
  let approvalNote = "";
  let candidateRetentionReport = "";
  let propertyUrlReport = "";
  let reconciliationReport = "";
  let collisionReport = "";
  let brandSeeds = "";
  let candidateSourceType = "osm";
  let maxPromotions = null;
  let apply = false;
  let sourcePolicyApproved = false;
  let includeReviewBeforePromote = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1])
      batchId = argv[++i].replace(/^"|"$/g, "");
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
    else if (a === "--candidate-retention-report" && argv[i + 1])
      candidateRetentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      candidateRetentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--property-url-report" && argv[i + 1])
      propertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-url-report="))
      propertyUrlReport = a.slice("--property-url-report=".length).replace(/^"|"$/g, "");
    else if (a === "--reconciliation-report" && argv[i + 1])
      reconciliationReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--reconciliation-report="))
      reconciliationReport = a
        .slice("--reconciliation-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--collision-report" && argv[i + 1])
      collisionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--collision-report="))
      collisionReport = a.slice("--collision-report=".length).replace(/^"|"$/g, "");
    else if (a === "--brand-seeds" && argv[i + 1])
      brandSeeds = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--brand-seeds="))
      brandSeeds = a.slice("--brand-seeds=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-source-type" && argv[i + 1])
      candidateSourceType = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--candidate-source-type="))
      candidateSourceType = a
        .slice("--candidate-source-type=".length)
        .replace(/^"|"$/g, "")
        .toLowerCase();
    else if (a === "--max-promotions" && argv[i + 1])
      maxPromotions = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-promotions="))
      maxPromotions = parseInt(a.slice("--max-promotions=".length), 10);
    else if (a === "--source-policy-approved") sourcePolicyApproved = true;
    else if (a === "--include-review-before-promote")
      includeReviewBeforePromote = true;
    else if (a === "--apply") apply = true;
    else if (a === "--dry-run") apply = false;
  }

  if (!parentCompany) throw new Error("Required: --parent-company");
  if (!batchId) throw new Error("Required: --batch-id");

  if (apply && (sourcePolicyApproved || approvedBy)) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error(
        "Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true"
      );
    }
  }

  if (apply && approvedBy && !includeReviewBeforePromote) {
    console.warn(
      "Note: --approved-by without --include-review-before-promote only promotes rows with promote_after_review (rare for corrected Choice evidence)."
    );
  }

  return {
    parentCompany,
    batchId,
    approvedBy,
    approvalNote,
    candidateRetentionReport,
    propertyUrlReport,
    reconciliationReport,
    collisionReport,
    brandSeeds,
    candidateSourceType,
    maxPromotions,
    apply,
    sourcePolicyApproved,
    includeReviewBeforePromote,
    projectRoot: process.cwd(),
  };
}

function printConsoleSummary(result) {
  console.log("\n--- Parent-company validation summary ---");
  console.log(`Parent company:              ${result.parentCompany}`);
  console.log(`Batch ID:                    ${result.batchId}`);
  console.log(`Mode:                        ${result.mode}`);
  console.log(`Brand Setup seeds matched:   ${result.brandSeeds.brandsMatched}`);
  console.log(`Property URL leads (CALA):   ${result.propertyLeadCount}`);
  console.log(`Reconciliation source:       ${result.reconciliationSource}`);
  console.log(`Direct property matches:     ${result.directMatchCount}`);
  console.log(`Linked OSM candidates:       ${result.linkedCandidatesReviewed}`);
  console.log(`Corrected evidence existing: ${result.evidenceRowsExisting}`);
  console.log(`Evidence would create:       ${result.evidenceRowsWouldCreate}`);
  if (result.apply && result.sourcePolicyApproved) {
    console.log(`Evidence newly created:      ${result.evidenceWrittenCount}`);
    console.log(`Evidence duplicate skipped:  ${result.evidenceSkippedDuplicate}`);
  }
  console.log(`ready_for_human_approval:    ${result.bucketCounts.ready_for_human_approval}`);
  console.log(`needs_enrichment:            ${result.bucketCounts.needs_enrichment}`);
  console.log(
    `duplicate_or_collision_review: ${result.bucketCounts.duplicate_or_collision_review}`
  );
  console.log(`hold_low_priority:           ${result.bucketCounts.hold_low_priority}`);
  if (result.apply && result.approvedBy) {
    console.log(`Promoted to Verified:        ${result.promotedCount}`);
    console.log(`Promotion duplicate skip:  ${result.promotionSkippedDuplicate}`);
  }
  console.log("\nReport files:");
  console.log(`  ${result.reportFiles.json}`);
  console.log(`  ${result.reportFiles.csv}`);
}

async function main() {
  const args = parseArgs();

  console.log("=== Parent-company batch validation ===\n");
  console.log(`Parent company:  ${args.parentCompany}`);
  console.log(`Batch ID:        ${args.batchId}`);
  console.log(`Mode:            ${args.apply ? "APPLY" : "DRY-RUN"}`);
  if (args.propertyUrlReport) {
    console.log(`Property URLs:   ${args.propertyUrlReport}`);
  }
  if (args.candidateRetentionReport) {
    console.log(`Retention:       ${args.candidateRetentionReport}`);
  }
  console.log("Prior 4Q collision evidence: ignored for promotion buckets");
  console.log("No property HTML fetch. No STR/CoStar.\n");

  const result = await runParentCompanyValidationBatch({
    parentCompany: args.parentCompany,
    batchId: args.batchId,
    approvedBy: args.approvedBy,
    approvalNote: args.approvalNote,
    propertyUrlReportPath: args.propertyUrlReport,
    candidateRetentionReportPath: args.candidateRetentionReport,
    reconciliationReportPath: args.reconciliationReport,
    collisionReportPath: args.collisionReport,
    brandSeedsPath: args.brandSeeds ? join(process.cwd(), args.brandSeeds) : "",
    candidateSourceType: args.candidateSourceType,
    maxPromotions: args.maxPromotions,
    apply: args.apply,
    sourcePolicyApproved: args.sourcePolicyApproved,
    includeReviewBeforePromote: args.includeReviewBeforePromote,
    projectRoot: args.projectRoot,
  });

  writeJson(result.reportFiles.json, {
    ...result,
    validationRows: result.validationRows,
  });
  writeCsv(
    result.reportFiles.csv,
    result.validationRows.map(validationRowToCsv),
    VALIDATION_CSV_COLUMNS
  );

  printConsoleSummary(result);

  const ready = result.validationRows.filter(
    (r) => r.validationBucket === "ready_for_human_approval"
  );
  if (ready.length) {
    console.log("\n--- ready_for_human_approval ---");
    for (const r of ready) {
      console.log(
        `  ${r.choicePropertyId} | ${r.candidateHotelName} | ${r.candidateAirtableRecordId} | score ${r.matchScore}`
      );
    }
  }

  console.log(
    "\n✓ Hotel Census, Brand Setup, Brand Alias untouched. No auto-promotion of collision rows."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
