/**
 * Phase 4S — Choice brand-directory property ID collision review (READ-ONLY).
 *
 * No Airtable writes. Uses local JSON reports only.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  buildCollisionReview,
  indexPropertyUrlExtract,
  summarizeCollisionReview,
  collisionDetailToCsv,
  COLLISION_CSV_COLUMNS,
} from "../lib/independent-census/brand-directory-collision-review.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let promotionReview = "";
  let matchReport = "";
  let propertyUrlReport = "";
  let batchId = "choice-collision-review-2026-05-20";
  let reportSlug = "choice-collision-review-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--promotion-review" && argv[i + 1])
      promotionReview = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--promotion-review="))
      promotionReview = a.slice("--promotion-review=".length).replace(/^"|"$/g, "");
    else if (a === "--match-report" && argv[i + 1])
      matchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--match-report="))
      matchReport = a.slice("--match-report=".length).replace(/^"|"$/g, "");
    else if (a === "--property-url-report" && argv[i + 1])
      propertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-url-report="))
      propertyUrlReport = a
        .slice("--property-url-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--report-slug" && argv[i + 1])
      reportSlug = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--report-slug="))
      reportSlug = a.slice("--report-slug=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4S is report-only.");
    }
  }

  if (!promotionReview || !matchReport || !propertyUrlReport) {
    throw new Error(
      "Required: --promotion-review, --match-report, --property-url-report"
    );
  }

  return {
    promotionReviewPath: join(process.cwd(), promotionReview),
    matchReportPath: join(process.cwd(), matchReport),
    propertyUrlReportPath: join(process.cwd(), propertyUrlReport),
    batchId,
    reportSlug,
  };
}

function loadJson(path, label) {
  if (!existsSync(path)) throw new Error(`${label} not found: ${path}`);
  return JSON.parse(readFileSync(path, "utf8"));
}

async function main() {
  const args = parseArgs();
  const promotionReview = loadJson(args.promotionReviewPath, "Promotion review");
  const matchReport = loadJson(args.matchReportPath, "Match report");
  const propertyUrlReport = loadJson(
    args.propertyUrlReportPath,
    "Property URL extract"
  );

  const propertyUrlById = indexPropertyUrlExtract(propertyUrlReport);

  const { groups, detailRows } = buildCollisionReview({
    promotionReview,
    matchReport,
    propertyUrlById,
    batchId: args.batchId,
  });

  const summary = summarizeCollisionReview(groups, detailRows);

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-${args.reportSlug}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-${args.reportSlug}.csv`
  );

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4S-choice-collision-review",
    batchId: args.batchId,
    inputs: {
      promotionReviewPath: args.promotionReviewPath,
      matchReportPath: args.matchReportPath,
      propertyUrlReportPath: args.propertyUrlReportPath,
    },
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    verifiedTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    summary,
    collisionGroups: groups,
    detailRows,
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, detailRows.map(collisionDetailToCsv), COLLISION_CSV_COLUMNS);

  console.log("=== Choice brand-directory collision review (Phase 4S, read-only) ===\n");
  console.log(`Batch ID:           ${args.batchId}`);
  console.log(`Promotion review:   ${args.promotionReviewPath}`);
  console.log(`Match report:       ${args.matchReportPath}`);
  console.log(`Property URLs:      ${args.propertyUrlReportPath}\n`);

  console.log("--- Collision summary ---");
  console.log(`  Collision groups reviewed:     ${summary.collisionGroupsReviewed}`);
  console.log(
    `  Total Choice property IDs:     ${summary.totalChoicePropertyIdsInCollisions}`
  );
  console.log(
    `  select_single_choice_property_id: ${summary.select_single_choice_property_id}`
  );
  console.log(
    `  needs_manual_property_review:     ${summary.needs_manual_property_review}`
  );
  console.log(
    `  reject_collision_group:           ${summary.reject_collision_group}`
  );
  console.log(
    `  hold_for_property_page_metadata:  ${summary.hold_for_property_page_metadata}`
  );
  console.log(
    `  Ready for cleanup (yes):          ${summary.readyForEvidenceCleanupYes}`
  );
  console.log(
    `  Ready after manual confirm:       ${summary.readyForEvidenceCleanupAfterManual}`
  );

  if (groups.length) {
    console.log("\n--- Groups ---");
    for (const g of groups) {
      console.log(
        `  ${g.osmCandidateName} (${g.osmCandidateRecordId}): ${g.choicePropertyIdCount} property IDs → ${g.collisionRecommendation} → ${g.recommendedSelectedPropertyId || "(none)"}`
      );
      console.log(`    ${g.humanNotes}`);
    }
  }

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ No Airtable writes. Hotel Census, Brand Setup, Brand Alias, Candidates, Evidence, and Verified untouched."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
