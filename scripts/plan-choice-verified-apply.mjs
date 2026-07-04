/**
 * Choice Verified apply plan (report-only).
 */
import "../load-env.js";
import { join } from "path";
import {
  buildChoiceVerifiedApplyPlan,
  choiceApplyPlanRowToCsv,
  CHOICE_APPLY_PLAN_CSV_COLUMNS,
} from "../lib/independent-census/choice-verified-apply-plan.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let promotionReview =
    "reports/independent-census-choice-promotion-review-choice-promotion-review-2026-05-20.json";
  let targetMatchReport =
    "reports/independent-census-choice-target-osm-match-choice-target-osm-match-2026-05-20.json";
  let retentionReport =
    "reports/independent-census-candidate-coverage-dedupe-2026-05-20.json";
  let batchId = "choice-verified-apply-plan-001-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--promotion-review" && argv[i + 1])
      promotionReview = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--promotion-review="))
      promotionReview = a.slice("--promotion-review=".length).replace(/^"|"$/g, "");
    else if (a === "--target-match-report" && argv[i + 1])
      targetMatchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--target-match-report="))
      targetMatchReport = a.slice("--target-match-report=".length).replace(/^"|"$/g, "");
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

  return {
    promotionReviewPath: join(process.cwd(), promotionReview),
    targetMatchReportPath: join(process.cwd(), targetMatchReport),
    retentionReportPath: join(process.cwd(), retentionReport),
    applyPlanBatchId: batchId,
  };
}

async function main() {
  const args = parseArgs();
  const result = buildChoiceVerifiedApplyPlan(args);

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-${result.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-${result.batchId}.csv`
  );

  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "Choice-verified-apply-plan",
    ...result,
  });
  await writeCsv(
    csvPath,
    result.planRows.map(choiceApplyPlanRowToCsv),
    CHOICE_APPLY_PLAN_CSV_COLUMNS
  );

  console.log("\n--- Choice Verified apply plan ---");
  console.log(`Ready bucket in review:  ${result.readyBucketCount}`);
  console.log(`Apply plan rows:         ${result.applyPlanCount}`);
  console.log("Skipped:", result.skipped);
  console.log("By country:", result.byCountry);
  console.log("By brand:", result.byBrand);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
