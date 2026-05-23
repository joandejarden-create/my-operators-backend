/**
 * Choice no-match enrichment plan (report-only).
 */
import "../load-env.js";
import { join } from "path";
import {
  buildChoiceNoMatchEnrichmentPlan,
  NO_MATCH_ENRICHMENT_CSV_COLUMNS,
} from "../lib/independent-census/choice-no-match-enrichment-plan.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let promotionReview =
    "reports/independent-census-choice-promotion-review-choice-promotion-review-2026-05-20.json";
  let targetList =
    "reports/independent-census-choice-target-list-choice-targets-2026-05-20.json";
  let choicePropertyUrlReport =
    "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";
  let batchId = "choice-no-match-enrichment-plan-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--promotion-review" && argv[i + 1])
      promotionReview = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--promotion-review="))
      promotionReview = a.slice("--promotion-review=".length).replace(/^"|"$/g, "");
    else if (a === "--target-list" && argv[i + 1])
      targetList = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--target-list="))
      targetList = a.slice("--target-list=".length).replace(/^"|"$/g, "");
    else if (a === "--choice-property-url-report" && argv[i + 1])
      choicePropertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--choice-property-url-report="))
      choicePropertyUrlReport = a
        .slice("--choice-property-url-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  return {
    promotionReviewPath: join(process.cwd(), promotionReview),
    targetListPath: join(process.cwd(), targetList),
    choicePropertyUrlReportPath: join(process.cwd(), choicePropertyUrlReport),
    batchId,
  };
}

async function main() {
  const args = parseArgs();
  const result = buildChoiceNoMatchEnrichmentPlan(args);

  const base = `independent-census-${result.batchId}`;
  const jsonPath = join(REPORTS_DIR, `${base}.json`);
  const csvPath = join(REPORTS_DIR, `${base}.csv`);

  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "Choice-no-match-enrichment-plan",
    ...result,
  });
  await writeCsv(csvPath, result.planRows, NO_MATCH_ENRICHMENT_CSV_COLUMNS);

  console.log("\n--- Choice no-match enrichment plan ---");
  console.log(`No-match targets:        ${result.noMatchCount}`);
  console.log("Actions:", result.actionCounts);
  console.log("Top countries:", result.byCountry);
  console.log("Top brands:", result.byBrand);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
