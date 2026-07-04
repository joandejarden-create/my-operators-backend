/**
 * Phase Choice-D — Promotion review buckets for Choice OSM recovery (report-only).
 */
import "../load-env.js";
import { join } from "path";
import {
  runChoicePromotionReview,
  CHOICE_PROMOTION_REVIEW_CSV_COLUMNS,
} from "../lib/independent-census/choice-promotion-review.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let targetMatchReport = "";
  let targetedLookupReport = "";
  let choicePropertyUrlReport = "";
  let batchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--target-match-report" && argv[i + 1])
      targetMatchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--target-match-report="))
      targetMatchReport = a.slice("--target-match-report=".length).replace(/^"|"$/g, "");
    else if (a === "--targeted-lookup-report" && argv[i + 1])
      targetedLookupReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--targeted-lookup-report="))
      targetedLookupReport = a
        .slice("--targeted-lookup-report=".length)
        .replace(/^"|"$/g, "");
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

  if (!targetMatchReport) throw new Error("Required: --target-match-report");
  if (!batchId) throw new Error("Required: --batch-id");

  return {
    targetMatchReportPath: join(process.cwd(), targetMatchReport),
    targetedLookupReportPath: targetedLookupReport
      ? join(process.cwd(), targetedLookupReport)
      : "",
    choicePropertyUrlReportPath: choicePropertyUrlReport
      ? join(process.cwd(), choicePropertyUrlReport)
      : "",
    batchId,
  };
}

async function main() {
  const args = parseArgs();
  const result = await runChoicePromotionReview(args);

  const base = `independent-census-choice-promotion-review-${args.batchId}`;
  const jsonPath = join(REPORTS_DIR, `${base}.json`);
  const csvPath = join(REPORTS_DIR, `${base}.csv`);

  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "Choice-D-promotion-review",
    ...result,
  });
  await writeCsv(csvPath, result.reviewRows, CHOICE_PROMOTION_REVIEW_CSV_COLUMNS);

  console.log("\n--- Choice promotion review ---");
  console.log(`Choice targets:            ${result.choiceTargetCount}`);
  console.log(`Verified-ready:            ${result.verifiedReadyCount}`);
  console.log("Buckets:", result.bucketCounts);
  console.log(`Sitemap URLs indexed:      ${result.sitemapUrlsIndexed}`);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
