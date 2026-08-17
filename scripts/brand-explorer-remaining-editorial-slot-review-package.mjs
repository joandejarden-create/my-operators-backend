#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRemainingEditorialSlotReviewPackageMarkdown,
  buildBrandExplorerRemainingEditorialSlotReviewPackageReport,
} from "../lib/partner-intelligence/brand-explorer-remaining-editorial-slot-review-package.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const report = await buildBrandExplorerRemainingEditorialSlotReviewPackageReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerRemainingEditorialSlotReviewPackageMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Original manual-review slots: ${report.originalManualReviewSlotCount}`);
  console.log(`Kept for editorial: ${report.slotsKeptForEditorialFounderReview.length}`);
  console.log(`Moved to evidence_required: ${report.slotsMovedToEvidenceRequired.length}`);
  console.log(`v21B writer safe: ${report.v21BWriterSafeToBuild ? "yes" : "no"}`);
  console.log(`v21B batch size: ${report.v21BApplyBatchCount}`);
  console.log(`Projected score after v21B: ${report.projectedSlotCoverageScoreAfterV21B}`);
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
