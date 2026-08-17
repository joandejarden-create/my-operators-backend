#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  RECONCILIATION_JSON_NAME,
  RECONCILIATION_MD_NAME,
  REMAINING_PLAN_JSON_NAME,
  REMAINING_PLAN_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerSlotCompletionReconciliationMarkdown,
  buildBrandExplorerSlotCompletionRemainingPlanMarkdown,
  buildBrandExplorerSlotCompletionWriterMarkdown,
  buildBrandExplorerSlotCompletionWriterReport,
} from "../lib/partner-intelligence/brand-explorer-slot-completion-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);
const RECONCILIATION_JSON = join(ROOT, "reports", RECONCILIATION_JSON_NAME);
const RECONCILIATION_MD = join(ROOT, "reports", RECONCILIATION_MD_NAME);
const REMAINING_PLAN_JSON = join(ROOT, "reports", REMAINING_PLAN_JSON_NAME);
const REMAINING_PLAN_MD = join(ROOT, "reports", REMAINING_PLAN_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const apply = hasFlag("--apply");
  const applyApproved = hasFlag("--approve-brand-explorer-slot-completion-v20B");
  const dryRun = hasFlag("--dry-run") || !apply;

  const report = await buildBrandExplorerSlotCompletionWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    applyApproved,
  });

  const markdown = buildBrandExplorerSlotCompletionWriterMarkdown(report);
  const reconciliationMarkdown = buildBrandExplorerSlotCompletionReconciliationMarkdown(report);
  const remainingPlanMarkdown = buildBrandExplorerSlotCompletionRemainingPlanMarkdown(report.remainingSlotPlan);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");
  writeFileSync(RECONCILIATION_JSON, JSON.stringify({
    generatedAt: report.generatedAt,
    brand: report.brand,
    matchedSlotCount: report.matchedSlotCount,
    trueWouldUpdateCount: report.trueWouldUpdateCount,
    wouldCreateCount: report.rowsWouldCreate.length,
    unexpectedDifferenceCount: report.unexpectedDifferenceCount,
    v20BIdempotent: report.v20BIdempotent,
    postApplyReconciliation: report.postApplyReconciliation,
    rowsUnexpectedDifference: report.rowsUnexpectedDifference,
  }, null, 2), "utf8");
  writeFileSync(RECONCILIATION_MD, reconciliationMarkdown, "utf8");
  writeFileSync(REMAINING_PLAN_JSON, JSON.stringify(report.remainingSlotPlan, null, 2), "utf8");
  writeFileSync(REMAINING_PLAN_MD, remainingPlanMarkdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Wrote ${RECONCILIATION_MD}`);
  console.log(`Wrote ${RECONCILIATION_JSON}`);
  console.log(`Wrote ${REMAINING_PLAN_MD}`);
  console.log(`Wrote ${REMAINING_PLAN_JSON}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Slots targeted: ${report.slotsTargetedCount}`);
  console.log(`Would create: ${report.rowsWouldCreate.length}`);
  console.log(`Would update: ${report.trueWouldUpdateCount}`);
  console.log(`Matched (no-op): ${report.matchedSlotCount}`);
  console.log(`Unexpected differences: ${report.unexpectedDifferenceCount}`);
  console.log(`v20B idempotent: ${report.v20BIdempotent ? "yes" : "no"}`);
  console.log(`Projected score after apply: ${report.projectedScoreAfterApply}`);
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
