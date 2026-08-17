#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  REQUIRED_APPLY_FLAG,
  buildBrandExplorerRemainingEditorialSlotCompletionWriterMarkdown,
  buildBrandExplorerRemainingEditorialSlotCompletionWriterReport,
} from "../lib/partner-intelligence/brand-explorer-remaining-editorial-slot-completion-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

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
  const applyApproved = hasFlag(REQUIRED_APPLY_FLAG);
  const dryRun = hasFlag("--dry-run") || !apply;

  const report = await buildBrandExplorerRemainingEditorialSlotCompletionWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    applyApproved,
  });

  const markdown = buildBrandExplorerRemainingEditorialSlotCompletionWriterMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Slots targeted: ${report.slotsTargetedCount}`);
  console.log(`Write targets: ${report.writeTargetsCount}`);
  console.log(`Would create: ${report.wouldCreateCount}`);
  console.log(`Would update: ${report.trueWouldUpdateCount}`);
  console.log(`Matched (no-op): ${report.matchedRowCount}`);
  console.log(`insight.similar model: ${report.insightSimilarRowModel.model} (${report.insightSimilarRowModel.rowCount} rows)`);
  console.log(`Projected score after apply: ${report.projectedScoreAfterApply}`);
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
