#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerEvidenceRequiredSlotReadinessPlanMarkdown,
  buildBrandExplorerEvidenceRequiredSlotReadinessPlanReport,
} from "../lib/partner-intelligence/brand-explorer-evidence-required-slot-readiness-plan.js";

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
  const dryRun = process.argv.includes("--dry-run") || !process.argv.includes("--apply");
  if (!dryRun) {
    console.error("v22 readiness plan is read-only. Use --dry-run (default).");
    process.exit(1);
  }

  const report = await buildBrandExplorerEvidenceRequiredSlotReadinessPlanReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerEvidenceRequiredSlotReadinessPlanMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Slots reviewed: ${report.remainingEvidenceMediaManualSlotsReviewed}`);
  console.log(`v23A safe writer slots: ${report.recommendedV23Batches.v23A_source_backed_safe_writer.count}`);
  console.log(`v23B human-review slots: ${report.recommendedV23Batches.v23B_human_review_evidence_writer.count}`);
  console.log(`Projected coverage after v23A: ${report.scoreProjection.projectedSlotCoverageAfterV23A}`);
  console.log(`Completed-brand comparable after v23A: ${report.scoreProjection.tributeCompletedBrandComparableAfterV23A ? "yes" : "no"}`);
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
