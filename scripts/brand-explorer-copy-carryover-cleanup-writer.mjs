#!/usr/bin/env node
/**
 * Brand Explorer Copy Carryover Cleanup Writer v26A.
 *
 *   npm run brand-explorer-copy-carryover-cleanup-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-copy-carryover-cleanup-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v26A-copy-carryover-cleanup --founder-reviewed-final-copy-cleanup --confirm-no-company-validation-claim
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerCopyCarryoverCleanupWriterMarkdown,
  buildBrandExplorerCopyCarryoverCleanupWriterReport,
} from "../lib/partner-intelligence/brand-explorer-copy-carryover-cleanup-writer.js";

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
  const dryRun = hasFlag("--dry-run") || !apply;
  const approveBatch = hasFlag(APPLY_FLAG_APPROVE);
  const founderReviewed = hasFlag(APPLY_FLAG_FOUNDER);
  const noValidationClaim = hasFlag(APPLY_FLAG_NO_VALIDATION);

  if (apply && (!approveBatch || !founderReviewed || !noValidationClaim)) {
    console.error(
      `[brand-explorer-copy-carryover-cleanup-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerCopyCarryoverCleanupWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noValidationClaim,
  });
  const markdown = buildBrandExplorerCopyCarryoverCleanupWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Rows inspected: ${report.rowsInspected}`);
  console.log(`Defects before: ${report.defectsBefore.length}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Projected critical defects after apply: ${report.projectedCriticalDefectsAfterApply}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
  }

  if (report.applyResults?.errors?.length) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
