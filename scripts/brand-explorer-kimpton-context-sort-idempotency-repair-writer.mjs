#!/usr/bin/env node
/**
 * Brand Explorer Kimpton Portfolio Context + Sort-Order Idempotency Repair v30A-R1.
 *
 *   npm run brand-explorer-kimpton-context-sort-idempotency-repair-writer -- --brand kimpton --dry-run
 *   npm run brand-explorer-kimpton-context-sort-idempotency-repair-writer -- --brand kimpton --apply --approve-brand-explorer-v30A-R1-kimpton-context-sort-repair --founder-reviewed-kimpton-context-copy --confirm-no-company-validation-claim
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
  buildKimptonContextSortIdempotencyRepairReport,
  buildMarkdown,
} from "../lib/partner-intelligence/brand-explorer-kimpton-context-sort-idempotency-repair-writer.js";

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
  const brandArg = argValue("--brand", "kimpton");

  if (apply && (!approveBatch || !founderReviewed || !noValidationClaim)) {
    console.error(
      `[brand-explorer-kimpton-context-sort-idempotency-repair-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildKimptonContextSortIdempotencyRepairReport({
    brandArg,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noValidationClaim,
  });
  const markdown = buildMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v30A exists: ${report.v30AWriterExists ? "yes" : "no"}`);
  console.log(`v30A-R1 exists: ${report.v30AR1WriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Portfolio issue: ${report.portfolioContextDiagnosis.issueClass}`);
  console.log(
    `Sort drift — legacy v30A: ${report.sortOrderDriftDiagnosis.legacyV30aWouldUpdateCount}; idempotent: ${report.sortOrderDriftDiagnosis.idempotentWouldUpdateCount}`
  );
  console.log(
    `Rows to create: ${report.rowsWouldCreate.length}; rows to update: ${report.rowsWouldUpdate.length}`
  );
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(
    `Expected Final QA: ~${report.expectedFinalQaAfterApply.overallNumeric} (${report.expectedFinalQaAfterApply.overallActiveProfileReadiness})`
  );
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  console.log(`Exact apply: ${report.exactApplyCommand || "(none)"}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
