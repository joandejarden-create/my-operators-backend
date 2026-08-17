#!/usr/bin/env node
/**
 * Brand Explorer Radisson by Choice Active Profile Repair Writer v28E.
 *
 *   npm run brand-explorer-radisson-active-profile-repair-writer -- --brand radisson --dry-run
 *   npm run brand-explorer-radisson-active-profile-repair-writer -- --brand radisson --apply --approve-brand-explorer-v28E-radisson-active-profile-repair --founder-reviewed-radisson-copy-repair --confirm-no-company-validation-claim
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_APPROVE_FACTS,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonActiveProfileRepairWriterMarkdown,
  buildBrandExplorerRadissonActiveProfileRepairWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-active-profile-repair-writer.js";

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
  const approvePendingFacts = hasFlag(APPLY_FLAG_APPROVE_FACTS);

  if (apply && (!approveBatch || !founderReviewed || !noValidationClaim)) {
    console.error(
      `[brand-explorer-radisson-active-profile-repair-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerRadissonActiveProfileRepairWriterReport({
    brandIdOrName: argValue("--brand", "radisson"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noValidationClaim,
    approvePendingFacts,
  });
  const markdown = buildBrandExplorerRadissonActiveProfileRepairWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v28E exists: ${report.v28EWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(
    `Rows to create: ${report.rowsWouldCreate.length}; rows to update: ${report.rowsWouldUpdate.length}`
  );
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(
    `Projected Final QA: ~${report.projectedFinalQa.overallNumeric} (${report.projectedFinalQa.overallActiveProfileReadiness})`
  );
  console.log(`Expected active-profile after apply: ${report.expectedActiveProfileAfterApply ? "yes" : "no"}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  console.log(`Exact apply: ${report.exactApplyCommand}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
