#!/usr/bin/env node
/**
 * Brand Explorer Choice Expansion Partial Profile Backfill + Visual Repair v31A.
 *
 *   npm run brand-explorer-choice-expansion-partial-profile-backfill-writer -- --brands suburban-studios,radisson-individuals-by-choice --dry-run
 *   npm run brand-explorer-choice-expansion-partial-profile-backfill-writer -- --brands suburban-studios,radisson-individuals-by-choice --apply --approve-brand-explorer-v31A-choice-expansion-partial-profile-backfill --founder-reviewed-choice-expansion-copy-repair --confirm-no-company-validation-claim
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
  TARGET_BRANDS,
  buildBrandExplorerChoiceExpansionPartialProfileBackfillWriterReport,
  buildMarkdown,
} from "../lib/partner-intelligence/brand-explorer-choice-expansion-partial-profile-backfill-writer.js";

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
  const brandsArg = argValue("--brands", TARGET_BRANDS.map((b) => b.slug).join(","));

  if (apply && (!approveBatch || !founderReviewed || !noValidationClaim)) {
    console.error(
      `[brand-explorer-choice-expansion-partial-profile-backfill-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerChoiceExpansionPartialProfileBackfillWriterReport({
    brandsArg,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noValidationClaim,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  writeFileSync(DOC_MD, report.markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31A exists: ${report.v31AWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  for (const br of report.brandReports) {
    console.log(
      `${br.brand.slug}: creates=${br.rowsWouldCreate.length} updates=${br.rowsWouldUpdate.length} expectedQA=${br.expectedFinalQaAfterApply.overallNumeric} (${br.expectedFinalQaAfterApply.overallActiveProfileReadiness})`
    );
  }

  if (report.exactApplyCommand) {
    console.log(`Apply: ${report.exactApplyCommand}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
