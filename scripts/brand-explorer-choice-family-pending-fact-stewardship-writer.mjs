#!/usr/bin/env node
/**
 * Brand Explorer Choice-Family Pending Fact Stewardship Writer v29B.
 *
 *   npm run brand-explorer-choice-family-pending-fact-stewardship-writer -- --brands ascend,radisson-blu --dry-run
 *   npm run brand-explorer-choice-family-pending-fact-stewardship-writer -- --brands ascend,radisson-blu --apply --approve-brand-explorer-v29B-choice-family-pending-fact-stewardship --founder-reviewed-choice-family-fact-governance --confirm-no-company-validation-claim
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
  buildBrandExplorerChoiceFamilyPendingFactStewardshipWriterMarkdown,
  buildBrandExplorerChoiceFamilyPendingFactStewardshipWriterReport,
} from "../lib/partner-intelligence/brand-explorer-choice-family-pending-fact-stewardship-writer.js";
import { TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-choice-family-active-profile-repair-writer.js";

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
  const stopOnCritical = hasFlag("--stop-on-critical");
  const brandsArg = argValue("--brands", TARGET_BRANDS.map((b) => b.slug).join(","));

  if (apply && (!approveBatch || !founderReviewed || !noValidationClaim)) {
    console.error(
      `[brand-explorer-choice-family-pending-fact-stewardship-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerChoiceFamilyPendingFactStewardshipWriterReport({
    brandsArg,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noValidationClaim,
    stopOnCritical,
  });
  const markdown = buildBrandExplorerChoiceFamilyPendingFactStewardshipWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v29B exists: ${report.v29BWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Brands: ${report.brandsRequested.join(", ")}`);
  console.log(
    `Approve: ${report.summary.factsToApprove}; Reject/internal: ${report.summary.factsToReject}; Hold: ${report.summary.factsToHold}`
  );
  console.log(`Dry-run clean: ${report.summary.dryRunClean ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  for (const br of report.brandReports) {
    if (br.skipped) continue;
    console.log(
      `${br.brand.slug}: pending ${br.projectedGovernance.pendingFactsBefore} → ${br.projectedGovernance.pendingFactsAfter}; active-profile after: ${br.expectedActiveProfileAfterApply ? "yes" : "no"}`
    );
    if (br.applyBlockers?.length) {
      console.log(`${br.brand.slug} blockers: ${br.applyBlockers.join("; ")}`);
    }
  }
  console.log(`Exact apply: ${report.exactApplyCommand || "(none)"}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
