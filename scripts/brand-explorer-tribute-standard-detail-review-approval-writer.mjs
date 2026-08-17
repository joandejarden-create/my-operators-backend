#!/usr/bin/env node
/**
 * Brand Explorer Tribute Standard Detail Review Approval State Writer v25C-5C.
 *
 *   npm run brand-explorer-tribute-standard-detail-review-approval-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-tribute-standard-detail-review-approval-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-5C-standard-detail-review-state --founder-reviewed-standard-detail-owner-planning-copy --confirm-not-legal-or-company-validation
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_LEGAL,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerTributeStandardDetailReviewApprovalWriterMarkdown,
  buildBrandExplorerTributeStandardDetailReviewApprovalWriterReport,
} from "../lib/partner-intelligence/brand-explorer-tribute-standard-detail-review-approval-writer.js";

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
  const noLegalOrCompanyConfirmed = hasFlag(APPLY_FLAG_NO_LEGAL);

  if (apply && (!approveBatch || !founderReviewed || !noLegalOrCompanyConfirmed)) {
    console.error(
      `[brand-explorer-tribute-standard-detail-review-approval-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_LEGAL}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerTributeStandardDetailReviewApprovalWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noLegalOrCompanyConfirmed,
  });
  const markdown = buildBrandExplorerTributeStandardDetailReviewApprovalWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Requirement rows: ${report.requirementRowCount}`);
  console.log(`Current readiness: ${report.currentStandardDetailReadiness.contractStatusBeforeApply}`);
  console.log(`Ready after apply: ${report.projectedStandardDetailReadyAfterApply ? "yes" : "no"}`);
  console.log(`Governance rows to create: ${report.rowsWouldCreate.length}`);
  console.log(`Governance rows to update: ${report.rowsWouldUpdate.length}`);
  console.log(`Copy changes required: ${report.copyChangesRequired.length > 0 ? "yes" : "no"}`);
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
