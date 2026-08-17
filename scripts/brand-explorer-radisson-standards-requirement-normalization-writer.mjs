#!/usr/bin/env node
/**
 * Brand Explorer Radisson Standards Requirement Column Normalization Writer v27D.
 *
 *   npm run brand-explorer-radisson-standards-requirement-normalization-writer -- --brand radisson --dry-run
 *   npm run brand-explorer-radisson-standards-requirement-normalization-writer -- --brand radisson --apply --approve-brand-explorer-v27D-radisson-standards-requirement-normalization --founder-reviewed-standard-detail-owner-planning-copy --confirm-not-legal-or-company-validation
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
  buildBrandExplorerRadissonStandardsRequirementNormalizationWriterMarkdown,
  buildBrandExplorerRadissonStandardsRequirementNormalizationWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-standards-requirement-normalization-writer.js";

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
      `[brand-explorer-radisson-standards-requirement-normalization-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_LEGAL}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerRadissonStandardsRequirementNormalizationWriterReport({
    brandIdOrName: argValue("--brand", "radisson"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noLegalOrCompanyConfirmed,
  });
  const markdown = buildBrandExplorerRadissonStandardsRequirementNormalizationWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v27D exists: ${report.v27DWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(
    `Requirement rows: ${report.diagnosis.completeBefore}/${report.diagnosis.requirementRowCount} complete → projected ${report.diagnosis.completeAfterProjected}/${report.diagnosis.requirementRowCount}`
  );
  console.log(`Contract: ${report.diagnosis.contractScoreBefore} → ${report.projectedContractScore}`);
  console.log(`Requirement updates: ${report.requirementRowsToUpdate.length}`);
  console.log(
    `Governance create/update: ${report.governanceRowsToCreate.length}/${report.governanceRowsToUpdate.length}`
  );
  console.log(`Dry-run clean for apply: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
  }
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }

  if (report.applyResults?.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
