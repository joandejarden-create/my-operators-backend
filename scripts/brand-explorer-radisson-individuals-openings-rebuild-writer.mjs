#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Openings Rebuild v31L.
 *
 *   npm run brand-explorer-radisson-individuals-openings-rebuild-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_APPROVED_ONLY,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsOpeningsRebuildWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-openings-rebuild-writer.js";

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
  const approvedAssetsOnly = hasFlag(APPLY_FLAG_APPROVED_ONLY);
  const noValidationClaim = hasFlag(APPLY_FLAG_NO_VALIDATION);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (apply && (!approveBatch || !founderReviewed || !approvedAssetsOnly || !noValidationClaim)) {
    console.error(
      `[v31L] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, ${APPLY_FLAG_APPROVED_ONLY}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  const report = await buildBrandExplorerRadissonIndividualsOpeningsRebuildWriterReport({
    brandArg: brand,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    approvedAssetsOnly,
    noValidationClaim,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Openings Rebuild v31L\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31L exists: ${report.v31lWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Rows audited: ${report.openingsRowDiagnosis.length}`);
  console.log(`Rows to rebuild: ${report.rowsToRebuild.length}`);
  console.log(`Rows to reactivate: ${report.rowsToReactivate.length}`);
  console.log(`Keep quarantined: ${report.rowsToKeepQuarantined.length}`);
  console.log(`Empty shell fix: ${report.emptyShellFix.after}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Images approved: no`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  if (report.exactApplyCommand) console.log(`Apply: ${report.exactApplyCommand}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
