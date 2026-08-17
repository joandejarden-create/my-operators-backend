#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Approved Asset Materialization + Row Reactivation v31E.
 *
 *   npm run brand-explorer-radisson-individuals-approved-asset-materialization-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_UNAPPROVED_IMAGE,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsApprovedAssetMaterializationWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";

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
  const founderApproved = hasFlag(APPLY_FLAG_FOUNDER);
  const noValidationClaim = hasFlag(APPLY_FLAG_NO_VALIDATION);
  const noUnapprovedImage = hasFlag(APPLY_FLAG_NO_UNAPPROVED_IMAGE);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (apply && (!approveBatch || !founderApproved || !noValidationClaim || !noUnapprovedImage)) {
    console.error(
      `[v31E] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, ${APPLY_FLAG_NO_VALIDATION}, and ${APPLY_FLAG_NO_UNAPPROVED_IMAGE}`
    );
    process.exit(1);
  }

  const report =
    await buildBrandExplorerRadissonIndividualsApprovedAssetMaterializationWriterReport({
      brandArg: brand,
      apply: apply && !dryRun,
      approveBatch,
      founderApproved,
      noValidationClaim,
      noUnapprovedImage,
    });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Approved Asset Materialization Writer v31E\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  const s = report.assetApprovalSummary || {};
  const d = report.currentReadinessDiagnosis || {};
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31E exists: ${report.v31EWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Approved assets: ${s.approved?.length ?? 0}`);
  console.log(`Do Not Use: ${s.doNotUse?.length ?? 0} · Pending: ${s.pendingImageReview?.length ?? 0}`);
  console.log(`Assets to materialize: ${report.assetsToMaterialize?.length ?? 0}`);
  console.log(`Rows to reactivate: ${report.rowsToReactivate?.length ?? 0}`);
  console.log(`Rows kept hidden: ${report.rowsToKeepHidden?.length ?? 0}`);
  console.log(`Replacement needs: ${report.rowsNeedingReplacementImages?.length ?? 0}`);
  console.log(`Final QA: ${d.finalQaScore ?? "—"} (${d.finalQaReadiness ?? "—"})`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  if (report.exactApplyCommand) console.log(`Apply: ${report.exactApplyCommand}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
