#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Final Visible UI Cleanup v31D.
 *
 *   npm run brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_IMAGE,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsFinalVisibleUiCleanupWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.js";

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
  const noImageApproval = hasFlag(APPLY_FLAG_NO_IMAGE);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (apply && (!approveBatch || !founderReviewed || !noValidationClaim || !noImageApproval)) {
    console.error(
      `[v31D] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, ${APPLY_FLAG_NO_VALIDATION}, and ${APPLY_FLAG_NO_IMAGE}`
    );
    process.exit(1);
  }

  const report = await buildBrandExplorerRadissonIndividualsFinalVisibleUiCleanupWriterReport({
    brandArg: brand,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noValidationClaim,
    noImageApproval,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Final Visible UI Cleanup Writer v31D\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  const d = report.currentVisibleBlockerDiagnosis || {};
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31D exists: ${report.v31DWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Final QA: ${d.finalQaScore ?? "—"} (${d.finalQaReadiness ?? "—"})`);
  console.log(`Visual defects: ${d.visualAuditDefectCount ?? "—"} (${d.visualAuditHighDefectCount ?? 0} high)`);
  console.log(`Quarantined: ${d.quarantinedRowCount ?? 0} · Registry pending: ${d.registryPendingQueueCount ?? 0}`);
  console.log(`Featured create: ${d.missingFeaturedApplicationRow ? "yes" : "no"}`);
  console.log(`Copy fixes: ${report.rowsWouldUpdate?.filter((r) => r.action === "copy_cleanup").length ?? 0}`);
  console.log(`Image clears: ${report.rowsWouldUpdate?.filter((r) => r.action === "clear_unapproved_image").length ?? 0}`);
  console.log(`Quarantine integrity: ${report.quarantineIntegrity?.integrityOk ? "ok" : "leak"}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  if (report.exactApplyCommand) console.log(`Apply: ${report.exactApplyCommand}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
