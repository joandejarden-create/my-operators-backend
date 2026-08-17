#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Gallery Image Restore + Image Governance Scope Fix v31D-R1.
 *
 *   npm run brand-explorer-radisson-individuals-gallery-restore-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_IMAGE_APPROVAL,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_RESTORE,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsGalleryRestoreWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-gallery-restore-writer.js";

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
  const restorePending = hasFlag(APPLY_FLAG_RESTORE);
  const noImageApproval = hasFlag(APPLY_FLAG_NO_IMAGE_APPROVAL);
  const noValidationClaim = hasFlag(APPLY_FLAG_NO_VALIDATION);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (
    apply &&
    (!approveBatch || !restorePending || !noImageApproval || !noValidationClaim)
  ) {
    console.error(
      `[v31D-R1] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_RESTORE}, ${APPLY_FLAG_NO_IMAGE_APPROVAL}, and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  const report = await buildBrandExplorerRadissonIndividualsGalleryRestoreWriterReport({
    brandArg: brand,
    apply: apply && !dryRun,
    approveBatch,
    restorePending,
    noImageApproval,
    noValidationClaim,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Gallery Image Restore Writer v31D-R1\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  const d = report.currentReadinessDiagnosis || {};
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31D-R1 exists: ${report.v31dR1WriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Gallery cleared by v31D: ${report.galleryClearedByV31D?.length ?? 0}`);
  console.log(`Safe to restore: ${report.imagesSafeToRestore?.length ?? 0}`);
  console.log(`Not restored: ${report.imagesNotRestored?.length ?? 0}`);
  console.log(`Rows to update: ${report.rowsToUpdate?.length ?? 0}`);
  console.log(`Registry creates: ${report.proposedRegistryCreates?.length ?? 0}`);
  console.log(`Final QA: ${d.finalQaScore ?? "—"} (${d.finalQaReadiness ?? "—"})`);
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
