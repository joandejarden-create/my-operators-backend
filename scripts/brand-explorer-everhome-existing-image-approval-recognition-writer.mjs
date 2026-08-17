#!/usr/bin/env node
/**
 * Brand Explorer Everhome Existing Image Approval Recognition v32G-R1.
 *
 *   npm run brand-explorer-everhome-existing-image-approval-recognition-writer -- --brand everhome-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_EVERHOME_ONLY,
  APPLY_FLAG_FOUNDER_CONFIRMED,
  APPLY_FLAG_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_PRESERVE_IMAGES,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerEverhomeExistingImageApprovalRecognitionWriterReport,
  v32gR1WriterExists,
} from "../lib/partner-intelligence/brand-explorer-everhome-existing-image-approval-recognition-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

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
  const brand = argValue("--brand", "everhome-suites");

  const report =
    await buildBrandExplorerEverhomeExistingImageApprovalRecognitionWriterReport({
      brandArg: brand,
      apply,
      approveBatch: hasFlag(APPLY_FLAG_APPROVE),
      founderConfirmed: hasFlag(APPLY_FLAG_FOUNDER_CONFIRMED),
      preserveWorkingImages: hasFlag(APPLY_FLAG_PRESERVE_IMAGES),
      noImageFieldChanges: hasFlag(APPLY_FLAG_NO_IMAGE_FIELDS),
      noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
      everhomeOnly: hasFlag(APPLY_FLAG_EVERHOME_ONLY),
    });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v32G-R1 exists: ${v32gR1WriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Image fields untouched: ${report.imageFieldsUntouched ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(
    `Working images qualifying: ${report.workingImageConfirmationAudit.filter((r) => r.qualifiesForFounderConfirmedRecognition).length}`
  );
  console.log(`Registry align/approve proposed: ${report.registryAssetsApprovedAligned.length}`);
  console.log(`Registry creates proposed: ${report.registryAssetsCreated.length}`);
  console.log(`Registry patches: ${report.registryPatches.length}`);
  console.log(`Final QA: ${report.finalQaExpectedResult}`);
  console.log(`Complete Build: ${report.completeBuildExpectedResult}`);
  console.log(`Visual defects: ${report.visualDefectExpectedResult}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  if (report.exactApplyCommand) {
    console.log(`Apply command:\n${report.exactApplyCommand}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
