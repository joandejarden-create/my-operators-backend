#!/usr/bin/env node
/**
 * Brand Explorer Everhome Image Governance Recognition + Momentum Proper Case v32F.
 *
 *   npm run brand-explorer-everhome-image-governance-recognition-writer -- --brand everhome-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_EVERHOME_ONLY,
  APPLY_FLAG_FOUNDER_ONLY,
  APPLY_FLAG_NO_OPENING_LABELS,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_PRESERVE_IMAGES,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerEverhomeImageGovernanceRecognitionWriterReport,
  v32fWriterExists,
} from "../lib/partner-intelligence/brand-explorer-everhome-image-governance-recognition-writer.js";

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
  const dryRun = hasFlag("--dry-run") || !apply;
  const brand = argValue("--brand", "everhome-suites");

  const report = await buildBrandExplorerEverhomeImageGovernanceRecognitionWriterReport({
    brandArg: brand,
    apply: apply && !dryRun,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    founderApprovedOnly: hasFlag(APPLY_FLAG_FOUNDER_ONLY),
    preserveWorkingImages: hasFlag(APPLY_FLAG_PRESERVE_IMAGES),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noOpeningLabelChanges: hasFlag(APPLY_FLAG_NO_OPENING_LABELS),
    everhomeOnly: hasFlag(APPLY_FLAG_EVERHOME_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v32F exists: ${v32fWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Working images audited: ${report.workingImageAudit.length}`);
  console.log(`Images preserved: ${report.imagesPreserved.length}`);
  console.log(`Registry patches: ${report.registryPatches.length}`);
  console.log(`Momentum title fixes: ${report.momentumHeadingBeforeAfter.length}`);
  console.log(`Images materialized: ${report.imagesMaterialized.length}`);
  console.log(`Next writer: ${report.recommendedNextWriter}`);
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
