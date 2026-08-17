#!/usr/bin/env node
/**
 * Brand Explorer WoodSpring Six-Image Gallery Completion v33H.
 *
 *   npm run brand-explorer-woodspring-six-image-gallery-completion-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_MINIMUM_SIX,
  APPLY_FLAG_NO_OTHER_SECTIONS,
  APPLY_FLAG_NO_SUMMARY_URL,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_OFFICIAL_ONLY,
  APPLY_FLAG_WOODSPRING_ONLY,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerWoodspringSixImageGalleryCompletionWriterReport,
  v33hWriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-six-image-gallery-completion-writer.js";

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
  const brand = argValue("--brand", "woodspring-suites");

  const report = await buildBrandExplorerWoodspringSixImageGalleryCompletionWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    founderApproved: hasFlag(APPLY_FLAG_FOUNDER),
    officialImagesOnly: hasFlag(APPLY_FLAG_OFFICIAL_ONLY),
    minimumSixConfirmed: hasFlag(APPLY_FLAG_MINIMUM_SIX),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noSummaryUrl: hasFlag(APPLY_FLAG_NO_SUMMARY_URL),
    noOtherSectionChanges: hasFlag(APPLY_FLAG_NO_OTHER_SECTIONS),
    woodspringOnly: hasFlag(APPLY_FLAG_WOODSPRING_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v33H exists: ${v33hWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Distinct safe images: ${report.distinctSafeImages}/6`);
  console.log(`Projected visible gallery: ${report.projectedVisibleGalleryCount}/6`);
  console.log(`Projected API imageUrls: ${report.projectedApiImageUrlCount}/6`);
  console.log(`Presentation patches: ${report.presentationPatches.length}`);
  console.log(`Registry patches/creates: ${report.registryPatches.length}/${report.registryCreates.length}`);
  console.log(`Logo/generic remain visible: ${report.logoOrGenericRemainVisible ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Final QA: ${report.expectedFinalQaResult}`);
  console.log(`Complete Build: ${report.expectedCompleteBuildResult}`);
  console.log(`Visual defects: ${report.expectedVisualDefectResult}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  if (report.exactApplyCommand) {
    console.log(`Apply command: ${report.exactApplyCommand}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
