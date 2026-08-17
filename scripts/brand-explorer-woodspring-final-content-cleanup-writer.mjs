#!/usr/bin/env node
/**
 * Brand Explorer WoodSpring Final Content Cleanup v33E.
 *
 *   npm run brand-explorer-woodspring-final-content-cleanup-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_HIDDEN_GALLERY,
  APPLY_FLAG_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_OPENINGS_MOMENTUM,
  APPLY_FLAG_NO_REGISTRY,
  APPLY_FLAG_NO_SOURCE_LIBRARY,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_WOODSPRING_ONLY,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerWoodspringFinalContentCleanupWriterReport,
  v33eWriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-final-content-cleanup-writer.js";

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

  const report = await buildBrandExplorerWoodspringFinalContentCleanupWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noImageFieldChanges: hasFlag(APPLY_FLAG_NO_IMAGE_FIELDS),
    noRegistryChanges: hasFlag(APPLY_FLAG_NO_REGISTRY),
    noSourceLibrary: hasFlag(APPLY_FLAG_NO_SOURCE_LIBRARY),
    noOpeningMomentumChanges: hasFlag(APPLY_FLAG_NO_OPENINGS_MOMENTUM),
    hiddenGalleryStaysHidden: hasFlag(APPLY_FLAG_HIDDEN_GALLERY),
    woodspringOnly: hasFlag(APPLY_FLAG_WOODSPRING_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v33E exists: ${v33eWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Presentation patches: ${report.presentationPatches.length}`);
  console.log(`Fact patches: ${report.factPatches.length}`);
  console.log(`Images untouched: ${report.imagesUntouched ? "yes" : "no"}`);
  console.log(`Openings/Momentum untouched: ${report.openingsMomentumUntouched ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Final QA: ${report.expectedFinalQaResult}`);
  console.log(`Complete Build: ${report.expectedCompleteBuildResult}`);
  console.log(`Visual defects: ${report.expectedVisualDefectResult}`);
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
