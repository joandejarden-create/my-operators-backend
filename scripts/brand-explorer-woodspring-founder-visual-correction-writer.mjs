#!/usr/bin/env node
/**
 * Brand Explorer WoodSpring Founder Visual QA Correction v33G.
 *
 *   npm run brand-explorer-woodspring-founder-visual-correction-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_OTHER_SECTIONS,
  APPLY_FLAG_NO_SOURCE_LIBRARY,
  APPLY_FLAG_NO_SUMMARY_URL,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_WOODSPRING_ONLY,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerWoodspringFounderVisualCorrectionWriterReport,
  v33gWriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-founder-visual-correction-writer.js";

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

  const report = await buildBrandExplorerWoodspringFounderVisualCorrectionWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noSourceLibraryChanges: hasFlag(APPLY_FLAG_NO_SOURCE_LIBRARY),
    noSummaryUrl: hasFlag(APPLY_FLAG_NO_SUMMARY_URL),
    noOtherSectionChanges: hasFlag(APPLY_FLAG_NO_OTHER_SECTIONS),
    woodspringOnly: hasFlag(APPLY_FLAG_WOODSPRING_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v33G exists: ${v33gWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Copy patches: ${report.presentationCopyPatches.length}`);
  console.log(`Hide patches: ${report.presentationHidePatches.length}`);
  console.log(`Registry link patches: ${report.presentationRegistryLinkPatches.length}`);
  console.log(`Creates: ${report.presentationCreates?.length || 0}`);
  console.log(`Image patches: ${report.presentationImagePatches?.length || 0}`);
  console.log(`Image fields untouched: ${report.imageFieldsUntouched ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  if (report.applyBlockers?.length) {
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
