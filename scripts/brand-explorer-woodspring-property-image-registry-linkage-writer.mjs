#!/usr/bin/env node
/**
 * Brand Explorer WoodSpring Property Image Registry Linkage Recognition v33C-R3.
 *
 *   npm run brand-explorer-woodspring-property-image-registry-linkage-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_IMAGE_CHANGES,
  APPLY_FLAG_NO_OTHER_SECTIONS,
  APPLY_FLAG_NO_SOURCE_LIBRARY,
  APPLY_FLAG_NO_SUMMARY_URL,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_WOODSPRING_ONLY,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerWoodspringPropertyImageRegistryLinkageWriterReport,
  v33cR3WriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-property-image-registry-linkage-writer.js";

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

  const report = await buildBrandExplorerWoodspringPropertyImageRegistryLinkageWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    founderApproved: hasFlag(APPLY_FLAG_FOUNDER),
    noImageFieldChanges: hasFlag(APPLY_FLAG_NO_IMAGE_CHANGES),
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
  console.log(`v33C-R3 exists: ${v33cR3WriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Rows audited: ${report.linkageAudit.length}`);
  console.log(`Presentation relink patches: ${report.presentationRegistryLinkPatches.length}`);
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

