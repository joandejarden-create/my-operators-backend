#!/usr/bin/env node
/**
 * Brand Explorer WoodSpring Openings + Momentum Build v33C.
 *
 *   npm run brand-explorer-woodspring-openings-momentum-build-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_REGISTRY_APPROVAL,
  APPLY_FLAG_NO_SOURCE_LIBRARY,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_WOODSPRING_ONLY,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerWoodspringOpeningsMomentumBuildWriterReport,
  v33cWriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-openings-momentum-build-writer.js";

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

  const report = await buildBrandExplorerWoodspringOpeningsMomentumBuildWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noImageFieldChanges: hasFlag(APPLY_FLAG_NO_IMAGE_FIELDS),
    noSourceLibrary: hasFlag(APPLY_FLAG_NO_SOURCE_LIBRARY),
    noRegistryApproval: hasFlag(APPLY_FLAG_NO_REGISTRY_APPROVAL),
    woodspringOnly: hasFlag(APPLY_FLAG_WOODSPRING_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v33C exists: ${v33cWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Openings proposed: ${report.openingsRowsProposed.length}`);
  console.log(`Momentum proposed: ${report.momentumRowsProposed.length}`);
  console.log(`Blocked/skipped: ${report.rowsBlockedOrSkipped.length}`);
  console.log(`Image fields untouched: ${report.imageFieldsUntouched ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Final QA: ${report.expectedFinalQaResult}`);
  console.log(`Complete Build: ${report.expectedCompleteBuildResult}`);
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
