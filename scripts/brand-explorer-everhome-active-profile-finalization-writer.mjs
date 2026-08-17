#!/usr/bin/env node
/**
 * Brand Explorer Everhome Active Profile Finalization v32G.
 *
 *   npm run brand-explorer-everhome-active-profile-finalization-writer -- --brand everhome-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_EVERHOME_ONLY,
  APPLY_FLAG_FOUNDER_ONLY,
  APPLY_FLAG_NO_COPY,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_PRESERVE_IMAGES,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerEverhomeActiveProfileFinalizationWriterReport,
  v32gWriterExists,
} from "../lib/partner-intelligence/brand-explorer-everhome-active-profile-finalization-writer.js";

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

  const report = await buildBrandExplorerEverhomeActiveProfileFinalizationWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    founderApprovedOnly: hasFlag(APPLY_FLAG_FOUNDER_ONLY),
    preserveWorkingImages: hasFlag(APPLY_FLAG_PRESERVE_IMAGES),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noCopyChanges: hasFlag(APPLY_FLAG_NO_COPY),
    everhomeOnly: hasFlag(APPLY_FLAG_EVERHOME_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v32G exists: ${v32gWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Active-profile gate: ${report.activeProfileGatePassing ? "pass" : "blocked"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Visual rows audited: ${report.activeProfileVisualAudit.length}`);
  console.log(`Founder-approved registry assets: ${report.founderApprovedAssetRecognition.filter((a) => a.bucket === "approved_for_explorer_use").length}`);
  console.log(`Rows requiring founder approval: ${report.rowsRequiringFounderApproval.length}`);
  console.log(`Images preserved: ${report.imagesPreserved.length}`);
  console.log(`Registry patches: ${report.registryPatches.length}`);
  console.log(`Final QA: ${report.finalQaExpectedResult}`);
  console.log(`Complete Build: ${report.completeBuildExpectedResult}`);
  console.log(`Openings: ${report.openingsReadiness.summary}`);
  console.log(`Momentum: ${report.momentumReadiness.summary}`);
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
