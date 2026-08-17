#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Final Presentation Quality Sync v31N.
 *
 *   npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_OPENING_CHANGES,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsFinalPresentationQualitySyncWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.js";

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
  const noValidationClaim = hasFlag(APPLY_FLAG_NO_VALIDATION);
  const noOpeningChanges = hasFlag(APPLY_FLAG_NO_OPENING_CHANGES);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (apply && (!approveBatch || !noValidationClaim || !noOpeningChanges)) {
    console.error(
      `[v31N] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_NO_VALIDATION}, and ${APPLY_FLAG_NO_OPENING_CHANGES}`
    );
    process.exit(1);
  }

  const report =
    await buildBrandExplorerRadissonIndividualsFinalPresentationQualitySyncWriterReport({
      brandArg: brand,
      apply: apply && !dryRun,
      approveBatch,
      noValidationClaim,
      noOpeningChanges,
    });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Final Presentation Quality Sync v31N\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31N exists: ${report.v31nWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Score mismatch: ${report.scoreMismatchAudit.rootCause.classification}`);
  console.log(`Orchestrator code changed: ${report.orchestratorCodeChanged ? "yes" : "no"}`);
  console.log(`Featured copy upgrade: ${report.featuredApplicationAudit.copyDiagnosis.needsUpgrade ? "yes" : "no"}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Expected Complete Build ready: ${report.expectedCompleteBuild.readyForActiveProfile}`);
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
