#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Opening Asset Approval Reconciliation v31M-R1.
 *
 *   npm run brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_APPROVED_ONLY,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsOpeningAssetApprovalReconciliationWriterReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.js";

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
  const approvedAssetsOnly = hasFlag(APPLY_FLAG_APPROVED_ONLY);
  const noValidationClaim = hasFlag(APPLY_FLAG_NO_VALIDATION);
  const founderReviewed = hasFlag(APPLY_FLAG_FOUNDER);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (apply && (!approveBatch || !approvedAssetsOnly || !noValidationClaim || !founderReviewed)) {
    console.error(
      `[v31M-R1] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_APPROVED_ONLY}, ${APPLY_FLAG_NO_VALIDATION}, and ${APPLY_FLAG_FOUNDER}`
    );
    process.exit(1);
  }

  const report =
    await buildBrandExplorerRadissonIndividualsOpeningAssetApprovalReconciliationWriterReport({
      brandArg: brand,
      apply: apply && !dryRun,
      approveBatch,
      approvedAssetsOnly,
      noValidationClaim,
      founderReviewed,
    });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Opening Asset Approval Reconciliation v31M-R1\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31M-R1 exists: ${report.v31mR1WriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Duplicate groups: ${report.duplicateRegistryGroups.length}`);
  console.log(`Rows to materialize: ${report.rowsMaterialized.length}`);
  console.log(`Rows to reactivate: ${report.rowsReactivated.length}`);
  console.log(`Momentum v31M applied: ${report.momentumApplyStatus.v31mFullyApplied ? "yes" : "no"}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
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
