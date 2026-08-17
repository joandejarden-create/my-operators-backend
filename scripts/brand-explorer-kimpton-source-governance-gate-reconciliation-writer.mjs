#!/usr/bin/env node
/**
 * Brand Explorer Kimpton Source Governance Gate Reconciliation Writer v30D.
 *
 *   npm run brand-explorer-kimpton-source-governance-gate-reconciliation-writer -- --brand kimpton --dry-run
 *   npm run brand-explorer-kimpton-source-governance-gate-reconciliation-writer -- --brand kimpton --apply --approve-brand-explorer-v30D-kimpton-source-governance-gate-reconciliation --confirm-no-company-validation-claim
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_VALIDATION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildKimptonSourceGovernanceGateReconciliationReport,
  buildMarkdown,
} from "../lib/partner-intelligence/brand-explorer-kimpton-source-governance-gate-reconciliation-writer.js";

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
  const brandArg = argValue("--brand", "kimpton");

  if (apply && (!approveBatch || !noValidationClaim)) {
    console.error(
      `[brand-explorer-kimpton-source-governance-gate-reconciliation-writer] Apply requires ${APPLY_FLAG_APPROVE} and ${APPLY_FLAG_NO_VALIDATION}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildKimptonSourceGovernanceGateReconciliationReport({
    brandArg,
    apply: apply && !dryRun,
    approveBatch,
    noValidationClaim,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  writeFileSync(DOC_MD, report.markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v30D exists: ${report.v30DWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Root cause: ${report.rootCause}`);
  console.log(`Explorer governedPlatformReady: ${report.explorerGovernance.governedPlatformReady ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(
    `Complete Build readyForActiveProfile: ${report.completeBuildAfterFix.readyForActiveProfile ? "yes" : "no"}`
  );
  console.log(
    `Kimpton active-profile ready: ${report.expectedKimptonActiveProfileReady ? "yes" : "no"}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
