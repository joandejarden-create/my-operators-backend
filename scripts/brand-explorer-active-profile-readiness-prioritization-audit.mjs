#!/usr/bin/env node
/**
 * Brand Explorer Active Profile Readiness Prioritization Audit v28A (read-only).
 *
 *   npm run brand-explorer-active-profile-readiness-prioritization-audit -- --all-active --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerActiveProfileReadinessPrioritizationAuditMarkdown,
  buildBrandExplorerActiveProfileReadinessPrioritizationAuditReport,
} from "../lib/partner-intelligence/brand-explorer-active-profile-readiness-prioritization-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

async function main() {
  if (hasFlag("--apply")) {
    console.error(
      "brand-explorer-active-profile-readiness-prioritization-audit is read-only. Use --dry-run (default)."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerActiveProfileReadinessPrioritizationAuditReport({
    allActive: hasFlag("--all-active") || true,
  });
  const markdown = buildBrandExplorerActiveProfileReadinessPrioritizationAuditMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v28A exists: ${report.v28AExists ? "yes" : "no"}`);
  console.log(`All six contract 100: ${report.allSixContract100 ? "yes" : "no"}`);
  console.log(`Tribute active-profile ready: ${report.tributeActiveProfileReady ? "yes" : "no"}`);
  if (report.recommendations.nextBrand) {
    console.log(
      `Recommended next brand: ${report.recommendations.nextBrand.name} (ease ${report.recommendations.nextBrand.easeScore})`
    );
  }
  console.log(`Next writer: ${report.recommendations.nextWriterToBuild}`);
  console.log(
    `Multi-brand visual cleanup safe: ${report.recommendations.multiBrandVisualCleanupSafe ? "yes" : "no"}`
  );
  console.log(
    `Multi-brand apply-approved safe: ${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}`
  );
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
