#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerFactoryGapMatrixAuditMarkdown,
  buildBrandExplorerFactoryGapMatrixAuditReport,
} from "../lib/partner-intelligence/brand-explorer-factory-gap-matrix-audit.js";

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
  if (hasFlag("--apply")) {
    console.error("brand-explorer-factory-gap-matrix-audit is read-only. Use --dry-run (default).");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerFactoryGapMatrixAuditReport({
    allActive: hasFlag("--all-active"),
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerFactoryGapMatrixAuditMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v27A exists: ${report.v27AExists ? "yes" : "no"}`);
  console.log(`Brands audited: ${report.brandsAudited.join(", ")}`);
  console.log(`Shared contract 63 pattern: ${report.sharedFailureAnalysis.uniformContractScore ? "yes" : "no"}`);
  console.log(`Tribute active-profile ready: ${report.tributeActiveProfileReady ? "yes" : "no"}`);
  if (report.recommendations.recommendedNextBrand) {
    console.log(
      `Recommended next brand: ${report.recommendations.recommendedNextBrand.name} (${report.recommendations.recommendedNextBrand.slug})`
    );
  }
  console.log(`Generalize contract first: ${report.recommendations.generalizeContractFirst ? "yes" : "no"}`);
  console.log(`Multi-brand apply-approved safe: ${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
