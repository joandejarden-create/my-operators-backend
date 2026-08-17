#!/usr/bin/env node
/**
 * Brand Explorer Choice Extended-Stay Batch Readiness + Source Audit v32A.
 *
 *   npm run brand-explorer-choice-extended-stay-batch-readiness-audit -- --brands woodspring-suites,everhome-suites,suburban-studios --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  AUDIT_VERSION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerChoiceExtendedStayBatchReadinessAuditReport,
  v32aAuditExists,
} from "../lib/partner-intelligence/brand-explorer-choice-extended-stay-batch-readiness-audit.js";

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
    console.error("[v32A] Batch readiness audit is read-only. Use --dry-run (default).");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brands = argValue("--brands", "woodspring-suites,everhome-suites,suburban-studios");
  const includeFinalQa = hasFlag("--include-final-qa");
  const includeCompleteBuild = hasFlag("--include-complete-build");

  const report = await buildBrandExplorerChoiceExtendedStayBatchReadinessAuditReport({
    brands,
    includeFinalQa,
    includeCompleteBuild,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Choice Extended-Stay Batch Readiness Audit v32A\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v32A exists: ${v32aAuditExists() ? "yes" : "no"}`);
  console.log(`Audit version: ${AUDIT_VERSION}`);
  console.log(`Brands: ${report.batchBrands.join(", ")}`);
  console.log(`Airtable modified: no`);
  console.log(`Company Validated untouched: yes`);
  console.log(`Next writer: ${report.recommendedNextWriter}`);
  for (const r of report.batchFeasibilityRanking) {
    console.log(`  ${r.rank}. ${r.displayName}: ${r.band} (${r.reason})`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
