#!/usr/bin/env node
/**
 * Brand Explorer Tribute Geographic Footprint Refinement Writer v25C-5D.
 *
 *   npm run brand-explorer-tribute-geographic-footprint-refinement-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-tribute-geographic-footprint-refinement-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-5D-geographic-footprint-refinement --founder-reviewed-geographic-footprint-copy --confirm-no-unsupported-footprint-counts
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_COUNTS,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerTributeGeographicFootprintRefinementWriterMarkdown,
  buildBrandExplorerTributeGeographicFootprintRefinementWriterReport,
} from "../lib/partner-intelligence/brand-explorer-tribute-geographic-footprint-refinement-writer.js";

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
  const founderReviewed = hasFlag(APPLY_FLAG_FOUNDER);
  const noUnsupportedCounts = hasFlag(APPLY_FLAG_NO_COUNTS);

  if (apply && (!approveBatch || !founderReviewed || !noUnsupportedCounts)) {
    console.error(
      `[brand-explorer-tribute-geographic-footprint-refinement-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_COUNTS}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerTributeGeographicFootprintRefinementWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noUnsupportedCounts,
  });
  const markdown = buildBrandExplorerTributeGeographicFootprintRefinementWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Region rows inspected: ${report.footprintRowsInspected.length}`);
  console.log(`Current status: ${report.currentGeographicFootprintReadiness.contractStatusBeforeApply}`);
  console.log(`Ready after apply: ${report.projectedGeographicFootprintReadyAfterApply ? "yes" : "no"}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Unsupported counts excluded: ${report.unsupportedCountsExcluded ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
  }

  if (report.applyResults?.errors?.length) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
