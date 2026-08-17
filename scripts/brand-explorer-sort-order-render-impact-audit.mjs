#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerSortOrderRenderImpactAuditMarkdown,
  buildBrandExplorerSortOrderRenderImpactAuditReport,
} from "../lib/partner-intelligence/brand-explorer-sort-order-render-impact-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  if (process.argv.includes("--apply")) {
    console.error("v24D render-impact audit is read-only. Use --dry-run (default).");
    process.exit(1);
  }
  const report = await buildBrandExplorerSortOrderRenderImpactAuditReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerSortOrderRenderImpactAuditMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Rows reviewed: ${report.totalTributeRowsReviewed}`);
  console.log(`Writer-default rows: ${report.rowsWithWriterDefaultSortOrder}`);
  console.log(`Sort-likely sections: ${report.sectionsLikelyAffectedBySortOrder.length}`);
  console.log(`High-confidence corrections: ${report.highConfidenceSortOrderCorrections.length}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
