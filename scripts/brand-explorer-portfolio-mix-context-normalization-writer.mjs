#!/usr/bin/env node
/**
 * Brand Explorer Portfolio Mix + Portfolio Context Normalization Writer v25C-4C.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_ALL_ACTIVE,
  APPLY_FLAG_ALL_ACTIVE_FOUNDER,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_STATS,
  APPLY_FLAG_TRIBUTE,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerPortfolioMixContextNormalizationWriterMarkdown,
  buildBrandExplorerPortfolioMixContextNormalizationWriterReport,
} from "../lib/partner-intelligence/brand-explorer-portfolio-mix-context-normalization-writer.js";

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
  const allActive = hasFlag("--all-active");
  const approveBatch = hasFlag(allActive ? APPLY_FLAG_ALL_ACTIVE : APPLY_FLAG_TRIBUTE);
  const founderReviewed = hasFlag(
    allActive ? APPLY_FLAG_ALL_ACTIVE_FOUNDER : APPLY_FLAG_FOUNDER
  );
  const noUnsupportedStatsConfirmed = hasFlag(APPLY_FLAG_NO_STATS);

  if (apply) {
    const required = allActive
      ? [APPLY_FLAG_ALL_ACTIVE, APPLY_FLAG_ALL_ACTIVE_FOUNDER, APPLY_FLAG_NO_STATS]
      : [APPLY_FLAG_TRIBUTE, APPLY_FLAG_FOUNDER, APPLY_FLAG_NO_STATS];
    if (!required.every((f) => hasFlag(f))) {
      console.error(
        `[brand-explorer-portfolio-mix-context-normalization-writer] Apply requires: ${required.join(", ")}`
      );
      process.exit(1);
    }
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerPortfolioMixContextNormalizationWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    allActive,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noUnsupportedStatsConfirmed,
  });
  const markdown = buildBrandExplorerPortfolioMixContextNormalizationWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Portfolio Context root cause: ${report.portfolioContextRootCause}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Rows would deactivate: ${report.rowsWouldDeactivate.length}`);
  console.log(`Unsupported % removed: ${report.unsupportedPercentagesRemoved ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log("Exact apply command:");
  console.log(report.exactApplyCommand);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
