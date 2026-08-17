#!/usr/bin/env node
/**
 * Brand Explorer Tribute Demand Scenario Row Review Package v25C-5A (read-only).
 *
 *   npm run brand-explorer-tribute-demand-scenario-row-review-package -- --brand tribute-portfolio --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerTributeDemandScenarioRowReviewPackageMarkdown,
  buildBrandExplorerTributeDemandScenarioRowReviewPackageReport,
} from "../lib/partner-intelligence/brand-explorer-tribute-demand-scenario-row-review-package.js";

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
    console.error(
      "[brand-explorer-tribute-demand-scenario-row-review-package] v25C-5A is read-only. Use --dry-run (default)."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerTributeDemandScenarioRowReviewPackageReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerTributeDemandScenarioRowReviewPackageMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Demand proposed: ${report.proposedDemandScenarioRows.length}`);
  console.log(`Demand ready for founder review: ${report.demandRowsReadyForFounderReview}`);
  console.log(`Demand ready for row creation: ${report.demandRowsReadyForRowCreation}`);
  console.log(`At least 3 complete rows: ${report.atLeastThreeCompleteRowsAvailable ? "yes" : "no"}`);
  console.log(`Meets minimum after v25C-5B: ${report.demandMeetsMinimumAfterV25C5B ? "yes" : "no"}`);
  console.log(`v25C-5B row creation ready: ${report.v25C5BRowCreationReady ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Next writer (dry-run): ${report.exactNextWriterCommandDryRun}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
