#!/usr/bin/env node
/**
 * Brand Explorer Openings + Momentum Source Capture Completion v25C-3A (read-only).
 *
 *   npm run brand-explorer-openings-momentum-source-capture-completion -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-openings-momentum-source-capture-completion -- --brand tribute-portfolio --dry-run --use-puppeteer
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerOpeningsMomentumSourceCaptureCompletionMarkdown,
  buildBrandExplorerOpeningsMomentumSourceCaptureCompletionReport,
} from "../lib/partner-intelligence/brand-explorer-openings-momentum-source-capture-completion.js";

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
      "[brand-explorer-openings-momentum-source-capture-completion] v25C-3A completion is read-only. Use --dry-run (default)."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerOpeningsMomentumSourceCaptureCompletionReport({
    usePuppeteer: hasFlag("--use-puppeteer"),
  });
  const markdown = buildBrandExplorerOpeningsMomentumSourceCaptureCompletionMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(
    `Property candidates ready: ${report.propertyExampleCandidatesReadyForFounderReview}/${report.propertyExampleCandidatesReviewed}`
  );
  console.log(`Property candidates blocked: ${report.propertyExampleCandidatesStillBlocked}`);
  console.log(`Momentum rows ready: ${report.recentMomentumRowsReadyForFounderReview}`);
  console.log(`Momentum rows blocked: ${report.recentMomentumRowsStillBlocked}`);
  console.log(`Openings row review package safe: ${report.openingsRowReviewPackageSafe ? "yes" : "no"}`);
  console.log(`Momentum row review package safe: ${report.momentumRowReviewPackageSafe ? "yes" : "no"}`);
  console.log(`Row creation safe now: ${report.rowCreationSafeNow ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Next batch: ${report.exactNextBatch}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
