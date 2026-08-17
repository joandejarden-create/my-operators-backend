#!/usr/bin/env node
/**
 * Brand Explorer Source Evidence + Cross-Section Visual Completion Writer v24C.
 *
 *   npm run brand-explorer-source-evidence-visual-completion-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-source-evidence-visual-completion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v24C-source-evidence-visual-completion --founder-reviewed-visual-completion-copy --approve-brand-explorer-v24C-image-materialization
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_IMAGE,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerSourceEvidenceVisualCompletionWriterMarkdown,
  buildBrandExplorerSourceEvidenceVisualCompletionWriterReport,
} from "../lib/partner-intelligence/brand-explorer-source-evidence-visual-completion-writer.js";

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
  const imageMaterializationApproved = hasFlag(APPLY_FLAG_IMAGE);

  if (apply && (!approveBatch || !founderReviewed || !imageMaterializationApproved)) {
    console.error(
      `[brand-explorer-source-evidence-visual-completion-writer] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_IMAGE}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerSourceEvidenceVisualCompletionWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    imageMaterializationApproved,
  });
  const markdown = buildBrandExplorerSourceEvidenceVisualCompletionWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Gallery.3 root cause: ${report.gallery3Diagnosis.rootCause}`);
  console.log(`Cross-section root cause: ${report.crossSectionVisualDefect.rootCause}`);
  console.log(`Featured Application root cause: ${report.featuredApplicationDefect.rootCause}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Sort-order normalizations: ${report.sortOrderUpdates.length}`);
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
