#!/usr/bin/env node
/**
 * Brand Explorer Tribute Standard Detail Table Writer v25C-4B.
 *
 *   npm run brand-explorer-tribute-standard-detail-table-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-tribute-standard-detail-table-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-4B-tribute-standard-detail --founder-reviewed-tribute-standard-detail-copy --confirm-no-legal-or-curio-copy
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_LEGAL,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerTributeStandardDetailTableWriterMarkdown,
  buildBrandExplorerTributeStandardDetailTableWriterReport,
} from "../lib/partner-intelligence/brand-explorer-tribute-standard-detail-table-writer.js";

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
  const approveBatch = hasFlag(APPLY_FLAG_BATCH);
  const founderReviewed = hasFlag(APPLY_FLAG_FOUNDER);
  const noLegalOrCurioConfirmed = hasFlag(APPLY_FLAG_NO_LEGAL);

  if (apply && (!approveBatch || !founderReviewed || !noLegalOrCurioConfirmed)) {
    console.error(
      `[brand-explorer-tribute-standard-detail-table-writer] Apply requires ${APPLY_FLAG_BATCH}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_NO_LEGAL}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerTributeStandardDetailTableWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noLegalOrCurioConfirmed,
  });
  const markdown = buildBrandExplorerTributeStandardDetailTableWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Placeholder would render: ${report.standardDetailDiagnosis.placeholderWouldRender ? "yes" : "no"}`);
  console.log(`Requirement rows proposed: ${report.requirementRowCount}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Curio/Hilton excluded: ${report.curioHiltonLanguageExcluded ? "yes" : "no"}`);
  console.log(`Raw FDD/legal excluded: ${report.rawFddLegalFragmentsExcluded ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.length}`);
  }
  console.log("Exact apply command:");
  console.log(report.exactApplyCommand);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
