#!/usr/bin/env node
/**
 * Brand Explorer Bonvoy Source Stewardship Writer v25C-2G-S.
 *
 *   npm run brand-explorer-bonvoy-source-stewardship-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-bonvoy-source-stewardship-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2G-source-stewardship --confirm-official-marriott-bonvoy-sources
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_CONFIRM,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerBonvoySourceStewardshipWriterMarkdown,
  buildBrandExplorerBonvoySourceStewardshipWriterReport,
} from "../lib/partner-intelligence/brand-explorer-bonvoy-source-stewardship-writer.js";

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
  const officialSourcesConfirmed = hasFlag(APPLY_FLAG_CONFIRM);

  if (apply && (!approveBatch || !officialSourcesConfirmed)) {
    console.error(
      `[brand-explorer-bonvoy-source-stewardship-writer] Apply requires ${APPLY_FLAG_BATCH} and ${APPLY_FLAG_CONFIRM}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerBonvoySourceStewardshipWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    officialSourcesConfirmed,
  });
  const markdown = buildBrandExplorerBonvoySourceStewardshipWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Sources would update: ${report.sourcesWouldUpdate.length}`);
  console.log(`Sources matched: ${report.sourcesMatched.length}`);
  console.log(`Non-target sources touched: ${report.nonTargetSourcesTouched ? "yes" : "no"}`);
  console.log(`Partner facts untouched: ${report.partnerFactsUntouched ? "yes" : "no"}`);
  console.log(`Presentation rows untouched: ${report.presentationRowsUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
    console.log(`v25C-2G re-run:\n${report.exactRichFactApprovalDryRunCommand}`);
  }

  if (report.applyResults?.errors?.length) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
