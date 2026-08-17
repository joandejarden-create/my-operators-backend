#!/usr/bin/env node
/**
 * Brand Explorer Bonvoy Loyalty Rich Source + Fact Stewardship Writer v25C-2F.
 *
 *   npm run brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2F-bonvoy-sources-facts --approve-brand-explorer-v25C-2F-source-library-create --approve-brand-explorer-v25C-2F-pending-fact-create
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_FACTS,
  APPLY_FLAG_SOURCES,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerBonvoyLoyaltySourceFactStewardshipMarkdown,
  buildBrandExplorerBonvoyLoyaltySourceFactStewardshipReport,
} from "../lib/partner-intelligence/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.js";

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
  const approveSources = hasFlag(APPLY_FLAG_SOURCES);
  const approveFacts = hasFlag(APPLY_FLAG_FACTS);

  if (apply && (!approveBatch || !approveSources || !approveFacts)) {
    console.error(
      `[brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer] Apply requires ${APPLY_FLAG_BATCH}, ${APPLY_FLAG_SOURCES}, and ${APPLY_FLAG_FACTS}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerBonvoyLoyaltySourceFactStewardshipReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    approveSources,
    approveFacts,
  });
  const markdown = buildBrandExplorerBonvoyLoyaltySourceFactStewardshipMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(
    `Sources would create: ${report.sourceLibraryRecordsWouldCreate.filter((s) => s.action === "create").length}`
  );
  console.log(`Sources already existing: ${report.sourceLibraryRecordsAlreadyExisting.length}`);
  console.log(`Pending facts would create: ${report.pendingFactsWouldCreate.length}`);
  console.log(`Duplicate facts: ${report.duplicateFactsFound.length}`);
  console.log(`Facts excluded: ${report.factsExcluded.length}`);
  console.log(`KPI facts excluded: ${report.kpiFactsExcluded ? "yes" : "no"}`);
  console.log(`Presentation rows untouched: ${report.presentationRowsUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
    console.log(`Next batch:\n${report.exactNextBatchCommand}`);
  }

  if (report.applyResults?.errors?.length) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
