#!/usr/bin/env node
/**
 * Brand Explorer Opening Path + Loyalty Section Quality Repair Writer v25C-4E.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG,
  APPLY_FLAG_FACTS,
  APPLY_FLAG_FOUNDER,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerOpeningLoyaltyQualityRepairWriterMarkdown,
  buildBrandExplorerOpeningLoyaltyQualityRepairWriterReport,
} from "../lib/partner-intelligence/brand-explorer-opening-loyalty-quality-repair-writer.js";

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
  const approveBatch = hasFlag(APPLY_FLAG);
  const founderReviewed = hasFlag(APPLY_FLAG_FOUNDER);
  const approvedFactsOnlyConfirmed = hasFlag(APPLY_FLAG_FACTS);

  if (apply) {
    const required = [APPLY_FLAG, APPLY_FLAG_FOUNDER, APPLY_FLAG_FACTS];
    if (!required.every((f) => hasFlag(f))) {
      console.error(
        `[brand-explorer-opening-loyalty-quality-repair-writer] Apply requires: ${required.join(", ")}`
      );
      process.exit(1);
    }
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerOpeningLoyaltyQualityRepairWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    approvedFactsOnlyConfirmed,
  });
  const markdown = buildBrandExplorerOpeningLoyaltyQualityRepairWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Opening path root cause: ${report.openingPathDiagnosis.rootCause}`);
  console.log(`Proof cards (current): ${report.keyBenefitsDiagnosis.currentCount}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`6 benefits cards: ${report.sixBenefitsCardsWillRender ? "yes" : "no"}`);
  console.log(`Duplicate generic removed: ${report.duplicateGenericLinesRemoved ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  if (report.applyBlockers?.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  console.log("Exact apply command:");
  console.log(report.exactApplyCommand);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
