#!/usr/bin/env node
/**
 * Brand Explorer Bonvoy Loyalty Row Enhancement Writer v25C-2H.
 *
 *   npm run brand-explorer-bonvoy-loyalty-row-enhancement-writer -- --brand tribute-portfolio --package rich --dry-run
 *   npm run brand-explorer-bonvoy-loyalty-row-enhancement-writer -- --brand tribute-portfolio --package rich --apply --approve-brand-explorer-v25C-2H-rich-bonvoy-loyalty-rows --founder-reviewed-rich-bonvoy-loyalty-copy --confirm-approved-bonvoy-facts-only
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_FACTS,
  APPLY_FLAG_FOUNDER,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerBonvoyLoyaltyRowEnhancementWriterMarkdown,
  buildBrandExplorerBonvoyLoyaltyRowEnhancementWriterReport,
} from "../lib/partner-intelligence/brand-explorer-bonvoy-loyalty-row-enhancement-writer.js";

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
  const approvedFactsOnlyConfirmed = hasFlag(APPLY_FLAG_FACTS);
  const packageMode = argValue("--package", "rich");

  if (apply && (!approveBatch || !founderReviewed || !approvedFactsOnlyConfirmed)) {
    console.error(
      `[brand-explorer-bonvoy-loyalty-row-enhancement-writer] Apply requires ${APPLY_FLAG_BATCH}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_FACTS}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerBonvoyLoyaltyRowEnhancementWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    packageMode,
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    approvedFactsOnlyConfirmed,
  });
  const markdown = buildBrandExplorerBonvoyLoyaltyRowEnhancementWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Package: ${report.packageMode}`);
  console.log(`Approved facts used: ${report.approvedFactsUsed.length}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Earn bullets before→after: ${report.densityComparison.earnBulletsBefore}→${report.densityComparison.earnBulletsAfter}`);
  console.log(`Redeem bullets before→after: ${report.densityComparison.redeemBulletsBefore}→${report.densityComparison.redeemBulletsAfter}`);
  console.log(`Elite detail score before→after: ${report.densityComparison.eliteDetailScoreBefore}→${report.densityComparison.eliteDetailScoreAfter}`);
  console.log(`KPI rows excluded: ${report.kpiRowsExcluded ? "yes" : "no"}`);
  console.log(`Pending/internal/FDD facts excluded: ${report.pendingFactsExcluded.length === 0 && report.internalOrFddFactsExcluded.length >= 0 ? "yes" : "no"}`);
  console.log(`Presentation rows touched (target only): ${report.presentationRowsTouchedOnly.length}`);
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
