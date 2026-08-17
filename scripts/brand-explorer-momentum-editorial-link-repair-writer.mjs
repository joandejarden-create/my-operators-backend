#!/usr/bin/env node
/**
 * Brand Explorer Momentum Editorial + Link Repair Writer v25C-3E.
 *
 *   npm run brand-explorer-momentum-editorial-link-repair-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-momentum-editorial-link-repair-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-3E-momentum-editorial-link-repair --founder-reviewed-momentum-ui-copy --confirm-no-false-announcement-links
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_LINKS,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerMomentumEditorialLinkRepairWriterMarkdown,
  buildBrandExplorerMomentumEditorialLinkRepairWriterReport,
} from "../lib/partner-intelligence/brand-explorer-momentum-editorial-link-repair-writer.js";

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
  const noFalseAnnouncementLinksConfirmed = hasFlag(APPLY_FLAG_LINKS);

  if (apply && (!approveBatch || !founderReviewed || !noFalseAnnouncementLinksConfirmed)) {
    console.error(
      `[brand-explorer-momentum-editorial-link-repair-writer] Apply requires ${APPLY_FLAG_BATCH}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_LINKS}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerMomentumEditorialLinkRepairWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noFalseAnnouncementLinksConfirmed,
  });
  const markdown = buildBrandExplorerMomentumEditorialLinkRepairWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Momentum rows inspected: ${report.rowDiagnostics.length}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Announcement removed (non-PR): ${report.announcementRemovedUnlessPrBacked ? "yes" : "no"}`);
  console.log(`Internal language removed: ${report.internalSourceCaptureLanguageRemoved ? "yes" : "no"}`);
  console.log(`Casa Nizuc excluded from momentum: ${report.casaNizucExcludedFromMomentum ? "yes" : "no"}`);
  console.log(`Loyalty untouched: ${report.loyaltyRowsUntouched ? "yes" : "no"}`);
  console.log(`Openings untouched: ${report.openingsRowsUntouched ? "yes" : "no"}`);
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
