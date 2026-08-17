#!/usr/bin/env node
/**
 * Brand Explorer Openings / Examples Visual + Modal Repair Writer v25C-3D.
 *
 *   npm run brand-explorer-openings-visual-modal-repair-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-openings-visual-modal-repair-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-3D-openings-visual-modal-repair --founder-reviewed-openings-ui-copy --approve-brand-explorer-v25C-3D-image-render-repair
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_IMAGE,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerOpeningsVisualModalRepairWriterMarkdown,
  buildBrandExplorerOpeningsVisualModalRepairWriterReport,
} from "../lib/partner-intelligence/brand-explorer-openings-visual-modal-repair-writer.js";

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
  const imageRepairApproved = hasFlag(APPLY_FLAG_IMAGE);

  if (apply && (!approveBatch || !founderReviewed || !imageRepairApproved)) {
    console.error(
      `[brand-explorer-openings-visual-modal-repair-writer] Apply requires ${APPLY_FLAG_BATCH}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_IMAGE}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerOpeningsVisualModalRepairWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    imageRepairApproved,
  });
  const markdown = buildBrandExplorerOpeningsVisualModalRepairWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
  console.log(`Rows would create: ${report.rowsWouldCreate.length}`);
  console.log(`Root cause (images): ${report.rootCauseOfMissingImages}`);
  console.log(`MARSHA removed from UI: ${report.marshaCodesRemovedFromUiFieldsAfterRepair ? "yes" : "no"}`);
  console.log(
    `Consumer-site listing removed: ${report.consumerSiteListingRemovedFromUiFieldsAfterRepair ? "yes" : "no"}`
  );
  console.log(`All cards will have images: ${report.allOpeningCardsWillHaveImagesAfterRepair ? "yes" : "no"}`);
  console.log(`Modal complete after repair: ${report.modalRequiredFieldsCompleteAfterRepair ? "yes" : "no"}`);
  console.log(`Loyalty untouched: ${report.loyaltyRowsUntouched ? "yes" : "no"}`);
  console.log(`Momentum untouched: ${report.momentumRowsUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
  }

  if (report.applyBlockers?.length && !report.applyGates?.canApply) {
    console.log(`Apply blockers: ${report.applyBlockers.join(", ")}`);
  }

  if (report.applyResults?.errors?.length) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
