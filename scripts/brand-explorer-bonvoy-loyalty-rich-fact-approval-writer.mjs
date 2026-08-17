#!/usr/bin/env node
/**
 * Brand Explorer Bonvoy Loyalty Rich Fact Approval Writer v25C-2G.
 *
 *   npm run brand-explorer-bonvoy-loyalty-rich-fact-approval-writer -- --brand tribute-portfolio --dry-run
 *   npm run brand-explorer-bonvoy-loyalty-rich-fact-approval-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2G-rich-bonvoy-facts --founder-reviewed-rich-bonvoy-facts --confirm-bonvoy-sources-explorer-safe
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_BATCH,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_SOURCES,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerBonvoyLoyaltyRichFactApprovalWriterMarkdown,
  buildBrandExplorerBonvoyLoyaltyRichFactApprovalWriterReport,
} from "../lib/partner-intelligence/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.js";

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
  const sourcesExplorerSafeConfirmed = hasFlag(APPLY_FLAG_SOURCES);

  if (apply && (!approveBatch || !founderReviewed || !sourcesExplorerSafeConfirmed)) {
    console.error(
      `[brand-explorer-bonvoy-loyalty-rich-fact-approval-writer] Apply requires ${APPLY_FLAG_BATCH}, ${APPLY_FLAG_FOUNDER}, and ${APPLY_FLAG_SOURCES}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerBonvoyLoyaltyRichFactApprovalWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    sourcesExplorerSafeConfirmed,
  });
  const markdown = buildBrandExplorerBonvoyLoyaltyRichFactApprovalWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Source stewardship sufficient: ${report.sourceStewardshipSufficient ? "yes" : "no"}`);
  console.log(`Facts inspected: ${report.factsInspected.length}`);
  console.log(`Facts would approve (now): ${report.factsWouldApprove.length}`);
  console.log(`Facts safe when stewarded: ${report.factsSafeToApproveWhenStewarded.length}`);
  console.log(`Facts excluded: ${report.factsExcluded.length}`);
  console.log(`Unsupported KPI in plan: ${report.unsupportedKpiFactsIncluded ? "yes" : "no"}`);
  console.log(`Internal/FDD in plan: ${report.internalFddFactsIncluded ? "yes" : "no"}`);
  console.log(`Reference-brand leak: ${report.referenceBrandFactsLeakedIntoPlan ? "yes" : "no"}`);
  console.log(`Presentation rows untouched: ${report.presentationRowsUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);

  if (!apply) {
    console.log(`Exact apply command:\n${report.exactApplyCommand}`);
    console.log(`Next batch (v25C-2H):\n${report.exactNextBatchCommand}`);
  }

  if (report.applyBlockers?.length && !report.applyGates?.canApply) {
    console.log(`Apply blockers: ${report.applyBlockers.slice(0, 5).join("; ")}${report.applyBlockers.length > 5 ? "…" : ""}`);
  }

  if (report.applyResults?.errors?.length) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
