#!/usr/bin/env node
/**
 * Brand Explorer Expansion Backlog + Wave Planner v28B (read-only by default).
 *
 *   npm run brand-explorer-expansion-backlog-planner -- --dry-run
 *   npm run brand-explorer-expansion-backlog-planner -- --apply-create-backlog
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_CREATE_BACKLOG_FLAG,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerExpansionBacklogPlannerMarkdown,
  buildBrandExplorerExpansionBacklogPlannerReport,
} from "../lib/partner-intelligence/brand-explorer-expansion-backlog-planner.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

async function main() {
  const applyCreate = hasFlag(APPLY_CREATE_BACKLOG_FLAG);
  const explicitApply = hasFlag("--apply");

  if (explicitApply && !applyCreate) {
    console.error(
      "brand-explorer-expansion-backlog-planner does not support generic --apply. Use --dry-run (default) or --apply-create-backlog for minimal Brand Basics stubs only."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerExpansionBacklogPlannerReport({
    dryRun: !applyCreate,
    applyCreateBacklog: applyCreate,
  });
  const markdown = buildBrandExplorerExpansionBacklogPlannerMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v28B exists: ${report.v28BExists ? "yes" : "no"}`);
  console.log(`Backlog total: ${report.backlogTotal}`);
  console.log(`Existing Brand Basics: ${report.existingBrandBasicsCount}`);
  console.log(`New records needed: ${report.newBrandBasicsNeeded}`);
  console.log(`Waves: ${report.proposedWaves.length}`);
  console.log(`First recommended: ${report.recommendedFirst10[0]?.brandName || "n/a"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
