#!/usr/bin/env node
/**
 * Brand Explorer Portfolio Context Ladder Mapping Repair v25C-4D.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG,
  APPLY_FLAG_OWNER_PLANNING,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerPortfolioContextLadderMappingRepairMarkdown,
  buildBrandExplorerPortfolioContextLadderMappingRepairReport,
} from "../lib/partner-intelligence/brand-explorer-portfolio-context-ladder-mapping-repair.js";

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
  const ownerPlanningConfirmed = hasFlag(APPLY_FLAG_OWNER_PLANNING);

  if (apply) {
    const required = [APPLY_FLAG, APPLY_FLAG_OWNER_PLANNING];
    if (!required.every((f) => hasFlag(f))) {
      console.error(
        `[brand-explorer-portfolio-context-ladder-mapping-repair] Apply requires: ${required.join(", ")}`
      );
      process.exit(1);
    }
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerPortfolioContextLadderMappingRepairReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    approveBatch,
    ownerPlanningConfirmed,
  });
  const markdown = buildBrandExplorerPortfolioContextLadderMappingRepairMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Root cause: ${report.rootCause}`);
  console.log(`Mapping ready: ${report.applyGates.mappingReady ? "yes" : "no"}`);
  console.log(`Rows would update: ${report.rowsWouldUpdate.length}`);
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
