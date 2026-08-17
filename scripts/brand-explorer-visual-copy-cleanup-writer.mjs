#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerVisualCopyCleanupWriterMarkdown,
  buildBrandExplorerVisualCopyCleanupWriterReport,
} from "../lib/partner-intelligence/brand-explorer-visual-copy-cleanup-writer.js";

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
  const applyApproved = hasFlag("--approve-brand-explorer-v24B-copy-cleanup");
  const dryRun = hasFlag("--dry-run") || !apply;

  const report = await buildBrandExplorerVisualCopyCleanupWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    apply: apply && !dryRun,
    applyApproved,
  });

  const markdown = buildBrandExplorerVisualCopyCleanupWriterMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Target slots: ${report.targetSlotsInspected}`);
  console.log(`Would update: ${report.wouldUpdateCount}`);
  console.log(`Would create: ${report.wouldCreateCount}`);
  console.log(`Missing rows: ${report.missingTargetRows.length}`);
  console.log(`Preflight passed: ${report.preflightPassed ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
