#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandResidencesStatusAuditMarkdown,
  buildBrandResidencesStatusAuditReport,
} from "../lib/partner-intelligence/brand-residences-status-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  if (process.argv.includes("--apply")) {
    console.error("brand-residences-status-audit is read-only. Use --dry-run (default).");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandResidencesStatusAuditReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    allActive: process.argv.includes("--all-active"),
  });
  const markdown = buildBrandResidencesStatusAuditMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Tribute status: ${report.tributeCurrentStatus?.status}`);
  console.log(`Frontend renders: ${report.frontendRendersIndicator ? "yes" : "no"}`);
  console.log(`Defects: ${report.defectCounts.total}`);
  console.log(`Airtable modified: no`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
