#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerFinalQaAuditorMarkdown,
  buildBrandExplorerFinalQaAuditorReport,
} from "../lib/partner-intelligence/brand-explorer-final-qa-auditor.js";

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
  if (hasFlag("--apply")) {
    console.error("brand-explorer-final-qa-auditor is read-only. Use --dry-run (default).");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const allActive = hasFlag("--all-active");
  const brand = argValue("--brand", "tribute-portfolio");

  const report = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: brand,
    allActive,
  });
  const markdown = buildBrandExplorerFinalQaAuditorMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  const primary = (report.brandReports || []).find((b) => !b.error) || report.brandReports?.[0];
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  if (primary && !primary.error) {
    console.log(`Brand: ${primary.brand.name}`);
    console.log(`Overall readiness: ${primary.scores.overallActiveProfileReadiness} (${primary.scores.overallNumeric})`);
    console.log(`Defects: ${primary.defectCounts.total} (critical ${primary.defectCounts.critical}, high ${primary.defectCounts.high})`);
  } else if (allActive) {
    console.log(`Brands audited: ${report.brandReports.length}`);
  }
  console.log(`Airtable modified: no`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
