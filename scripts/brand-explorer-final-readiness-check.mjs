#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerFinalReadinessV26BMarkdown,
  buildBrandExplorerFinalReadinessV26BReport,
} from "../lib/partner-intelligence/brand-explorer-final-readiness-v26B.js";

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
    console.error("brand-explorer-final-readiness-check is read-only. Use --dry-run (default).");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerFinalReadinessV26BReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    brandRecordId: argValue("--brand-id", "recCvV0PuZOi8c3hC"),
  });
  const markdown = buildBrandExplorerFinalReadinessV26BMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v26B exists: ${report.v26BExists ? "yes" : "no"}`);
  console.log(`Final QA: ${report.finalQa.status} (${report.finalQa.score})`);
  console.log(`Required sections: ${report.requiredSection.score} ready=${report.requiredSection.ready}`);
  console.log(`Complete build active-profile ready: ${report.activeProfileReady ? "yes" : "no"}`);
  console.log(`Governed ready: ${report.packagePipeline.governedReady ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
