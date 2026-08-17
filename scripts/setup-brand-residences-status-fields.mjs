#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  runBrandResidencesStatusFieldSetup,
} from "../lib/partner-intelligence/brand-residences-status-setup.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "brand-residences-status-setup.json");
const REPORT_MD = join(ROOT, "reports", "brand-residences-status-setup.md");
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

async function main() {
  const apply = process.argv.includes("--apply");
  const dryRun = process.argv.includes("--dry-run") || !apply;

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await runBrandResidencesStatusFieldSetup({ dryRun });
  const markdown = [
    "# Brand Residences Status Field Setup",
    "",
    `- Mode: **${report.mode}**`,
    `- Table: **${report.table}**`,
    `- Present: ${report.fieldsPresent.length}`,
    `- Would create: ${report.fieldsWouldCreate.length}`,
    `- Created: ${report.fieldsCreated.length}`,
    `- Failed: ${report.fieldsFailed.length}`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Apply command",
    "```bash",
    report.exactApplyCommand,
    "```",
  ].join("\n");

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Fields present: ${report.fieldsPresent.join(", ") || "none"}`);
  console.log(`Would create: ${report.fieldsWouldCreate.length}`);
  console.log(`Created: ${report.fieldsCreated.length}`);
  if (report.fieldsFailed.length) {
    console.error("Failures:", report.fieldsFailed);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
