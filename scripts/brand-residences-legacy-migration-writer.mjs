#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG,
  APPLY_FLAG_INFERENCE,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandResidencesLegacyMigrationWriterMarkdown,
  buildBrandResidencesLegacyMigrationWriterReport,
} from "../lib/partner-intelligence/brand-residences-legacy-migration-writer.js";

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

  if (apply) {
    const required = [APPLY_FLAG, APPLY_FLAG_INFERENCE];
    if (!required.every((f) => hasFlag(f))) {
      console.error(
        `[brand-residences-legacy-migration-writer] Apply requires: ${required.join(", ")}`
      );
      process.exit(1);
    }
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandResidencesLegacyMigrationWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    allActive: hasFlag("--all-active"),
    apply: apply && !dryRun,
    approveBatch: hasFlag(APPLY_FLAG),
    confirmNoUnsupportedInference: hasFlag(APPLY_FLAG_INFERENCE),
  });
  const markdown = buildBrandResidencesLegacyMigrationWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Already synced: ${report.summary.alreadySynced}`);
  console.log(`Migrate from legacy: ${report.summary.migrateFromLegacy}`);
  console.log(`Conflicts: ${report.summary.conflicts}`);
  console.log(`Would update: ${report.summary.wouldUpdate}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  if (report.applyGates.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyGates.applyBlockers.length}`);
  }
  console.log(`Exact apply command:`);
  console.log(report.exactApplyCommand);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
