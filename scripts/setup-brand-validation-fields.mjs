#!/usr/bin/env node
/**
 * Ensure P1 profile-level governance fields on Brand Setup root tables.
 * Idempotent — skips fields that already exist or have documented aliases.
 *
 * Tables:
 *   - Brand Setup - Brand Basics
 *   - Brand Setup - Brand Explorer Presentation
 *
 * Excluded (Partner Intelligence SSOT): Source URL / File Path, Source Date
 *
 * Usage:
 *   node scripts/setup-brand-validation-fields.mjs --dry-run
 *   node scripts/setup-brand-validation-fields.mjs --apply
 *
 * Requires:
 *   AIRTABLE_API_KEY with schema.bases:read (+ write for --apply)
 *   AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { P1_BRAND_TABLES } from "../lib/brand-operator-validation-audit/p1-profile-governance-field-specs.js";
import { runP1GovernanceSetup } from "../lib/brand-operator-validation-audit/run-p1-governance-setup.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "brand-validation-fields-setup.json");
const REPORT_MD = join(ROOT, "reports", "brand-validation-fields-setup.md");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

async function main() {
  const report = await runP1GovernanceSetup({
    scriptName: "setup-brand-validation-fields",
    title: "Brand P1 Profile Governance Fields Setup",
    tableSpecs: P1_BRAND_TABLES,
    reportJsonPath: REPORT_JSON,
    reportMdPath: REPORT_MD,
    root: ROOT,
    dryRun: DRY_RUN,
  });
  if (report.failed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
