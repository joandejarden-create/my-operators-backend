#!/usr/bin/env node
/**
 * Ensure P1 profile-level governance fields on Operator Setup root tables.
 * Idempotent — skips fields that already exist or have documented aliases.
 *
 * Tables:
 *   - Operator Setup - Master
 *   - Operator Setup - Explorer Materials
 *
 * Alias handling:
 *   - Source Type — skip if exact column exists on Master
 *   - Data Confidence Level — treated as partial equivalent for Confidence Level (no duplicate)
 *   - Last Updated Date — not equivalent to Last Reviewed Date (will create if missing)
 *
 * Excluded (Partner Intelligence SSOT): Source URL / File Path, Source Date
 *
 * Usage:
 *   node scripts/setup-operator-validation-fields.mjs --dry-run
 *   node scripts/setup-operator-validation-fields.mjs --apply
 *
 * Requires:
 *   AIRTABLE_API_KEY with schema.bases:read (+ write for --apply)
 *   AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { P1_OPERATOR_TABLES } from "../lib/brand-operator-validation-audit/p1-profile-governance-field-specs.js";
import { runP1GovernanceSetup } from "../lib/brand-operator-validation-audit/run-p1-governance-setup.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "operator-validation-fields-setup.json");
const REPORT_MD = join(ROOT, "reports", "operator-validation-fields-setup.md");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

async function main() {
  const report = await runP1GovernanceSetup({
    scriptName: "setup-operator-validation-fields",
    title: "Operator P1 Profile Governance Fields Setup",
    tableSpecs: P1_OPERATOR_TABLES,
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
