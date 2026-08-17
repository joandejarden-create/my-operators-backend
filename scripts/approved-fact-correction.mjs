#!/usr/bin/env node
/**
 * Approved Fact Correction v1 — steward-reviewed PI fact value corrections.
 *
 * Dry-run (default):
 *   npm run approved-fact-correction -- --fact-id rec... --correct-value "..." --reason "..." [--evidence-source-id rec...] --dry-run
 *
 * Apply (founder/steward approval only):
 *   npm run approved-fact-correction -- ... --apply --approve-approved-fact-correction
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPROVAL_CLI_FLAG,
  buildApprovedFactCorrectionMarkdown,
  factCorrectionReportFileNames,
  runApprovedFactCorrection,
} from "../lib/partner-intelligence/approved-fact-correction.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const FACT_ID = argValue("--fact-id");
const CORRECT_VALUE = argValue("--correct-value");
const REASON = argValue("--reason");
const EVIDENCE_SOURCE_ID = argValue("--evidence-source-id") || null;
const APPLY = process.argv.includes("--apply");
const APPROVAL = process.argv.includes(APPROVAL_CLI_FLAG);

function writeReports(report) {
  const paths = factCorrectionReportFileNames(report.factId);
  const dir = join(ROOT, "reports");
  mkdirSync(dir, { recursive: true });
  const json = JSON.stringify(report, null, 2);
  const md = buildApprovedFactCorrectionMarkdown(report);
  const files = [
    join(dir, paths.latestJson),
    join(dir, paths.latestMd),
    join(dir, paths.perFactJson),
    join(dir, paths.perFactMd),
  ];
  writeFileSync(files[0], json, "utf8");
  writeFileSync(files[1], md, "utf8");
  writeFileSync(files[2], json, "utf8");
  writeFileSync(files[3], md, "utf8");
  return files;
}

async function main() {
  if (!FACT_ID || !CORRECT_VALUE || !REASON) {
    console.error(
      "Usage: npm run approved-fact-correction -- --fact-id rec... --correct-value \"...\" --reason \"...\" [--evidence-source-id rec...] [--dry-run|--apply --approve-approved-fact-correction]"
    );
    process.exit(1);
  }

  if (APPLY && !APPROVAL) {
    console.error(
      "[approved-fact-correction] --apply requires --approve-approved-fact-correction"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await runApprovedFactCorrection({
    factId: FACT_ID,
    correctValue: CORRECT_VALUE,
    reason: REASON,
    evidenceSourceId: EVIDENCE_SOURCE_ID,
    apply: APPLY && APPROVAL,
    approvalPresent: APPROVAL,
  });

  const files = writeReports(report);

  const status = report.validation.ok
    ? report.mode === "apply" && report.writeResult?.ok
      ? "applied"
      : "eligible_dry_run"
    : "blocked";

  console.log(
    `[approved-fact-correction] mode=${report.mode} status=${status} fact=${FACT_ID}`
  );
  if (report.fact?.fieldName) {
    console.log(`  key=${report.fact.fieldName}`);
  }
  if (!report.validation.ok) {
    console.log(`  failures: ${report.validation.failures.join(", ")}`);
  } else if (report.plan) {
    console.log(
      `  approved: "${String(report.plan.previousApprovedValue).slice(0, 50)}" → "${String(report.plan.correctedApprovedValue).slice(0, 50)}"`
    );
    console.log(`  status→${report.plan.humanReviewStatus}`);
  }
  for (const f of files) console.log(`Wrote ${f}`);

  if (!report.validation.ok) process.exit(2);
  if (report.mode === "apply" && !report.writeResult?.ok) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
