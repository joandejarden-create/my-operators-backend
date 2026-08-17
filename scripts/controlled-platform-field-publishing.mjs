#!/usr/bin/env node
/**
 * Controlled Platform Field Publishing v2 — guarded single-field writes.
 *
 * Dry-run (default):
 *   npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id rec... --fact-id rec... --destination-field specificMarkets --dry-run
 *
 * Steward correction (populated destination):
 *   npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id rec... --destination-field specificMarkets --correct-value "..." --reason "..." --dry-run
 *
 * Apply (requires explicit approval — run only after steward review):
 *   npm run controlled-platform-field-publishing -- ... --apply --approve-controlled-field-publish
 *   npm run controlled-platform-field-publishing -- ... --correct-value "..." --reason "..." --apply --approve-controlled-field-correction
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPROVAL_CLI_FLAG,
  CORRECTION_APPROVAL_CLI_FLAG,
  buildControlledPublishCorrectionMarkdown,
  buildControlledPublishMarkdown,
  controlledPublishCorrectionReportFileNames,
  controlledPublishReportFileNames,
  runControlledPlatformFieldCorrection,
  runControlledPlatformFieldPublish,
} from "../lib/partner-intelligence/controlled-platform-field-publishing.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const ENTITY_TYPE = argValue("--entity-type");
const TARGET_REC_ID = argValue("--target-rec-id");
const FACT_ID = argValue("--fact-id");
const SUGGESTION_KEY = argValue("--suggestion-key");
const DESTINATION_FIELD = argValue("--destination-field");
const CORRECT_VALUE = argValue("--correct-value");
const CORRECTION_REASON = argValue("--reason");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVAL = process.argv.includes(APPROVAL_CLI_FLAG);
const CORRECTION_APPROVAL = process.argv.includes(CORRECTION_APPROVAL_CLI_FLAG);
const CORRECTION_MODE = CORRECT_VALUE !== "";

function writePublishReports(report) {
  const paths = controlledPublishReportFileNames(report.targetRecId);
  const dir = join(ROOT, "reports");
  mkdirSync(dir, { recursive: true });
  const json = JSON.stringify(report, null, 2);
  const md = buildControlledPublishMarkdown(report);
  const files = [
    join(dir, paths.latestJson),
    join(dir, paths.latestMd),
    join(dir, paths.perEntityJson),
    join(dir, paths.perEntityMd),
  ];
  writeFileSync(files[0], json, "utf8");
  writeFileSync(files[1], md, "utf8");
  writeFileSync(files[2], json, "utf8");
  writeFileSync(files[3], md, "utf8");
  return files;
}

function writeCorrectionRunReports(report) {
  const paths = controlledPublishCorrectionReportFileNames(report.targetRecId);
  const dir = join(ROOT, "reports");
  mkdirSync(dir, { recursive: true });
  const json = JSON.stringify(report, null, 2);
  const md = buildControlledPublishCorrectionMarkdown(report);
  const files = [join(dir, paths.runJson), join(dir, paths.runMd)];
  writeFileSync(files[0], json, "utf8");
  writeFileSync(files[1], md, "utf8");
  return files;
}

async function main() {
  if (!ENTITY_TYPE || !TARGET_REC_ID || !DESTINATION_FIELD) {
    console.error(
      "Usage: npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id rec... --destination-field specificMarkets [--fact-id rec...|--suggestion-key key] [--dry-run|--apply --approve-controlled-field-publish]"
    );
    console.error(
      "Correction: add --correct-value \"...\" --reason \"...\" [--apply --approve-controlled-field-correction]"
    );
    process.exit(1);
  }

  if (CORRECTION_MODE) {
    if (!CORRECTION_REASON) {
      console.error("[controlled-platform-field-publishing] correction mode requires --reason");
      process.exit(1);
    }
    if (APPLY && !CORRECTION_APPROVAL) {
      console.error(
        "[controlled-platform-field-publishing] correction --apply requires --approve-controlled-field-correction"
      );
      process.exit(1);
    }
    if (APPLY && APPROVAL) {
      console.error(
        "[controlled-platform-field-publishing] use --approve-controlled-field-correction for correction apply, not --approve-controlled-field-publish"
      );
      process.exit(1);
    }
  } else {
    if (!FACT_ID && !SUGGESTION_KEY) {
      console.error("Provide --fact-id or --suggestion-key.");
      process.exit(1);
    }
    if (APPLY && !APPROVAL) {
      console.error(
        "[controlled-platform-field-publishing] --apply requires --approve-controlled-field-publish"
      );
      process.exit(1);
    }
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  if (CORRECTION_MODE) {
    const report = await runControlledPlatformFieldCorrection({
      entityType: ENTITY_TYPE,
      targetRecId: TARGET_REC_ID,
      destinationFieldKey: DESTINATION_FIELD,
      correctValue: CORRECT_VALUE,
      reason: CORRECTION_REASON,
      apply: APPLY && CORRECTION_APPROVAL,
      approvalPresent: CORRECTION_APPROVAL,
    });
    const files = writeCorrectionRunReports(report);
    const status = report.validation.ok
      ? report.mode === "correction-apply" && report.writeResult?.ok
        ? "correction_applied"
        : "correction_eligible_dry_run"
      : "blocked";
    console.log(
      `[controlled-platform-field-publishing] mode=${report.mode} status=${status} correction=true`
    );
    if (!report.validation.ok) {
      console.log(`  failures: ${report.validation.failures.join(", ")}`);
    } else if (report.plan) {
      console.log(
        `  field=${report.plan.destinationField} prev="${String(report.plan.previousValue).slice(0, 60)}" new="${String(report.plan.newValue).slice(0, 60)}"`
      );
    }
    for (const f of files) console.log(`Wrote ${f}`);
    if (!report.validation.ok) process.exit(2);
    if (report.mode === "correction-apply" && !report.writeResult?.ok) process.exit(1);
    return;
  }

  const report = await runControlledPlatformFieldPublish({
    entityType: ENTITY_TYPE,
    targetRecId: TARGET_REC_ID,
    destinationFieldKey: DESTINATION_FIELD,
    factId: FACT_ID || null,
    suggestionKey: SUGGESTION_KEY || null,
    apply: APPLY && APPROVAL,
    approvalPresent: APPROVAL,
  });

  const files = writePublishReports(report);

  const status = report.validation.ok
    ? report.mode === "apply" && report.writeResult?.ok
      ? "applied"
      : "eligible_dry_run"
    : "blocked";

  console.log(
    `[controlled-platform-field-publishing] mode=${report.mode} status=${status} entity=${report.entityName}`
  );
  if (!report.validation.ok) {
    console.log(`  failures: ${report.validation.failures.join(", ")}`);
  } else if (report.plan) {
    console.log(`  field=${report.plan.destinationField} new="${String(report.plan.newValue).slice(0, 60)}..."`);
  }
  for (const f of files) console.log(`Wrote ${f}`);

  if (!report.validation.ok) process.exit(2);
  if (report.mode === "apply" && !report.writeResult?.ok) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
