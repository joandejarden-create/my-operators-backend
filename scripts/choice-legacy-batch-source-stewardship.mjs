#!/usr/bin/env node
/**
 * Choice legacy mini-batch source stewardship.
 *
 *   npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --dry-run
 *   npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  applyChoiceLegacyBatchStewardship,
  buildChoiceLegacyBatchStewardshipMarkdown,
  buildChoiceLegacyBatchStewardshipReport,
} from "../lib/partner-intelligence/choice-legacy-batch-source-stewardship.js";
import {
  getBatchReportFiles,
  parseBatchNameFromArgv,
} from "../lib/partner-intelligence/choice-legacy-batch-config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const batchName = parseBatchNameFromArgv();
const reportFiles = getBatchReportFiles(batchName, "stewardship");
const REPORT_JSON = join(ROOT, "reports", reportFiles.json);
const REPORT_MD = join(ROOT, "reports", reportFiles.md);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVED = process.argv.includes("--approve-choice-legacy-batch-stewardship");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

async function main() {
  if (APPLY && !APPROVED) {
    console.error(
      "[choice-legacy-batch-source-stewardship] Apply requires --approve-choice-legacy-batch-stewardship"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;

  console.log(
    `[choice-legacy-batch-source-stewardship] ${batchName} mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"}`
  );

  const report = await buildChoiceLegacyBatchStewardshipReport({ brandFilter, batchName });

  let applyResult = null;
  if (APPLY) {
    if (report.summary.sourcesEligibleForApproval === 0) {
      console.error(
        "[choice-legacy-batch-source-stewardship] Apply rejected: no eligible sources for batch approval."
      );
      process.exit(1);
    }
    applyResult = await applyChoiceLegacyBatchStewardship(report, { brandFilter });
    if (applyResult.rejected) {
      console.error("[choice-legacy-batch-source-stewardship] Apply rejected:", applyResult.errors);
      process.exit(1);
    }
    report.mode = "apply";
    report.airtableModified = applyResult.applied.length > 0;
    report.applyResult = applyResult;
    console.log(
      `[choice-legacy-batch-source-stewardship] apply applied=${applyResult.applied.length} skipped=${applyResult.skipped.length} errors=${applyResult.errors.length}`
    );
  } else {
    for (const brand of report.brands) {
      console.log(
        `  ${brand.brandName}: sources=${brand.sourcesFound} eligible=${brand.eligibleCount} skipped=${brand.skippedCount}`
      );
    }
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify({ ...report, applyResult }, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacyBatchStewardshipMarkdown(report), "utf8");

  console.log(
    `[choice-legacy-batch-source-stewardship] summary eligible=${report.summary.sourcesEligibleForApproval} skipped=${report.summary.sourcesSkipped}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (applyResult?.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
