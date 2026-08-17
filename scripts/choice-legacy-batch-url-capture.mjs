#!/usr/bin/env node
/**
 * Choice legacy mini-batch URL capture (consumer + press).
 *
 *   npm run choice-legacy-batch-url-capture -- --batch mini-batch-2 --dry-run
 *   npm run choice-legacy-batch-url-capture -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-url-capture
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  applyChoiceLegacyBatchUrlCapture,
  buildChoiceLegacyBatchUrlCaptureMarkdown,
  buildChoiceLegacyBatchUrlCaptureReport,
} from "../lib/partner-intelligence/choice-legacy-batch-url-capture.js";
import {
  getBatchReportFiles,
  parseBatchNameFromArgv,
} from "../lib/partner-intelligence/choice-legacy-batch-config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const batchName = parseBatchNameFromArgv();
const reportFiles = getBatchReportFiles(batchName, "urlCapture");
const REPORT_JSON = join(ROOT, "reports", reportFiles.json);
const REPORT_MD = join(ROOT, "reports", reportFiles.md);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVED = process.argv.includes("--approve-choice-legacy-batch-url-capture");
const SKIP_PROBE = process.argv.includes("--skip-url-probe");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

async function main() {
  if (APPLY && !APPROVED) {
    console.error(
      "[choice-legacy-batch-url-capture] Apply requires --approve-choice-legacy-batch-url-capture"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;

  console.log(
    `[choice-legacy-batch-url-capture] ${batchName} mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"} probe=${!SKIP_PROBE}`
  );

  const report = await buildChoiceLegacyBatchUrlCaptureReport({
    brandFilter,
    probeUrls: !SKIP_PROBE,
    batchName,
  });

  let applyResult = null;
  if (APPLY) {
    applyResult = await applyChoiceLegacyBatchUrlCapture(report, { brandFilter, batchName });
    report.mode = "apply";
    report.airtableModified = applyResult.captured.length > 0;
    report.applyResult = applyResult;
    report.summary.captured = applyResult.captured.length;
    report.summary.skippedDuplicates = applyResult.skippedDuplicates.length;
    report.summary.failed = applyResult.failed.length;
    report.summary.readyToCapture = report.urls.filter((u) => u.status === "ready_to_capture").length;

    for (const row of applyResult.captured) {
      const idx = report.urls.findIndex(
        (u) => u.brandKey === row.brandKey && u.slot === row.slot && u.sourceUrl === row.sourceUrl
      );
      if (idx >= 0) report.urls[idx] = row;
    }
    for (const row of [...applyResult.skippedDuplicates, ...applyResult.failed]) {
      const idx = report.urls.findIndex(
        (u) => u.brandKey === row.brandKey && u.slot === row.slot && u.sourceUrl === row.sourceUrl
      );
      if (idx >= 0) report.urls[idx] = { ...report.urls[idx], ...row };
    }

    console.log(
      `[choice-legacy-batch-url-capture] apply captured=${applyResult.captured.length} skipped_duplicates=${applyResult.skippedDuplicates.length} failed=${applyResult.failed.length}`
    );
  } else {
    for (const row of report.urls) {
      console.log(
        `  ${row.brand} [${row.slot}]: status=${row.status} http=${row.httpStatus ?? "—"} bytes=${row.bytes ?? "—"} dup=${row.duplicateCheck?.isDuplicate ? "yes" : "no"}`
      );
    }
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify({ ...report, applyResult }, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacyBatchUrlCaptureMarkdown(report), "utf8");

  console.log(
    `[choice-legacy-batch-url-capture] summary planned=${report.summary.totalUrlsPlanned} ready=${report.summary.readyToCapture} failed=${report.summary.failed}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (applyResult?.failed?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
