#!/usr/bin/env node
/**
 * Choice legacy mini-batch source packages (batch-config driven).
 *
 *   npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --dry-run
 *   npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-1 --dry-run --brand comfort-inn-suites
 *
 * Apply local PDFs only:
 *   npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-source-register
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  applyMiniBatchLocalPdfRegistrations,
  buildChoiceLegacyMiniBatchMarkdown,
  buildChoiceLegacyMiniBatchReport,
} from "../lib/partner-intelligence/choice-legacy-brand-source-package-batch.js";
import {
  getBatchDefinition,
  getBatchReportFiles,
  parseBatchNameFromArgv,
} from "../lib/partner-intelligence/choice-legacy-batch-config.js";
import {
  buildMiniBatchStatusMarkdown,
  buildMiniBatchStatusReport,
  enrichMiniBatchStatusWithLiveSources,
} from "../lib/partner-intelligence/choice-legacy-mini-batch-status.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const GOVERNANCE_JSON = join(ROOT, "reports", "active-brand-governance-upgrade.json");

const batchName = parseBatchNameFromArgv();
const reportFiles = getBatchReportFiles(batchName, "sourcePackage");
const REPORT_JSON = join(ROOT, "reports", reportFiles.json);
const REPORT_MD = join(ROOT, "reports", reportFiles.md);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || process.argv.includes("--plan") || !APPLY;
const APPROVED = process.argv.includes("--approve-choice-legacy-batch-source-register");
const SKIP_LIVE_PROBE = process.argv.includes("--skip-live-probe");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function loadGovernanceReport() {
  try {
    return JSON.parse(readFileSync(GOVERNANCE_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function main() {
  if (APPLY && !APPROVED) {
    console.error(
      "[choice-legacy-brand-source-package-batch] Apply requires --approve-choice-legacy-batch-source-register"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;
  const governanceReport = loadGovernanceReport();

  console.log(
    `[choice-legacy-brand-source-package-batch] ${batchName} mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"} live_probe=${!SKIP_LIVE_PROBE}`
  );

  const report = await buildChoiceLegacyMiniBatchReport({
    governanceReport,
    probeLive: !SKIP_LIVE_PROBE,
    brandFilter,
    batchName,
  });

  let applyResult = null;
  if (APPLY) {
    applyResult = await applyMiniBatchLocalPdfRegistrations(report, { brandFilter });
    report.mode = "apply";
    report.airtableModified = applyResult.applied.length > 0;
    report.applyResult = applyResult;
    console.log(
      `[choice-legacy-brand-source-package-batch] apply applied=${applyResult.applied.length} skipped=${applyResult.skipped.length} errors=${applyResult.errors.length}`
    );
  } else {
    for (const row of report.brands) {
      console.log(
        `  ${row.brandName}: pdf=${row.pdfRegistration.registrationStatus} dev_risk=${row.developmentJsShellRisk} text=${row.localPdf.textLength}`
      );
    }
  }

  const output = { ...report, applyResult };
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(output, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacyMiniBatchMarkdown(report), "utf8");

  const statusFiles = getBatchDefinition(batchName).reports.status;
  if (statusFiles) {
    let statusReport = buildMiniBatchStatusReport({
      batchName,
      sourcePackageReport: report,
      governanceReport,
    });
    statusReport = await enrichMiniBatchStatusWithLiveSources(statusReport, batchName);
    const STATUS_JSON = join(ROOT, "reports", statusFiles.json);
    const STATUS_MD = join(ROOT, "reports", statusFiles.md);
    writeFileSync(STATUS_JSON, JSON.stringify(statusReport, null, 2), "utf8");
    writeFileSync(STATUS_MD, buildMiniBatchStatusMarkdown(statusReport), "utf8");
    console.log(`Wrote ${STATUS_MD}`);
    console.log(`Wrote ${STATUS_JSON}`);
  }

  console.log(
    `[choice-legacy-brand-source-package-batch] summary ready_pdf=${report.summary.readyForPdfRegistration} url_capture=${report.summary.needingUrlCapture}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (applyResult?.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
