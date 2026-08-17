#!/usr/bin/env node
/**
 * Ascend Hotel Collection — source gap resolution (dry-run default).
 *
 *   npm run ascend-source-gap-resolution -- --dry-run
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildAscendSourceGapResolutionMarkdown,
  buildAscendSourceGapResolutionReport,
} from "../lib/partner-intelligence/ascend-source-gap-resolution.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const GOVERNANCE_JSON = join(ROOT, "reports", "active-brand-governance-upgrade.json");

const SKIP_URL_PROBE = process.argv.includes("--skip-url-probe");
const SKIP_DEV_PROBE = process.argv.includes("--skip-dev-probe");

function loadGovernanceReport() {
  try {
    return JSON.parse(readFileSync(GOVERNANCE_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const governanceReport = loadGovernanceReport();
  const governanceRow =
    governanceReport?.brands?.find((b) => b.recordId === "reclkgOzvAcBheUSo") || null;

  console.log("[ascend-source-gap-resolution] dry-run probe=" + !SKIP_URL_PROBE);

  const report = await buildAscendSourceGapResolutionReport({
    governanceRow,
    probeUrls: !SKIP_URL_PROBE,
    probeDevelopment: !SKIP_DEV_PROBE,
  });

  console.log(
    `  local_pdfs=${report.localFiles.localPdfCount} consumer_text=${report.urls.consumer.probe?.readableTextLength ?? "—"} press_text=${report.urls.pressKit.probe?.readableTextLength ?? "—"}`
  );
  console.log(
    `  recommendation=${report.sourcePackageRecommendation.recommendation} pipeline_ready=${report.canProceedThroughPipeline}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildAscendSourceGapResolutionMarkdown(report), "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
