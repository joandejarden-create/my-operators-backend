#!/usr/bin/env node
/**
 * Brand Asset Human Review Readiness v4.
 *
 *   npm run brand-asset-human-review-readiness -- --brand tribute-portfolio --dry-run
 *
 * Report-only — does not approve assets, download images, or write Brand Setup media fields.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildHumanReviewReadinessMarkdown,
  buildHumanReviewReadinessReport,
  BRAND_ASSET_PILOT_CONFIG,
} from "../lib/partner-intelligence/brand-asset-human-review-readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function parseBrandArg() {
  const idx = process.argv.indexOf("--brand");
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return "tribute-portfolio";
}

async function main() {
  if (process.argv.includes("--apply")) {
    console.error(
      "[brand-asset-human-review-readiness] This module is report-only. No apply gate exists in v4."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandKey = parseBrandArg();
  console.log(`[brand-asset-human-review-readiness] brand=${brandKey} mode=dry-run`);

  const report = await buildHumanReviewReadinessReport({ brandKey });

  if (report.registryReadError) {
    console.error(report.registryReadError);
    process.exit(1);
  }

  console.log(
    `  records=${report.totalRecordsScanned} primaries=${report.primaryCandidatesScanned} ready=${report.readyForHumanReview.length} needs_metadata=${report.needsMoreMetadata.length} needs_source=${report.needsSourceReview.length} needs_visual=${report.needsVisualInspection.length} not_ready=${report.notReady.length} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildHumanReviewReadinessMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
