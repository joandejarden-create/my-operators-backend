#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerScreenshotSeededRemediationReviewPackageMarkdown,
  buildBrandExplorerScreenshotSeededRemediationReviewPackageReport,
} from "../lib/partner-intelligence/brand-explorer-screenshot-seeded-remediation-review-package.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  if (process.argv.includes("--apply")) {
    console.error("Screenshot-seeded remediation package is read-only. Use --dry-run.");
    process.exit(1);
  }

  const report = await buildBrandExplorerScreenshotSeededRemediationReviewPackageReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerScreenshotSeededRemediationReviewPackageMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Screenshot defects confirmed: ${report.humanDefectsConfirmedCount}/${report.humanDefectsTotalChecks}`);
  console.log(`Copy-safe slots: ${report.copyCleanupSafe.length}`);
  console.log(`Standards safe now: ${report.sectionSafetyGates.standardsTableCanBeSafelyBuiltNow}`);
  console.log(`Loyalty complete now: ${report.sectionSafetyGates.loyaltySectionCanBeSafelyCompletedNow}`);
  console.log(`Next: ${report.recommendedNextBatch}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
