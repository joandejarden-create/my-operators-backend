#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG,
  BATCH_REPORT_JSON_NAME,
  BATCH_REPORT_MD_NAME,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBatchMarkdown,
  buildBrandExplorerCompleteBuildOrchestratorMarkdown,
  buildBrandExplorerCompleteBuildOrchestratorReport,
  buildPerBrandMarkdown,
  perBrandReportBasename,
  resolveMaxConcurrency,
} from "../lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const BATCH_JSON = join(ROOT, "reports", BATCH_REPORT_JSON_NAME);
const BATCH_MD = join(ROOT, "reports", BATCH_REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const applyApproved = hasFlag("--apply-approved");
  const dryRun = hasFlag("--dry-run") || !applyApproved;
  const allActive = hasFlag("--all-active");
  const brandsArg = argValue("--brands", "");
  const brands = brandsArg
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean);
  const brandIdOrName =
    brands.length || allActive ? "" : argValue("--brand", "tribute-portfolio");
  let maxConcurrency = Number.parseInt(argValue("--max-concurrency", "1"), 10) || 1;

  if (applyApproved && !hasFlag(APPLY_FLAG)) {
    console.error(`Apply-approved requires ${APPLY_FLAG}`);
    process.exit(1);
  }

  if (applyApproved && maxConcurrency > 1) {
    console.warn("Apply-approved mode forces --max-concurrency 1");
    maxConcurrency = 1;
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName,
    brands,
    allActive,
    targetQuality: argValue("--target-quality", "active-profile"),
    stopOnCritical: hasFlag("--stop-on-critical") || !hasFlag("--continue-through-warnings"),
    continueThroughWarnings: hasFlag("--continue-through-warnings"),
    generateNextWriters: hasFlag("--generate-next-writers"),
    applyApproved: applyApproved && !dryRun,
    maxConcurrency: resolveMaxConcurrency({ maxConcurrency, applyApproved: applyApproved && !dryRun }),
  });

  const markdown = buildBrandExplorerCompleteBuildOrchestratorMarkdown(report);
  const batchMarkdown = report.batchMode ? buildBatchMarkdown(report) : markdown;

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");

  if (report.batchMode) {
    writeFileSync(BATCH_JSON, JSON.stringify(report, null, 2), "utf8");
    writeFileSync(BATCH_MD, batchMarkdown, "utf8");
  }

  for (const brandResult of report.brandResults || []) {
    const basename = brandResult.reportBasename || perBrandReportBasename(brandResult.brand.slug);
    const perBrandJson = join(ROOT, "reports", `${basename}.json`);
    const perBrandMd = join(ROOT, "reports", `${basename}.md`);
    const perBrandPayload = {
      generatedAt: report.generatedAt,
      mode: report.mode,
      orchestratorVersion: report.orchestratorVersion,
      brand: brandResult.brand,
      ...brandResult,
    };
    writeFileSync(perBrandJson, JSON.stringify(perBrandPayload, null, 2), "utf8");
    writeFileSync(perBrandMd, buildPerBrandMarkdown(brandResult), "utf8");
  }

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  if (report.batchMode) {
    console.log(`Wrote ${BATCH_MD}`);
    console.log(`Wrote ${BATCH_JSON}`);
    console.log(`Per-brand reports: ${report.perBrandReports?.length || 0}`);
  }
  console.log(`Mode: ${report.mode}`);
  console.log(`Brands: ${report.brandsRequested?.join(", ") || brandIdOrName}`);
  console.log(`Max concurrency: ${report.maxConcurrency}`);
  console.log(`Batch mode: ${report.batchMode ? "yes" : "no"}`);
  if (report.batchAggregate) {
    console.log(`Ready: ${report.batchAggregate.brandsReady?.length || 0}`);
    console.log(`Almost ready: ${report.batchAggregate.brandsAlmostReady?.length || 0}`);
    console.log(`Blocked: ${report.batchAggregate.brandsBlocked?.length || 0}`);
  }
  const tribute = (report.brandResults || []).find((b) => b.brand?.slug === "tribute-portfolio");
  if (tribute) {
    console.log(`Tribute active-profile ready: ${tribute.readyForActiveProfile ? "yes" : "no"}`);
  }
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Workflow: ${report.exactWorkflowCommand}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
