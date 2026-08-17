#!/usr/bin/env node
/**
 * Choice legacy mini-batch brand extraction.
 *
 *   npm run choice-legacy-batch-extract -- --batch mini-batch-2 --dry-run
 *   npm run choice-legacy-batch-extract -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-extract
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { listPartnerFacts } from "../lib/partner-intelligence/airtable-facts.js";
import {
  applyChoiceLegacyBatchExtract,
  buildChoiceLegacyBatchExtractMarkdown,
  buildChoiceLegacyBatchExtractReport,
  getBatchExtractBrandConfigs,
} from "../lib/partner-intelligence/choice-legacy-batch-extract.js";
import {
  getBatchReportFiles,
  parseBatchNameFromArgv,
} from "../lib/partner-intelligence/choice-legacy-batch-config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const batchName = parseBatchNameFromArgv();
const reportFiles = getBatchReportFiles(batchName, "extract");
const REPORT_JSON = join(ROOT, "reports", reportFiles.json);
const REPORT_MD = join(ROOT, "reports", reportFiles.md);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE = process.argv.includes("--approve-choice-legacy-batch-extract");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function parseFactKeyList(raw) {
  if (!raw) return [];
  return String(raw)
    .split(/[,\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

async function fetchAllBrandFacts(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function main() {
  if (APPLY && !APPROVE) {
    console.error(
      "[choice-legacy-batch-extract] Apply requires --approve-choice-legacy-batch-extract"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;
  const targetKeys = parseFactKeyList(argValue("--fact-keys"));
  const limitFacts = Math.max(0, Number(argValue("--limit-facts") || "0") || 0) || null;

  console.log(
    `[choice-legacy-batch-extract] ${batchName} mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"}`
  );

  const brandConfigs = getBatchExtractBrandConfigs(batchName, brandFilter);
  const existingFactsByBrand = new Map();
  for (const brandConfig of brandConfigs) {
    existingFactsByBrand.set(brandConfig.recordId, await fetchAllBrandFacts(brandConfig.recordId));
  }

  const report = await buildChoiceLegacyBatchExtractReport({
    brandFilter,
    batchName,
    targetKeys,
    limitFacts,
    existingFactsByBrand,
  });

  let applyResult = null;
  if (APPLY && APPROVE) {
    applyResult = await applyChoiceLegacyBatchExtract({
      brandReports: report.brands,
      targetKeys: report.targetFactKeys,
      limitFacts,
      batchName,
    });
    report.mode = "apply";
    report.airtableModified = applyResult.factsCreated.length > 0;
    report.applyResult = applyResult;
    console.log(
      `[choice-legacy-batch-extract] apply runId=${applyResult.runId} facts=${applyResult.factsCreated.length}`
    );
  } else {
    for (const brand of report.brands) {
      console.log(
        `  ${brand.brandName}: sources=${brand.sourcesInScope.length} proposed=${brand.wouldWrite.factsWouldCreateCount} quality=${brand.extractionQuality.overall}`
      );
    }
  }

  const output = {
    ...report,
    sourcePreviews: report.brands.map((b) => ({
      brandName: b.brandName,
      previews: (b.sourcePreviews || []).map((p) => ({
        ...p,
        previewCandidates: (p.previewCandidates || []).map(({ _candidate, ...rest }) => rest),
      })),
    })),
    brands: report.brands.map((b) => ({
      ...b,
      sourcePreviews: (b.sourcePreviews || []).map((p) => ({
        ...p,
        previewCandidates: (p.previewCandidates || []).map(({ _candidate, ...rest }) => rest),
      })),
      wouldWrite: {
        ...b.wouldWrite,
        proposedCandidates: undefined,
      },
      sources: b.sources,
    })),
    applyResult,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(output, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacyBatchExtractMarkdown(report), "utf8");

  console.log(
    `[choice-legacy-batch-extract] summary brands=${report.summary.totalBrands} proposed=${report.summary.totalProposedFacts} ready=${report.summary.brandsReadyForBatchApply}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
