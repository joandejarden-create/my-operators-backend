/**
 * Limited global Verified apply preview (report-only).
 */
import "../load-env.js";
import { join } from "path";
import {
  buildGlobalVerifiedApplyPlan,
  applyPlanRowToCsv,
  APPLY_PLAN_CSV_COLUMNS,
  ALLOCATION_MODE,
  resolveApplyPlanOutputSlug,
} from "../lib/independent-census/global-verified-apply-plan.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let backwardsMatchReport =
    "reports/independent-census-backwards-match-legacy-backwards-global-legacy-match-2026-05-20.json";
  let maxRecords = 250;
  let outputSlug = "";
  let batchId = "global-verified-apply-plan-001-2026-05-20";
  let allocationMode = ALLOCATION_MODE.PRIORITY_FILL;
  let countryAllocations = "";
  let countryPriority = "";
  let maxPerCountry = null;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if ((a === "--input" || a === "--backwards-match-report") && argv[i + 1])
      backwardsMatchReport = argv[++i].replace(/^"|"$/g, "");
    else if (
      a.startsWith("--input=") ||
      a.startsWith("--backwards-match-report=")
    )
      backwardsMatchReport = a
        .split("=")
        .slice(1)
        .join("=")
        .replace(/^"|"$/g, "");
    else if (a === "--max-records" && argv[i + 1])
      maxRecords = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-records="))
      maxRecords = parseInt(a.slice("--max-records=".length), 10);
    else if (a === "--max-promotions" && argv[i + 1])
      maxRecords = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-promotions="))
      maxRecords = parseInt(a.slice("--max-promotions=".length), 10);
    else if (a === "--batch-id" && argv[i + 1])
      batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--output-slug" && argv[i + 1])
      outputSlug = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--output-slug="))
      outputSlug = a.slice("--output-slug=".length).replace(/^"|"$/g, "");
    else if (a === "--allocation-mode" && argv[i + 1])
      allocationMode = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--allocation-mode="))
      allocationMode = a.slice("--allocation-mode=".length).toLowerCase();
    else if (a === "--country-allocations" && argv[i + 1])
      countryAllocations = argv[++i];
    else if (a.startsWith("--country-allocations="))
      countryAllocations = a.slice("--country-allocations=".length);
    else if (a === "--country-priority" && argv[i + 1])
      countryPriority = argv[++i];
    else if (a.startsWith("--country-priority="))
      countryPriority = a.slice("--country-priority=".length);
    else if (a === "--max-per-country" && argv[i + 1])
      maxPerCountry = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-per-country="))
      maxPerCountry = parseInt(a.slice("--max-per-country=".length), 10);
  }

  if (
    allocationMode !== ALLOCATION_MODE.PRIORITY_FILL &&
    allocationMode !== ALLOCATION_MODE.COUNTRY_BALANCED
  ) {
    throw new Error(
      `Unknown --allocation-mode "${allocationMode}" (use priority-fill or country-balanced)`
    );
  }

  if (!outputSlug) {
    outputSlug = resolveApplyPlanOutputSlug(batchId, allocationMode);
  }

  return {
    backwardsMatchReportPath: join(process.cwd(), backwardsMatchReport),
    maxRecords,
    outputSlug,
    applyPlanBatchId: batchId,
    allocationMode,
    countryAllocations,
    countryPriority,
    maxPerCountry,
  };
}

async function main() {
  const args = parseArgs();
  console.log("=== Global Verified apply plan (preview only) ===\n");
  console.log(`Allocation mode:   ${args.allocationMode}`);
  console.log(`Max records:       ${args.maxRecords}`);
  console.log(`Batch ID:          ${args.applyPlanBatchId}`);
  console.log(`Input report:      ${args.backwardsMatchReportPath}\n`);

  const result = buildGlobalVerifiedApplyPlan({
    backwardsMatchReportPath: args.backwardsMatchReportPath,
    maxRecords: args.maxRecords,
    applyPlanBatchId: args.applyPlanBatchId,
    allocationMode: args.allocationMode,
    countryAllocations: args.countryAllocations,
    countryPriority: args.countryPriority,
    maxPerCountry: args.maxPerCountry,
  });

  const jsonPath = join(REPORTS_DIR, `${args.outputSlug}.json`);
  const csvPath = join(REPORTS_DIR, `${args.outputSlug}.csv`);

  writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "global-verified-apply-plan",
    ...result,
    reportFiles: { json: jsonPath, csv: csvPath },
  });
  writeCsv(csvPath, result.planRows.map(applyPlanRowToCsv), APPLY_PLAN_CSV_COLUMNS);

  console.log(`Total report rows:       ${result.totalReportRows}`);
  console.log(`Eligible (promotable):   ${result.eligibleBeforeCap}`);
  console.log(`Apply plan count:        ${result.applyPlanCount}`);
  console.log(`Skipped already verified: ${result.skippedAlreadyVerified}`);
  console.log(`Skipped duplicate/hold:   ${result.skippedDuplicateHold}`);
  console.log(`Skipped other ineligible: ${result.skippedOtherIneligible}`);

  if (result.allocationDetail?.selectedByCountry) {
    console.log("\nSelected by country (targets in allocationDetail):");
    for (const co of result.countryOrder) {
      const sel = result.allocationDetail.selectedByCountry[co] || 0;
      const tgt = result.allocationDetail.targetByCountry?.[co] ?? 0;
      const elig = result.eligibleByCountry[co] || 0;
      console.log(`  ${co}: ${sel} selected (target ${tgt}, eligible ${elig})`);
    }
  } else {
    console.log("\nEligible by country:");
    for (const [co, count] of Object.entries(result.eligibleByCountry).sort(
      (a, b) => b[1] - a[1]
    )) {
      console.log(`  ${co}: ${count}`);
    }
  }

  console.log("\nApply plan by country:");
  for (const [co, count] of Object.entries(result.byCountry).sort(
    (a, b) => b[1] - a[1]
  )) {
    console.log(`  ${co}: ${count}`);
  }

  console.log(`\nJSON: ${jsonPath}`);
  console.log(`CSV:  ${csvPath}`);
  console.log("\n✓ No Airtable writes. Review before any --apply.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
