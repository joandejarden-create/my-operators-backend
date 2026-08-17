/**
 * Plan Hilton directory enrichment for all Hilton brands with census rows.
 *
 *   node scripts/plan-hilton-census-enrichment-all.mjs
 *   node scripts/plan-hilton-census-enrichment-all.mjs --apply
 *   node scripts/plan-hilton-census-enrichment-all.mjs --brand-codes HP,GI,HI
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";
import { loadHiltonBrandDirectoryConfigs } from "../lib/hilton-brand-registry.js";
import { planHiltonBrandEnrichment } from "../lib/hotel-census/plan-hilton-brand-enrichment.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const COMBINED_JSON = join(REPORTS, "hilton-census-enrichment-plan-all-brands.json");
const COMBINED_CSV = join(REPORTS, "hilton-census-enrichment-plan-all-brands.csv");

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  const codes = get("--brand-codes");
  return {
    minConfidence: get("--min-confidence") || "low",
    apply: args.includes("--apply"),
    dryRunApply: args.includes("--dry-run-apply"),
    brandCodes: codes ? codes.split(",").map((s) => s.trim().toUpperCase()) : null,
    crawlDelayMs: Number(get("--crawl-delay-ms") || 200),
    skipZeroCensus: !args.includes("--include-zero-census"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function runApply(planPath, dryRun) {
  return new Promise((resolve, reject) => {
    const args = ["scripts/apply-hilton-census-enrichment.mjs", "--input", planPath];
    if (dryRun) args.push("--dry-run");
    const child = spawn(process.execPath, args, {
      cwd: join(__dirname, ".."),
      stdio: "inherit",
      shell: false,
    });
    child.on("exit", (code) => (code === 0 ? resolve() : reject(new Error(`apply exit ${code}`))));
  });
}

async function main() {
  const { minConfidence, apply, dryRunApply, brandCodes, crawlDelayMs, skipZeroCensus } =
    parseArgs();

  console.log("=== Plan Hilton Census Enrichment — ALL BRANDS ===\n");

  const brandConfigs = await loadHiltonBrandDirectoryConfigs({ brandCodes });
  console.log(`Brands on hilton.com with hotels: ${brandConfigs.length}\n`);

  const brandReports = [];
  const allPlanRows = [];
  const usedCensusIds = new Set();

  for (const brandConfig of brandConfigs) {
    console.log(`\n--- ${brandConfig.canonicalBrandName} (${brandConfig.brandCode}) ---`);
    const report = await planHiltonBrandEnrichment({
      brandConfig,
      minConfidence,
      crawlDelayMs,
      onProgress: (msg) => {
        if (msg.startsWith("Fetching index") || msg.includes("ERROR")) {
          console.log(" ", msg);
        }
      },
    });

    if (report.skippedReason === "no_census_rows_for_brand") {
      console.log("  Skip: no census rows for this brand");
      if (skipZeroCensus) continue;
    }

    console.log(
      `  Directory: ${report.crawlSummary.hotelsFound} | Census: ${report.censusRowsLoaded} | Matched: ${report.matched} | Ready: ${report.readyToApply} | Unmatched census: ${report.unmatchedCensus.length}`
    );

    for (const row of report.planRows) {
      if (row.censusRecordId && usedCensusIds.has(row.censusRecordId)) {
        console.warn(
          `  WARN duplicate census match skipped: ${row.censusName} [${row.censusRecordId}]`
        );
        continue;
      }
      if (row.censusRecordId) usedCensusIds.add(row.censusRecordId);
      allPlanRows.push(row);
    }

    brandReports.push({
      brand: report.brand,
      brandCode: brandConfig.brandCode,
      censusRowsLoaded: report.censusRowsLoaded,
      matched: report.matched,
      readyToApply: report.readyToApply,
      noChanges: report.noChanges,
      unmatchedCensus: report.unmatchedCensus.length,
      directoryHotels: report.crawlSummary.hotelsFound,
      fetchErrors: report.crawlSummary.fetchErrors.length,
      skippedReason: report.skippedReason || null,
    });
  }

  const combined = {
    generatedAt: new Date().toISOString(),
    scope: "all_hilton_brands",
    parentCompany: "Hilton",
    minConfidence,
    brandCount: brandReports.length,
    brands: brandReports,
    censusRowsLoaded: brandReports.reduce((s, b) => s + b.censusRowsLoaded, 0),
    matched: allPlanRows.filter((r) => r.censusRecordId).length,
    readyToApply: allPlanRows.filter((r) => r.status === "ready").length,
    noChanges: allPlanRows.filter((r) => r.status === "no_changes").length,
    unmatchedDirectory: allPlanRows.filter((r) => r.status === "unmatched_directory").length,
    alias: { affiliationMatchers: ["per-brand in planRows.brand"] },
    planRows: allPlanRows,
  };

  mkdirSync(REPORTS, { recursive: true });
  writeFileSync(COMBINED_JSON, JSON.stringify(combined, null, 2), "utf8");

  const csvHeaders = [
    "brand",
    "brandCode",
    "status",
    "censusRecordId",
    "censusName",
    "directoryName",
    "directoryBrandPropertyCode",
    "matchConfidence",
    "matchScore",
    "fieldsToApply",
  ];
  writeFileSync(
    COMBINED_CSV,
    `${csvHeaders.join(",")}\n${allPlanRows
      .map((r) =>
        [
          r.brand,
          r.brandCode,
          r.status,
          r.censusRecordId,
          r.censusName,
          r.directoryName,
          r.directoryBrandPropertyCode,
          r.matchConfidence,
          r.matchScore,
          Object.keys(r.applyFields || {}).join("; "),
        ]
          .map(csvEscape)
          .join(",")
      )
      .join("\n")}\n`,
    "utf8"
  );

  console.log("\n=== COMBINED SUMMARY ===");
  console.log(`  Brands processed: ${brandReports.length}`);
  console.log(`  Total census rows touched: ${combined.censusRowsLoaded}`);
  console.log(`  Matched: ${combined.matched}`);
  console.log(`  Ready to apply: ${combined.readyToApply}`);
  console.log(`  No changes: ${combined.noChanges}`);
  console.log(`\nJSON: ${COMBINED_JSON}`);
  console.log(`CSV:  ${COMBINED_CSV}`);

  console.log("\nPer brand:");
  for (const b of brandReports) {
    if (!b.censusRowsLoaded && b.skippedReason) continue;
    console.log(
      `  ${b.brandCode} ${b.brand}: census ${b.censusRowsLoaded} → ready ${b.readyToApply} (dir ${b.directoryHotels})`
    );
  }

  if (apply || dryRunApply) {
    console.log(`\n=== ${dryRunApply ? "DRY-RUN APPLY" : "APPLY"} ===`);
    await runApply(COMBINED_JSON, dryRunApply);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
