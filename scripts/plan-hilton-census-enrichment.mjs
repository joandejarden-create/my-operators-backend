/**
 * Plan Hilton directory → Hotel Census enrichment (dry-run; no Airtable writes).
 *
 *   node scripts/plan-hilton-census-enrichment.mjs --brand "Curio Collection by Hilton"
 *   node scripts/plan-hilton-census-enrichment.mjs --brand "Curio Collection by Hilton" --min-confidence medium
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { findBrandConfig, loadHiltonBrandDirectoryConfigs } from "../lib/hilton-brand-registry.js";
import { planHiltonBrandEnrichment } from "../lib/hotel-census/plan-hilton-brand-enrichment.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    brand: get("--brand") || "Curio Collection by Hilton",
    parentCompany: get("--parent-company") || "Hilton",
    minConfidence: get("--min-confidence") || "low",
    slug: get("--slug"),
  };
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const { brand, parentCompany, minConfidence, slug } = parseArgs();
  const configs = await loadHiltonBrandDirectoryConfigs();
  const brandConfig = findBrandConfig(brand, configs);
  if (!brandConfig) {
    throw new Error(`Unknown Hilton brand "${brand}". Use a formal name, slug, or brand code.`);
  }

  console.log(`=== Plan Hilton Census Enrichment: ${brandConfig.canonicalBrandName} ===\n`);

  const brandReport = await planHiltonBrandEnrichment({
    brandConfig,
    parentCompany,
    minConfidence,
    onProgress: (msg) => console.log(" ", msg),
  });

  if (brandReport.alias?.warnings?.length) {
    for (const w of brandReport.alias.warnings) console.warn("  alias warning:", w);
  }
  console.log("Affiliation matchers:", brandReport.alias.affiliationMatchers.join(" | "));
  console.log(
    `\nDirectory: ${brandReport.crawlSummary.hotelsFound} hotels (${brandReport.crawlSummary.countryPages} country pages + index)`
  );
  if (brandReport.crawlSummary.fetchErrors.length) {
    console.warn(`Fetch errors: ${brandReport.crawlSummary.fetchErrors.length}`);
  }
  console.log(`Census rows loaded: ${brandReport.censusRowsLoaded}`);

  const planRows = brandReport.planRows;
  const reportSlug = slug || slugify(brandConfig.canonicalBrandName);
  const jsonPath = join(REPORTS, `hilton-census-enrichment-plan-${reportSlug}.json`);
  const csvPath = join(REPORTS, `hilton-census-enrichment-plan-${reportSlug}.csv`);

  const report = {
    generatedAt: new Date().toISOString(),
    brand: brandConfig.canonicalBrandName,
    parentCompany,
    brandConfig,
    alias: brandReport.alias,
    minConfidence,
    crawlSummary: brandReport.crawlSummary,
    censusRowsLoaded: brandReport.censusRowsLoaded,
    matched: brandReport.matched,
    readyToApply: brandReport.readyToApply,
    noChanges: brandReport.noChanges,
    unmatchedDirectory: brandReport.unmatchedDirectory,
    unmatchedCensus: brandReport.unmatchedCensus,
    planRows,
  };

  mkdirSync(REPORTS, { recursive: true });
  writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const csvHeaders = [
    "status",
    "censusRecordId",
    "censusName",
    "directoryName",
    "directoryBrandPropertyCode",
    "matchConfidence",
    "matchScore",
    "matchReason",
    "fieldsToApply",
    "sourceUrl",
  ];
  const csvLines = planRows.map((r) =>
    [
      r.status,
      r.censusRecordId,
      r.censusName,
      r.directoryName,
      r.directoryBrandPropertyCode,
      r.matchConfidence,
      r.matchScore,
      r.matchReason,
      Object.keys(r.applyFields || {}).join("; "),
      r.sourceUrl,
    ]
      .map(csvEscape)
      .join(",")
  );
  writeFileSync(csvPath, `${csvHeaders.join(",")}\n${csvLines.join("\n")}\n`, "utf8");

  console.log("\n--- Summary ---");
  console.log(`  Matched: ${report.matched}`);
  console.log(`  Ready to apply (fill-blank): ${report.readyToApply}`);
  console.log(`  No changes needed: ${report.noChanges}`);
  console.log(`  Unmatched directory hotels: ${report.unmatchedDirectory}`);
  console.log(`  Unmatched census rows: ${report.unmatchedCensus.length}`);
  console.log(`\nJSON: ${jsonPath}`);
  console.log(`CSV:  ${csvPath}`);

  if (report.unmatchedCensus.length) {
    console.log("\nCensus rows without directory match:");
    for (const r of report.unmatchedCensus.slice(0, 10)) {
      console.log(`  - ${r.name} (${r.city}, ${r.country}) [${r.recordId}]`);
    }
  }
  if (report.unmatchedDirectory) {
    console.log("\nDirectory hotels without census match:");
    for (const r of planRows.filter((p) => p.status === "unmatched_directory").slice(0, 10)) {
      console.log(`  - ${r.directoryName} [${r.directoryBrandPropertyCode}]`);
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
