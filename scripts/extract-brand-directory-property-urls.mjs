/**
 * Phase 4J — Extract property URLs from brand-directory sitemap (report-only).
 */
import "../load-env.js";
import { join } from "path";
import { existsSync } from "fs";
import {
  extractChoicePropertyUrls,
  loadBrandSeedsFile,
  EXTRACT_CSV_COLUMNS,
  extractRowToCsv,
} from "../lib/independent-census/brand-directory-property-url-extract.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let propertySitemapUrl = "https://www.choicehotels.com/propertysitemap.xml.gz";
  let parentCompany = "Choice Hotels International";
  let brandSeeds =
    "fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json";
  let regionFilter = "";
  let countryFilter = "";
  let maxUrls = null;
  let batchId = "choice-property-urls-cala-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--property-sitemap-url" && argv[i + 1])
      propertySitemapUrl = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-sitemap-url="))
      propertySitemapUrl = a.slice("--property-sitemap-url=".length).replace(/^"|"$/g, "");
    else if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--brand-seeds" && argv[i + 1])
      brandSeeds = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--brand-seeds="))
      brandSeeds = a.slice("--brand-seeds=".length).replace(/^"|"$/g, "");
    else if (a === "--region-filter" && argv[i + 1])
      regionFilter = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--region-filter="))
      regionFilter = a.slice("--region-filter=".length).replace(/^"|"$/g, "");
    else if (a === "--country-filter" && argv[i + 1])
      countryFilter = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country-filter="))
      countryFilter = a.slice("--country-filter=".length).replace(/^"|"$/g, "");
    else if (a === "--max-urls" && argv[i + 1])
      maxUrls = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-urls="))
      maxUrls = parseInt(a.slice("--max-urls=".length), 10);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4J is report-only.");
    }
  }

  const reportSlug = batchId.includes("choice")
    ? "choice-property-url-extract-cala-2026-05-20"
    : batchId.replace(/^choice-property-urls-/, "choice-property-url-extract-");

  return {
    propertySitemapUrl,
    parentCompany,
    brandSeedsPath: join(process.cwd(), brandSeeds),
    regionFilter,
    countryFilter,
    maxUrls,
    batchId,
    jsonPath: join(REPORTS_DIR, `independent-census-${reportSlug}.json`),
    csvPath: join(REPORTS_DIR, `independent-census-${reportSlug}.csv`),
  };
}

async function main() {
  const args = parseArgs();
  if (!existsSync(args.brandSeedsPath)) {
    throw new Error(`Brand seeds not found: ${args.brandSeedsPath}`);
  }

  const seeds = loadBrandSeedsFile(args.brandSeedsPath);

  console.log("=== Choice property URL extract (Phase 4J, report-only) ===\n");
  console.log(`Property sitemap: ${args.propertySitemapUrl}`);
  console.log(`Parent:         ${args.parentCompany}`);
  console.log(`Brand seeds:    ${seeds.length}`);
  if (args.regionFilter) console.log(`Region filter:  ${args.regionFilter}`);
  if (args.countryFilter) console.log(`Country filter: ${args.countryFilter}`);
  if (args.maxUrls) console.log(`Max URLs:       ${args.maxUrls}`);
  console.log("");

  const result = await extractChoicePropertyUrls({
    propertySitemapUrl: args.propertySitemapUrl,
    parentCompany: args.parentCompany,
    seeds,
    regionFilter: args.regionFilter,
    countryFilter: args.countryFilter,
    maxUrls: args.maxUrls,
    includeExcludedInOutput: true,
  });

  if (!result.ok) {
    console.error(result.error || "Extract failed");
    process.exit(1);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4J-choice-property-url-extract",
    batchId: args.batchId,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    independentCensusWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    propertySitemapUrl: args.propertySitemapUrl,
    parentCompany: args.parentCompany,
    brandSeedsPath: args.brandSeedsPath,
    filters: {
      regionFilter: args.regionFilter || null,
      countryFilter: args.countryFilter || null,
      maxUrls: args.maxUrls,
    },
    summary: {
      totalSitemapLocs: result.totalSitemapLocs,
      totalPropertyUrlsParsed: result.totalPropertyUrlsParsed,
      calaLikelyPropertyUrlCount: result.calaLikelyPropertyUrlCount,
      calaIncludedCount: result.calaIncludedCount,
      calaUncertainCount: result.calaUncertainCount,
      excludedNonCalaCount: result.excludedNonCalaCount,
      matchedBrandSetupBrandCount: result.matchedBrandSetupBrandCount,
      unmatchedBrandCount: result.unmatchedBrandCount,
      recommendedNextAction: result.recommendedNextAction,
    },
    matchedBrandSetupBrands: result.matchedBrandSetupBrands,
    unmatchedBrandSetupBrands: result.unmatchedBrandSetupBrands,
    countByBrand: result.countByBrand,
    countByCountryOrRegionSegment: result.countByCountryOrRegionSegment,
    sampleUrls: result.sampleUrls,
    sourceRiskNotes: result.sourceRiskNotes,
    reportFiles: { json: args.jsonPath, csv: args.csvPath },
    propertyRows: result.rows,
  };

  writeJson(args.jsonPath, report);
  writeCsv(args.csvPath, result.rows.map(extractRowToCsv), EXTRACT_CSV_COLUMNS);

  console.log("--- Summary ---");
  console.log(`  Total sitemap <loc> entries: ${result.totalSitemapLocs}`);
  console.log(`  Property URLs parsed:      ${result.totalPropertyUrlsParsed}`);
  console.log(`  CALA included:             ${result.calaIncludedCount}`);
  console.log(`  CALA uncertain:            ${result.calaUncertainCount}`);
  console.log(`  Excluded non-CALA:         ${result.excludedNonCalaCount}`);
  console.log(`  Matched Brand Setup brands: ${result.matchedBrandSetupBrandCount} / ${seeds.length}`);
  console.log(`  Recommended next action:   ${result.recommendedNextAction}`);

  const topSegments = Object.entries(result.countByCountryOrRegionSegment)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  console.log("\n  Top country/region segments:");
  topSegments.forEach(([k, v]) => console.log(`    ${k}: ${v}`));

  console.log("\nReport files:");
  console.log(`  ${args.jsonPath}`);
  console.log(`  ${args.csvPath}`);
  console.log("\n✓ URL-only extract. No property HTML. No Airtable writes.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
