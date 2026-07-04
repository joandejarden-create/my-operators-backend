/**
 * Phase 4H — Property-level discovery path analysis (report-only).
 *
 * Inspects each brand seed sourceUrl (+ optional robots.txt / one sitemap GET).
 * No deep crawl. No Airtable writes.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  analyzeAllBrandPropertyPaths,
  PROPERTY_PATH_CSV_COLUMNS,
  propertyPathToCsvRow,
} from "../lib/independent-census/brand-directory-property-paths.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let input = "";
  let batchId = "choice-property-paths-2026-05-20";
  let maxPagesPerBrand = 1;
  let fetchSitemap = true;
  let delayMs = 400;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1]) input = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--input="))
      input = a.slice("--input=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--max-pages-per-brand" && argv[i + 1])
      maxPagesPerBrand = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-pages-per-brand="))
      maxPagesPerBrand = parseInt(a.slice("--max-pages-per-brand=".length), 10);
    else if (a === "--fetch-sitemap=false") fetchSitemap = false;
    else if (a === "--delay-ms" && argv[i + 1]) delayMs = parseInt(argv[++i], 10);
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4H is analysis-only.");
    }
  }

  if (!input) {
    throw new Error("Missing --input (brand-directory seeds JSON)");
  }

  const slug = batchId.replace(/^choice-/, "").replace(/-property-paths.*/, "") || batchId;
  const reportSlug =
    batchId.includes("choice") || input.includes("choice")
      ? "choice-hotels-2026-05-20"
      : slug;

  return {
    inputPath: join(process.cwd(), input),
    batchId,
    reportSlug,
    maxPagesPerBrand,
    fetchSitemap,
    delayMs,
  };
}

function loadSeeds(path) {
  if (!existsSync(path)) throw new Error(`Input not found: ${path}`);
  const data = JSON.parse(readFileSync(path, "utf8"));
  const seeds = Array.isArray(data) ? data : data.seeds || [];
  if (!seeds.length) throw new Error("No seeds in input file");
  return { data, seeds };
}

async function main() {
  const args = parseArgs();
  const { data, seeds } = loadSeeds(args.inputPath);

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-brand-directory-property-paths-${args.reportSlug}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-brand-directory-property-paths-${args.reportSlug}.csv`
  );

  console.log("=== Brand directory property-path analysis (Phase 4H, read-only) ===\n");
  console.log(`Input:  ${args.inputPath}`);
  console.log(`Seeds:  ${seeds.length}`);
  console.log(`Max pages per brand (source only): ${args.maxPagesPerBrand}`);
  console.log(`Probe sitemap once when referenced: ${args.fetchSitemap}`);
  console.log(`Delay between brands: ${args.delayMs}ms\n`);

  const { rows, summary } = await analyzeAllBrandPropertyPaths(seeds, {
    fetchSitemap: args.fetchSitemap,
    delayMs: args.delayMs,
  });

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4H-brand-directory-property-paths",
    batchId: args.batchId,
    inputPath: args.inputPath,
    seedBatchId: data.batchId || null,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    independentCensusWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    maxPagesPerBrand: args.maxPagesPerBrand,
    fetchSitemapProbed: args.fetchSitemap,
    summary,
    rows,
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, rows.map(propertyPathToCsvRow), PROPERTY_PATH_CSV_COLUMNS);

  console.log("--- Summary ---");
  console.log(`  Brands analyzed: ${summary.brandsAnalyzed}`);
  console.log(`  Locator URLs found: ${summary.withLocatorUrl}`);
  console.log(`  Sitemap URLs found: ${summary.withSitemapUrl}`);
  console.log(`  Brands with direct property links on page: ${summary.withDirectPropertyUrls}`);
  console.log(`  Direct property links (on-page total): ${summary.totalDirectPropertyLinksOnPages}`);
  console.log(`  Hotel-like URLs in sitemap sample: ${summary.sitemapHotelUrlSamples}`);
  console.log("\n  By recommended method:");
  console.log(JSON.stringify(summary.byRecommendedDiscoveryMethod, null, 2));

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ Analysis only. No crawl beyond source page + optional robots/sitemap. No Airtable writes."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
