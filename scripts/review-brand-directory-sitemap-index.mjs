/**
 * Phase 4I — Sitemap index + limited child sitemap review (report-only).
 */
import "../load-env.js";
import { join } from "path";
import { existsSync } from "fs";
import {
  reviewSitemapIndex,
  loadBrandSeedsFile,
  SITEMAP_REVIEW_CSV_COLUMNS,
  sitemapRowToCsv,
} from "../lib/independent-census/brand-directory-sitemap-review.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let sitemapUrl = "https://www.choicehotels.com/sitemapindex.xml";
  let parentCompany = "Choice Hotels International";
  let brandSeeds = "fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json";
  let maxChildSitemaps = 5;
  let maxUrls = 500;
  let batchId = "choice-sitemap-review-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--sitemap-url" && argv[i + 1])
      sitemapUrl = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--sitemap-url="))
      sitemapUrl = a.slice("--sitemap-url=".length).replace(/^"|"$/g, "");
    else if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--brand-seeds" && argv[i + 1])
      brandSeeds = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--brand-seeds="))
      brandSeeds = a.slice("--brand-seeds=".length).replace(/^"|"$/g, "");
    else if (a === "--max-child-sitemaps" && argv[i + 1])
      maxChildSitemaps = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-child-sitemaps="))
      maxChildSitemaps = parseInt(a.slice("--max-child-sitemaps=".length), 10);
    else if (a === "--max-urls" && argv[i + 1]) maxUrls = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-urls="))
      maxUrls = parseInt(a.slice("--max-urls=".length), 10);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4I is report-only.");
    }
  }

  const reportSlug = batchId.includes("choice")
    ? "choice-2026-05-20"
    : batchId.replace(/^choice-sitemap-review-/, "sitemap-");

  return {
    sitemapUrl,
    parentCompany,
    brandSeedsPath: join(process.cwd(), brandSeeds),
    maxChildSitemaps,
    maxUrls,
    batchId,
    jsonPath: join(
      REPORTS_DIR,
      `independent-census-brand-directory-sitemap-review-${reportSlug}.json`
    ),
    csvPath: join(
      REPORTS_DIR,
      `independent-census-brand-directory-sitemap-review-${reportSlug}.csv`
    ),
  };
}

async function main() {
  const args = parseArgs();
  if (!existsSync(args.brandSeedsPath)) {
    throw new Error(`Brand seeds not found: ${args.brandSeedsPath}`);
  }

  const seeds = loadBrandSeedsFile(args.brandSeedsPath);

  console.log("=== Brand directory sitemap review (Phase 4I, report-only) ===\n");
  console.log(`Sitemap index: ${args.sitemapUrl}`);
  console.log(`Parent:        ${args.parentCompany}`);
  console.log(`Brand seeds:   ${seeds.length}`);
  console.log(`Max child sitemaps: ${args.maxChildSitemaps}`);
  console.log(`Max URLs:           ${args.maxUrls}\n`);

  const result = await reviewSitemapIndex({
    sitemapUrl: args.sitemapUrl,
    parentCompany: args.parentCompany,
    seeds,
    maxChildSitemaps: args.maxChildSitemaps,
    maxUrls: args.maxUrls,
    batchId: args.batchId,
  });

  if (!result.ok) {
    console.error(result.error || "Sitemap review failed");
    process.exit(1);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4I-sitemap-child-index-review",
    batchId: args.batchId,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    independentCensusWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    sitemapUrl: args.sitemapUrl,
    parentCompany: args.parentCompany,
    brandSeedsPath: args.brandSeedsPath,
    limits: {
      maxChildSitemaps: args.maxChildSitemaps,
      maxUrls: args.maxUrls,
    },
    summary: {
      childSitemapsFound: result.childSitemapsFound,
      childSitemapsRelevant: result.childSitemapsRelevant,
      childSitemapsInspected: result.childSitemapsInspected,
      totalUrlsParsed: result.totalUrlsParsed,
      likelyPropertyUrlCount: result.likelyPropertyUrlCount,
      likelyBrandUrlCount: result.likelyBrandUrlCount,
      likelyCityDestinationUrlCount: result.likelyCityDestinationUrlCount,
      likelyMarketingUrlCount: result.likelyMarketingUrlCount,
      matchedChoiceBrandCount: result.matchedChoiceBrandCount,
      unmatchedChoiceBrandCount: result.unmatchedChoiceBrands.length,
      recommendedNextAction: result.recommendedNextAction,
    },
    matchedChoiceBrands: result.matchedChoiceBrands,
    unmatchedChoiceBrands: result.unmatchedChoiceBrands,
    childSitemapUrlsInspected: result.childSitemapUrlsInspected,
    propertyUrlSamples: result.propertyUrlSamples,
    samplePropertyUrlPatterns: result.samplePropertyUrlPatterns,
    sourceRiskNotes: result.sourceRiskNotes,
    urlClassCounts: result.urlClassCounts,
    reportFiles: { json: args.jsonPath, csv: args.csvPath },
    parsedUrlRows: result.parsedUrlRows,
  };

  writeJson(args.jsonPath, report);
  writeCsv(
    args.csvPath,
    result.parsedUrlRows.map(sitemapRowToCsv),
    SITEMAP_REVIEW_CSV_COLUMNS
  );

  console.log("--- Summary ---");
  console.log(`  Child sitemaps found:     ${result.childSitemapsFound}`);
  console.log(`  Child sitemaps relevant:  ${result.childSitemapsRelevant}`);
  console.log(`  Child sitemaps inspected: ${result.childSitemapsInspected}`);
  console.log(`  Total URLs parsed:        ${result.totalUrlsParsed}`);
  console.log(`  Likely property URLs:     ${result.likelyPropertyUrlCount}`);
  console.log(`  Matched Choice brands:    ${result.matchedChoiceBrandCount} / ${seeds.length}`);
  console.log(`  Recommended next action:  ${result.recommendedNextAction}`);
  if (result.samplePropertyUrlPatterns?.length) {
    console.log("\n  Sample property URL patterns:");
    result.samplePropertyUrlPatterns.slice(0, 8).forEach((p) => console.log(`    ${p}`));
  }
  console.log("\nReport files:");
  console.log(`  ${args.jsonPath}`);
  console.log(`  ${args.csvPath}`);
  console.log("\n✓ Report-only. No property pages fetched. No Airtable writes.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
