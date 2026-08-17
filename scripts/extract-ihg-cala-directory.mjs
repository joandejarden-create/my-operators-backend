#!/usr/bin/env node
/**
 * Extract official IHG CALA hotel directory from destination country pages.
 *
 *   node scripts/extract-ihg-cala-directory.mjs
 *   node scripts/extract-ihg-cala-directory.mjs --merge-sitemap
 *   node scripts/extract-ihg-cala-directory.mjs --merge-sitemap --harvest-city-pages --enrich-gap-slugs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  crawlIhgHoteldetailSitemaps,
  extractIhgCalaDestinationDirectory,
  mergeIhgDirectoryWithSitemap,
  harvestIhgCalaCityDestinationPages,
  enrichIhgSitemapHotelsWithNames,
  unionIhgDirectoryRows,
} from "../lib/ihg-brand-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

/** City slugs known missing from country destination pages (official sitemap only). */
const IHG_CALA_GAP_CITY_SLUGS = {
  diamond: "Saint Vincent and the Grenadines",
  natal: "Brazil",
  morelia: "Mexico",
  "ciudad-del-carmen": "Mexico",
  irapuato: "Mexico",
  "mas-olas-resort-spa-todos-santos": "Mexico",
  "santo-domingo": "Dominican Republic",
  panama: "Panama",
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    mergeSitemap: args.includes("--merge-sitemap"),
    harvestCityPages: args.includes("--harvest-city-pages"),
    enrichGapSlugs: args.includes("--enrich-gap-slugs"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 200),
  };
}

function toCsv(rows) {
  const cols = [
    "propertyId",
    "name",
    "city",
    "country",
    "countryCode",
    "brand",
    "propertyUrl",
  ];
  const esc = (v) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  console.log("=== IHG CALA destination directory extract ===\n");
  const extracted = await extractIhgCalaDestinationDirectory({
    delayMs: opts.delayMs,
    onProgress: (msg) => console.log(" ", msg),
  });

  let propertyRows = extracted.propertyRows;
  let sitemapHotels = 0;
  let cityHarvestAdded = 0;
  let gapEnrichAdded = 0;
  /** @type {object|null} */
  let sitemapCrawl = null;

  if (opts.mergeSitemap || opts.enrichGapSlugs) {
    console.log("\n=== Crawl hoteldetail sitemaps ===\n");
    sitemapCrawl = await crawlIhgHoteldetailSitemaps({
      delayMs: 80,
      onProgress: (msg) => console.log(" ", msg),
    });
    sitemapHotels = sitemapCrawl.hotels?.length || 0;
    if (opts.mergeSitemap) {
      propertyRows = mergeIhgDirectoryWithSitemap(propertyRows, sitemapCrawl.hotels || []);
    }
  }

  if (opts.harvestCityPages) {
    console.log("\n=== Harvest CALA city destination pages ===\n");
    const known = new Set(propertyRows.map((r) => r.propertyId));
    const harvest = await harvestIhgCalaCityDestinationPages({
      delayMs: Math.min(opts.delayMs, 120),
      knownPropertyIds: known,
      onProgress: (msg) => console.log(" ", msg),
    });
    cityHarvestAdded = harvest.propertyRows.length;
    propertyRows = unionIhgDirectoryRows(propertyRows, harvest.propertyRows);
    console.log("City harvest added:", cityHarvestAdded);
  }

  if (opts.enrichGapSlugs) {
    console.log("\n=== Enrich gap city-slug hoteldetail pages ===\n");
    if (!sitemapCrawl?.hotels?.length) {
      throw new Error("--enrich-gap-slugs requires sitemap crawl (use with --merge-sitemap or alone after crawl)");
    }
    const known = new Set(propertyRows.map((r) => r.propertyId));
    const enrich = await enrichIhgSitemapHotelsWithNames(sitemapCrawl.hotels, {
      delayMs: 120,
      knownPropertyIds: known,
      citySlugs: Object.keys(IHG_CALA_GAP_CITY_SLUGS),
      countryByCitySlug: IHG_CALA_GAP_CITY_SLUGS,
      onProgress: (msg) => console.log(" ", msg),
    });
    gapEnrichAdded = enrich.propertyRows.length;
    propertyRows = unionIhgDirectoryRows(propertyRows, enrich.propertyRows);
    console.log("Gap enrich added:", gapEnrichAdded);
    for (const r of enrich.propertyRows) {
      console.log(`  + ${r.propertyId} | ${r.name} | ${r.country}`);
    }
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    ...extracted,
    propertyRows,
    sitemapHotelsMerged: sitemapHotels,
    cityHarvestAdded,
    gapEnrichAdded,
    propertyCount: propertyRows.length,
  };

  const jsonPath = join(REPORTS, "ihg-cala-directory-extract.json");
  const csvPath = join(REPORTS, "ihg-cala-directory-extract.csv");
  writeFileSync(jsonPath, JSON.stringify(payload, null, 2));
  writeFileSync(csvPath, toCsv(propertyRows));

  console.log("\nPages OK:", extracted.pagesOk);
  console.log("Hotels:", propertyRows.length);
  console.log("City harvest +:", cityHarvestAdded, "Gap enrich +:", gapEnrichAdded);
  console.log("JSON:", jsonPath);
  console.log("CSV:", csvPath);
  for (const p of extracted.pageResults.filter((x) => x.ok)) {
    console.log(`  ${p.country}: ${p.cards}`);
  }
  const missing = extracted.pageResults.filter((x) => !x.ok);
  if (missing.length) {
    console.log("\nNo destination page / 0 cards:");
    for (const p of missing) console.log(`  ${p.country}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
