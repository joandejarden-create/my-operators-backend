#!/usr/bin/env node
/**
 * Top-up Hyatt CALA directory with Inclusive Collection / leisure brand CDX
 * from official hyatt.com URLs via Wayback (no Airtable writes).
 *
 *   node scripts/harvest-hyatt-inclusive-cdx.mjs
 *   node scripts/harvest-hyatt-inclusive-cdx.mjs --delay-ms=800 --limit=500
 */
import { readFileSync, existsSync } from "node:fs";
import {
  harvestHyattInclusiveBrandCdx,
  harvestHyattCalaFromWaybackCdx,
  loadHyattSitemapFromFile,
  mergeHyattDirectoryRows,
  writeHyattDirectoryExtract,
  HYATT_CLASSIC_BRAND_CDX_FILTERS,
  HYATT_INCLUSIVE_CDX_PRIORITY_REGIONS,
} from "../lib/hyatt-brand-directory-extract.js";

const PATH = "reports/hyatt-cala-directory-extract.json";
const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 800);
const limitPerQuery = Number(process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || 500);
const skipGlobal = process.argv.includes("--skip-global");
const lean = process.argv.includes("--lean");
const alsoRaiseMexico = !process.argv.includes("--skip-region-topup");

const existing = existsSync(PATH)
  ? JSON.parse(readFileSync(PATH, "utf8"))
  : { propertyRows: [] };

const regions = lean
  ? ["mexico", "dominican-republic", "jamaica", "costa-rica", "nicaragua", "bahamas", "panama"]
  : HYATT_INCLUSIVE_CDX_PRIORITY_REGIONS;

console.log(
  `Inclusive brand CDX (delay=${delayMs}ms, limit=${limitPerQuery}, regions=${regions.length}, global=${!skipGlobal})…`
);
console.log("Existing directory unique:", (existing.propertyRows || []).length);

const inclusive = await harvestHyattInclusiveBrandCdx({
  regions,
  delayMs,
  limitPerQuery,
  fetchTimeoutMs: 45000,
  includeGlobalBrandPass: !skipGlobal,
  globalExtraBrandFilters: HYATT_CLASSIC_BRAND_CDX_FILTERS,
  onProgress: (msg) => console.log(" ", msg),
});

/** @type {object[]} */
const extras = [...(inclusive.propertyRows || [])];

if (alsoRaiseMexico) {
  console.log("\nRegion top-up (higher limit) for mexico + nicaragua…");
  const regionTop = await harvestHyattCalaFromWaybackCdx({
    regions: ["mexico", "nicaragua", "bahamas", "turks-and-caicos-islands", "turks-and-caicos"],
    delayMs,
    limitPerRegion: 2500,
    fetchTimeoutMs: 60000,
    onProgress: (msg) => console.log(" ", msg),
  });
  extras.push(...(regionTop.propertyRows || []));
  console.log("  Region top-up unique parse:", regionTop.propertyRows.length);
}

const sitemap = loadHyattSitemapFromFile("data/hyatt-sitemap-wayback-20240126.xml", {
  calaOnly: true,
});

const merged = mergeHyattDirectoryRows(
  sitemap.propertyRows || [],
  existing.propertyRows || [],
  extras
);

writeHyattDirectoryExtract(PATH, {
  ...existing,
  generatedAt: new Date().toISOString(),
  sourcePolicy: "official_hyatt_com_urls_only",
  sitemapMeta: {
    source: sitemap.source,
    locCount: sitemap.locCount,
    unique: (sitemap.propertyRows || []).length,
  },
  inclusiveCdxMeta: {
    locCount: inclusive.locCount,
    unique: inclusive.propertyRows.length,
    fetchLogSummary: {
      queries: inclusive.fetchLog.length,
      withUrls: inclusive.fetchLog.filter((f) => (f.urlCount || 0) > 0).length,
      errors: inclusive.fetchLog.filter((f) => f.error).length,
    },
  },
  propertyIdConflicts: merged.propertyIdConflicts || [],
  uniqueProperties: merged.propertyRows.length,
  propertyRows: merged.propertyRows,
});

const byBrand = {};
for (const r of merged.propertyRows) {
  const s = String(r.slug || "").toLowerCase();
  let b = "other";
  for (const tok of [
    "secrets",
    "dreams",
    "breathless",
    "sunscape",
    "zoetry",
    "impression",
    "now-",
    "ziva",
    "zilara",
    "vivid",
    "thompson",
    "andaz",
    "hyatt-place",
    "hyatt-regency",
    "hyatt-centric",
    "park-hyatt",
    "grand-hyatt",
  ]) {
    if (s.includes(tok)) {
      b = tok;
      break;
    }
  }
  byBrand[b] = (byBrand[b] || 0) + 1;
}

console.log("\nWas", (existing.propertyRows || []).length, "→ now", merged.propertyRows.length);
console.log("Inclusive CDX locs", inclusive.locCount, "unique parse", inclusive.propertyRows.length);
console.log("Brand counts:", byBrand);
console.log("Conflicts", (merged.propertyIdConflicts || []).length);
