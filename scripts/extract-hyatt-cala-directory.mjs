#!/usr/bin/env node
/**
 * Rebuild Hyatt CALA directory from local sitemap + Wayback CDX (no Airtable writes).
 */
import {
  harvestHyattCalaFromWaybackCdx,
  mergeHyattDirectoryRows,
  loadHyattSitemapFromFile,
  writeHyattDirectoryExtract,
  HYATT_CALA_REGION_SEGMENTS,
} from "../lib/hyatt-brand-directory-extract.js";

const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 1000);
const limitPerRegion = Number(
  process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || 500
);
const priorityOnly = process.argv.includes("--priority");

const PRIORITY_REGIONS = [
  "mexico",
  "brazil",
  "argentina",
  "colombia",
  "chile",
  "peru",
  "dominican-republic",
  "puerto-rico",
  "costa-rica",
  "panama",
  "jamaica",
  "bahamas",
  "barbados",
  "aruba",
  "uruguay",
  "ecuador",
  "guatemala",
  "honduras",
  "el-salvador",
  "bolivia",
  "paraguay",
  "venezuela",
  "cayman-islands",
  "curacao",
  "trinidad-and-tobago",
  "belize",
  "bermuda",
  "haiti",
  "saint-lucia",
  "antigua-and-barbuda",
  "saint-kitts-and-nevis",
  "turks-and-caicos-islands",
  "virgin-islands-us",
  "sint-maarten",
  "grenada",
  "guyana",
  "suriname",
];

const regions = priorityOnly ? PRIORITY_REGIONS : [...HYATT_CALA_REGION_SEGMENTS].sort();
console.log(`CDX harvest for ${regions.length} regions (delay=${delayMs}ms, limit=${limitPerRegion})…`);

const cdx = await harvestHyattCalaFromWaybackCdx({
  regions,
  delayMs,
  limitPerRegion,
  fetchTimeoutMs: 20000,
  onProgress: (msg) => console.log(" ", msg),
});

const sitemap = loadHyattSitemapFromFile("data/hyatt-sitemap-wayback-20240126.xml", {
  calaOnly: true,
});
const mergedResult = mergeHyattDirectoryRows(sitemap.propertyRows || [], cdx.propertyRows || []);
const merged = mergedResult.propertyRows || [];
writeHyattDirectoryExtract("reports/hyatt-cala-directory-extract.json", {
  generatedAt: new Date().toISOString(),
  sourcePolicy: "official_hyatt_com_urls_only",
  sitemapMeta: {
    source: sitemap.source,
    locCount: sitemap.locCount,
    unique: (sitemap.propertyRows || []).length,
  },
  cdxMeta: {
    locCount: cdx.locCount,
    unique: cdx.propertyRows.length,
    fetchLog: cdx.fetchLog,
  },
  propertyIdConflicts: mergedResult.propertyIdConflicts || [],
  uniqueProperties: merged.length,
  propertyRows: merged,
});

const byR = {};
for (const r of merged) byR[r.regionSlug] = (byR[r.regionSlug] || 0) + 1;
console.log("CDX locs", cdx.locCount, "unique parse", cdx.propertyRows.length);
console.log("Merged unique", merged.length);
console.log("Property ID conflicts", (mergedResult.propertyIdConflicts || []).length);
console.log(byR);
