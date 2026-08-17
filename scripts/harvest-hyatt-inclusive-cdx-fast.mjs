#!/usr/bin/env node
/**
 * Fast Inclusive Collection CDX top-up (global brand filters + mexico/DR/jamaica).
 * Official hyatt.com URLs via Wayback only.
 *
 *   node scripts/harvest-hyatt-inclusive-cdx-fast.mjs
 */
import { readFileSync, existsSync } from "node:fs";
import {
  harvestHyattCalaFromWaybackCdx,
  loadHyattSitemapFromFile,
  mergeHyattDirectoryRows,
  parseHyattPropertyRowsFromLocs,
  writeHyattDirectoryExtract,
  HYATT_CDX_SOURCE,
  HYATT_INCLUSIVE_BRAND_CDX_FILTERS,
  HYATT_CLASSIC_BRAND_CDX_FILTERS,
} from "../lib/hyatt-brand-directory-extract.js";

const PATH = "reports/hyatt-cala-directory-extract.json";
const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 900);
const limit = Number(process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || 800);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function cdxFetch(urlPattern, originalFilter, lim = limit) {
  let cdx = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
    urlPattern
  )}&output=json&fl=original&collapse=urlkey&limit=${lim}&filter=statuscode:200`;
  if (originalFilter) {
    cdx += `&filter=original:${encodeURIComponent(originalFilter)}`;
  }
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 40000);
  try {
    const res = await fetch(cdx, {
      headers: { "User-Agent": "DealalityResearch/1.0 (hyatt census)" },
      signal: ctrl.signal,
    });
    const text = await res.text();
    let rows = [];
    try {
      rows = JSON.parse(text);
    } catch {
      rows = [];
    }
    const urls = Array.isArray(rows)
      ? rows
          .slice(1)
          .map((r) => (Array.isArray(r) ? r[0] : ""))
          .filter(Boolean)
      : [];
    return { status: res.status, urls: [...new Set(urls)], error: null };
  } catch (err) {
    return { status: 0, urls: [], error: err?.message || String(err) };
  } finally {
    clearTimeout(timer);
  }
}

const existing = existsSync(PATH)
  ? JSON.parse(readFileSync(PATH, "utf8"))
  : { propertyRows: [] };

console.log("Existing unique:", (existing.propertyRows || []).length);

/** @type {string[]} */
const locs = [];
/** @type {object[]} */
const log = [];

const brands = [...HYATT_INCLUSIVE_BRAND_CDX_FILTERS, ...HYATT_CLASSIC_BRAND_CDX_FILTERS];

// 1) Global brand filters
for (const brand of brands) {
  const filter = `.*${brand}.*`;
  const r = await cdxFetch("www.hyatt.com/en-US/hotel/*", filter);
  log.push({ pass: "global", brand, status: r.status, n: r.urls.length, error: r.error });
  console.log(`  global/${brand}: ${r.error || r.urls.length + " urls"}`);
  locs.push(...r.urls);
  await sleep(delayMs);
}

// 2) High-yield region × inclusive combined regex (one query per region)
const regions = [
  "mexico",
  "dominican-republic",
  "jamaica",
  "costa-rica",
  "panama",
  "colombia",
  "nicaragua",
  "bahamas",
  "saint-lucia",
  "curacao",
  "aruba",
  "belize",
  "puerto-rico",
  "cayman-islands",
  "guyana",
  "turks-and-caicos-islands",
];
const inclusiveRe =
  ".*(secrets-|dreams-|breathless-|sunscape-|zoetry-|impression-|now-|hyatt-ziva-|hyatt-zilara-|hyatt-vivid-|thompson-|andaz-).*";

for (const region of regions) {
  const r = await cdxFetch(`www.hyatt.com/en-US/hotel/${region}/*`, inclusiveRe, 1200);
  log.push({ pass: "region-inclusive", region, status: r.status, n: r.urls.length, error: r.error });
  console.log(`  ${region}/inclusive: ${r.error || r.urls.length + " urls"}`);
  locs.push(...r.urls);
  await sleep(delayMs);
}

// 3) Mexico classic high-limit (catch Place/Regency/Centric/Park/Grand missed by 500 cap)
const mex = await cdxFetch("www.hyatt.com/en-US/hotel/mexico/*", null, 3000);
log.push({ pass: "mexico-all", status: mex.status, n: mex.urls.length, error: mex.error });
console.log(`  mexico/all: ${mex.error || mex.urls.length + " urls"}`);
locs.push(...mex.urls);

const cdxRows = parseHyattPropertyRowsFromLocs(locs, { calaOnly: true }).map((r) => ({
  ...r,
  source: HYATT_CDX_SOURCE,
}));

const sitemap = loadHyattSitemapFromFile("data/hyatt-sitemap-wayback-20240126.xml", {
  calaOnly: true,
});

const merged = mergeHyattDirectoryRows(
  sitemap.propertyRows || [],
  existing.propertyRows || [],
  cdxRows
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
  inclusiveCdxFastMeta: {
    locCount: locs.length,
    uniqueParse: cdxRows.length,
    log,
  },
  propertyIdConflicts: merged.propertyIdConflicts || [],
  uniqueProperties: merged.propertyRows.length,
  propertyRows: merged.propertyRows,
});

const byBrand = {};
for (const row of merged.propertyRows) {
  const s = String(row.slug || "").toLowerCase();
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
console.log("CDX unique parse", cdxRows.length);
console.log("Brand counts:", byBrand);
