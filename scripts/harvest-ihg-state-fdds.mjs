/**

 * Harvest IHG brand FDDs from U.S. state registration portals.

 *

 * Sources (tried in order):

 *   1. Minnesota CARDS — franchisor + franchise-name searches, multiple FDD doc types

 *   2. Wisconsin DFI — when reachable from this network

 *

 * Franchisors: Holiday Hospitality Franchising, LLC + IHG Franchising, LLC

 *

 *   node scripts/harvest-ihg-state-fdds.mjs

 *   node scripts/harvest-ihg-state-fdds.mjs --apply

 */

import "../load-env.js";

import fs from "fs";

import path from "path";

import { fileURLToPath } from "url";

import puppeteer from "puppeteer";

import {

  buildReferenceMaterialPaths,

  ensureReferenceDirectory,

  writeCaptureReadme,

  appendCaptureLog,

  resolveReferenceRoot,

  sanitizeFileName,

} from "../lib/partner-intelligence/reference-material-paths.js";

import { sleep } from "../lib/partner-intelligence/harvest-browser-capture.js";



const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ROOT = path.resolve(__dirname, "..");

const COMPANY = "IHG Hotels & Resorts";

const APPLY = process.argv.includes("--apply");



const FRANCHISORS = ["Holiday Hospitality", "IHG Franchising"];

const FRANCHISE_SEARCHES = [

  "Holiday Inn",

  "Holiday Inn Express",

  "Kimpton",

  "Staybridge",

  "Candlewood",

  "Atwell",

  "Avid",

  "Garner",

  "Six Senses",

  "Regent",

  "Crowne Plaza",

  "InterContinental",

  "Hotel Indigo",

  "Even Hotels",

  "Voco",

  "Vignette",

  "Ruby",

  "Iberostar",

  "Hualuxe",

];

const DOC_TYPES = ["Clean FDD", "Final FDD", "Revised FDD - Clean", ""];



const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";



/** @param {string} raw */

function normalizeBrandName(raw) {

  const t = raw.trim().replace(/\s+/g, " ");

  const map = {

    "CROWNE PLAZA HOTELS & RESORTS": "Crowne Plaza",

    "INTERCONTINENTAL HOTELS & RESORTS": "InterContinental",

    "HOTEL INDIGO": "Hotel Indigo",

    "EVEN HOTELS": "Even Hotels",

    "VIGNETTE COLLECTION": "Vignette Collection",

    VOCO: "Voco Hotels",

    "RUBY HOTELS": "Ruby",

    "HOLIDAY INN HOTELS & RESORTS": "Holiday Inn",

    "HOLIDAY INN": "Holiday Inn",

    "HOLIDAY INN EXPRESS": "Holiday Inn Express",

    "HOLIDAY INN CLUB VACATIONS": "Holiday Inn Club Vacations",

    "KIMPTON HOTELS & RESTAURANTS": "Kimpton",

    "STAYBRIDGE SUITES": "Staybridge Suites",

    "CANDLEWOOD SUITES": "Candlewood Suites",

    "ATWELL SUITES": "Atwell Suites",

    "AVID HOTELS": "avid hotels",

    "GARNER HOTELS": "Garner hotels",

    "SIX SENSES HOTELS RESORTS SPAS": "Six Senses",

    "SIX SENSES": "Six Senses",

    "REGENT HOTELS & RESORTS": "Regent",

    "IBEROSTAR BEACHFRONT RESORTS": "Iberostar",

    "HUALUXE HOTELS AND RESORTS": "Hualuxe Hotels And Resorts",

  };

  const upper = t.toUpperCase();

  if (map[upper]) return map[upper];

  return t

    .split(" ")

    .map((w) => (/^(&|AND|BY|THE|OF)$/i.test(w) ? w.toLowerCase() : w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()))

    .join(" ")

    .replace(/\bIhg\b/g, "IHG");

}



/** @param {import('puppeteer').Page} page @param {Record<string,string>} params */

async function mnCardsSearch(page, params) {

  const url = new URL("https://cards.web.commerce.state.mn.us/franchise-registrations");

  url.searchParams.set("doSearch", "true");

  for (const [k, v] of Object.entries(params)) {

    if (v) url.searchParams.set(k, v);

  }

  await page.goto(url.toString(), { waitUntil: "networkidle2", timeout: 120000 });

  await sleep(4000);

  const blocked = await page.evaluate(() => /403 Forbidden|429|rate limit/i.test(document.body.innerText));

  if (blocked) throw new Error("Minnesota CARDS blocked (403/429) — wait and retry or search manually");



  for (let i = 0; i < 20; i++) {

    const clicked = await page.evaluate(() => {

      const btn = [...document.querySelectorAll("button, a")].find((el) => /load more/i.test(el.textContent || ""));

      if (btn && !btn.disabled) {

        btn.click();

        return true;

      }

      return false;

    });

    if (!clicked) break;

    await sleep(2500);

  }



  return page.evaluate(() => {

    const results = [];

    for (const tr of document.querySelectorAll("table tr")) {

      const tds = [...tr.querySelectorAll("td")].map((td) => td.textContent?.replace(/\s+/g, " ").trim() || "");

      if (tds.length < 6) continue;

      const download = tr.querySelector('a[href*="/documents/"][href*="/download"]');

      if (!download) continue;

      results.push({

        documentId: tds[0] || "",

        franchisor: tds[1] || "",

        franchiseName: tds[2] || "",

        documentType: tds[3] || "",

        year: tds[4] || "",

        fileNumber: tds[5] || "",

        notes: tds[6] || "",

        receivedDate: tds[7] || "",

        addedOn: tds[8] || "",

        downloadUrl: download.href,

        source: "Minnesota CARDS",

      });

    }

    return results;

  });

}



/** @param {import('puppeteer').Browser} browser */

async function discoverMinnesotaFdds(browser) {

  const page = await browser.newPage();

  await page.setUserAgent(UA);

  /** @type {Map<string, object>} */

  const merged = new Map();



  const queries = [

    ...FRANCHISORS.flatMap((franchisor) =>

      DOC_TYPES.filter(Boolean).map((documentType) => ({ franchisor, documentType, label: `${franchisor} / ${documentType}` })),

    ),

    ...FRANCHISORS.map((franchisor) => ({ franchisor, label: `${franchisor} (all doc types)` })),

    ...FRANCHISE_SEARCHES.map((franchiseName) => ({ franchiseName, documentType: "Clean FDD", label: `franchise ${franchiseName}` })),

  ];



  for (const q of queries) {

    const { label, ...params } = q;

    try {

      console.log("  MN:", label);

      const rows = await mnCardsSearch(page, params);

      let added = 0;

      for (const row of rows) {

        if (!/fdd/i.test(row.documentType)) continue;

        const key = `${row.documentId}|${row.downloadUrl}`;

        if (!merged.has(key)) {

          merged.set(key, row);

          added++;

        }

      }

      console.log("    +", added, "FDD rows (total unique:", merged.size, ")");

    } catch (err) {

      console.warn("    skip:", err.message);

      if (/blocked/i.test(err.message)) break;

    }

    await sleep(6000);

  }



  await page.close();

  return [...merged.values()];

}



/** Pick latest Clean/Final FDD per brand */

function dedupeLatestPerBrand(rows) {

  /** @type {Map<string, typeof rows[0] & { brand: string }>} */

  const byBrand = new Map();

  for (const row of rows) {

    if (!/clean fdd|final fdd|revised fdd/i.test(row.documentType)) continue;

    const brand = normalizeBrandName(row.franchiseName);

    const year = Number(row.year) || 0;

    const added = Date.parse(row.addedOn) || 0;

    const prev = byBrand.get(brand);

    if (!prev) {

      byBrand.set(brand, { ...row, brand });

      continue;

    }

    const prevYear = Number(prev.year) || 0;

    const prevAdded = Date.parse(prev.addedOn) || 0;

    if (year > prevYear || (year === prevYear && added > prevAdded)) {

      byBrand.set(brand, { ...row, brand });

    }

  }

  return [...byBrand.values()].sort((a, b) => a.brand.localeCompare(b.brand));

}



async function downloadPdfFreshSession(downloadUrl) {

  const browser = await puppeteer.launch({ headless: "new" });

  const page = await browser.newPage();

  await page.setUserAgent(UA);

  try {

    await page.goto("https://cards.web.commerce.state.mn.us/franchise-registrations", {

      waitUntil: "networkidle2",

      timeout: 120000,

    });

    await sleep(5000);

    const bytes = await page.evaluate(async (url) => {

      const res = await fetch(url, { credentials: "include" });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      return Array.from(new Uint8Array(await res.arrayBuffer()));

    }, downloadUrl);

    const buf = Buffer.from(bytes);

    if (!buf.slice(0, 5).toString().startsWith("%PDF")) throw new Error("Response is not a PDF");

    return buf;

  } finally {

    await page.close();

    await browser.close();

  }

}



function saveFdd(asset, buf) {

  const title = `${asset.brand} FDD ${asset.year} (MN state filing)`;

  const paths = buildReferenceMaterialPaths({

    companyFolder: COMPANY,

    brandName: asset.brand,

    typeKey: "fdd",

    title: sanitizeFileName(title),

    ext: ".pdf",

  });

  ensureReferenceDirectory(paths.absoluteDir);

  writeCaptureReadme(COMPANY, path.join(resolveReferenceRoot(), COMPANY));

  fs.writeFileSync(paths.absoluteFile, buf);

  appendCaptureLog(COMPANY, {

    url: asset.downloadUrl,

    relativePath: paths.relativePath,

    brand: asset.brand,

    typeKey: "fdd",

    source: asset.source || "Minnesota CARDS",

    documentId: asset.documentId,

    year: asset.year,

  });

  return paths.relativePath;

}



async function main() {

  const report = {

    generatedAt: new Date().toISOString(),

    mode: APPLY ? "apply" : "dry-run",

    company: COMPANY,

    franchisorLegalNames: ["Holiday Hospitality Franchising, LLC", "IHG Franchising, LLC"],

    discovered: [],

    selected: [],

    downloaded: [],

    skipped: [],

    errors: [],

    notFoundInMinnesota: [

      "Holiday Inn / Holiday Inn Express — often in Wisconsin DFI or NASAA EFD, not always in MN CARDS last-10-year window",

      "Kimpton — filed under IHG Franchising, LLC; check Wisconsin + NASAA EFD",

      "Staybridge Suites, Candlewood Suites, Atwell, avid, Garner, Six Senses, Regent — check Wisconsin DFI manually if automation blocked",

    ],

    portalNotes: {

      minnesota: "Primary automated source; rate-limits after ~4–5 rapid downloads",

      wisconsin: "https://apps.dfi.wi.gov/apps/FranchiseSearch/MainSearch.aspx — often has brands missing from MN; timeout from cloud runners",

      indiana: "https://securities.sos.in.gov/public-portfolio-search/ — Cloudflare blocks automation",

      california: "https://dfpi.ca.gov/search",

      nasaaEfd: "https://www.nasaaefd.org/Franchise/Search — national index; CloudFront may block automation",

    },

  };



  console.log("Discovering IHG FDDs on Minnesota CARDS (exhaustive queries)…");

  const browser = await puppeteer.launch({ headless: "new" });

  const allRows = await discoverMinnesotaFdds(browser);

  await browser.close();



  report.discovered = allRows;

  console.log("Total unique FDD rows:", allRows.length);



  const selected = dedupeLatestPerBrand(allRows);

  report.selected = selected;

  console.log("Unique brands (latest filing each):", selected.length);

  for (const s of selected) {

    console.log(`  ${s.brand} — ${s.year} — ${s.documentId}`);

  }



  for (const asset of selected) {

    if (!APPLY) {

      console.log("WOULD", asset.brand, asset.downloadUrl);

      continue;

    }

    const dest = buildReferenceMaterialPaths({

      companyFolder: COMPANY,

      brandName: asset.brand,

      typeKey: "fdd",

      title: sanitizeFileName(`${asset.brand} FDD ${asset.year} (MN state filing)`),

      ext: ".pdf",

    });

    if (fs.existsSync(dest.absoluteFile)) {

      report.skipped.push({ brand: asset.brand, reason: "already exists", relativePath: dest.relativePath });

      console.log("SKIP exists", asset.brand);

      continue;

    }

    try {

      console.log("Downloading", asset.brand, "…");

      const buf = await downloadPdfFreshSession(asset.downloadUrl);

      const relativePath = saveFdd(asset, buf);

      report.downloaded.push({

        brand: asset.brand,

        year: asset.year,

        documentId: asset.documentId,

        downloadUrl: asset.downloadUrl,

        relativePath,

        bytes: buf.length,

      });

      console.log("  OK", relativePath, `(${buf.length} bytes)`);

      await sleep(12000);

    } catch (err) {

      const msg = err?.message || String(err);

      report.errors.push({ brand: asset.brand, downloadUrl: asset.downloadUrl, error: msg });

      console.warn("  FAIL", asset.brand, msg);

      await sleep(20000);

    }

  }



  const outPath = path.join(ROOT, "reports", "ihg-state-fdd-harvest.json");

  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  console.log("\nWrote", outPath);

  console.log(

    "Downloaded:",

    report.downloaded.length,

    "Skipped:",

    report.skipped.length,

    "Errors:",

    report.errors.length,

  );

  if (!APPLY) console.log("Dry run — add --apply to download.");

}



main().catch((err) => {

  console.error(err);

  process.exit(1);

});


