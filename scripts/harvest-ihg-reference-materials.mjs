/**
 * Exhaustive IHG development resources harvest from development.ihg.com/resources
 *
 * IHG hosts most brochures on Showpad (ihg.showpad.com/share/...), not as direct .pdf links.
 * This script uses Puppeteer to discover share URLs, resolve /processed PDF URLs, and download.
 *
 *   node scripts/harvest-ihg-reference-materials.mjs
 *   node scripts/harvest-ihg-reference-materials.mjs --apply
 *   node scripts/harvest-ihg-reference-materials.mjs --apply --resources-only   # PDFs from /resources only
 *   node scripts/harvest-ihg-reference-materials.mjs --apply --html-only
 *   node scripts/harvest-ihg-reference-materials.mjs --apply --max=5
 *
 * Default --apply (no flags): resources PDFs + supplemental PDFs + sitemap HTML + Showpad HTML fallbacks.
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
import {
  captureHtmlWithBrowser,
  captureShowpadHtml,
  discoverInternalLinks,
  slugToTitle,
  sleep,
} from "../lib/partner-intelligence/harvest-browser-capture.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const COMPANY = "IHG Hotels & Resorts";
const RESOURCES_URL = "https://development.ihg.com/resources";
const APPLY = process.argv.includes("--apply");
const RESOURCES_ONLY = process.argv.includes("--resources-only");
const HTML_ONLY = process.argv.includes("--html-only");
const SKIP_HTML = process.argv.includes("--skip-html");
const ALSO_PDF = process.argv.includes("--also-pdf");
const maxArg = process.argv.find((a) => a.startsWith("--max="));
const MAX = maxArg ? Number(maxArg.split("=")[1]) : 500;

const BASE = "https://development.ihg.com";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const REGIONS = ["Americas", "EMEAA", "Greater China"];

/** Extra PDFs outside the resources page */
const SUPPLEMENTAL_PDFS = [
  {
    url: "http://design.holidayinn.com/downloads/Holiday_Inn_Hotel_Brochure_Final_May_2019.pdf",
    title: "Holiday Inn H4 prototype brochure",
    typeKey: "development-brochure",
    brand: "Holiday Inn",
  },
  {
    url: "https://design.holidayinnexpress.com/downloads/HIEX_OnePager_Dev-Brochure-December-2020.pdf",
    title: "Holiday Inn Express development one-pager",
    typeKey: "one-sheet",
    brand: "Holiday Inn Express",
  },
  {
    url: "https://design.holidayinnexpress.com/downloads/Formula-Blue-2-Prototype-Book-December-2020.pdf",
    title: "Holiday Inn Express Formula Blue 2 prototype book",
    typeKey: "prototype",
    brand: "Holiday Inn Express",
  },
  {
    url: "https://www.ihgplc.com/~/media/Files/I/Ihg-Plc/investors/annual-report/2025/ihg-ar25-interactive.pdf",
    title: "IHG Annual Report and Form 20-F 2025",
    typeKey: "press",
  },
  {
    url: "https://www.ihgplc.com/~/media/Files/I/Ihg-Plc/investors/annual-report/2024/ihg-ar-2024.pdf",
    title: "IHG Annual Report and Form 20-F 2024",
    typeKey: "press",
  },
];

/** Key development pages (always capture even if missing from sitemap) */
const CURATED_HTML_PAGES = [
  { url: `${BASE}/`, title: "IHG development home", category: "General company overview" },
  { url: `${BASE}/resources`, title: "Development resources library", category: "Brochure index" },
  { url: `${BASE}/hotel-development/getting-started`, title: "Getting started owner lifecycle", category: "Operating model / pre-opening" },
  { url: `${BASE}/hotel-development/owner-value`, title: "Owner value hub", category: "Operating model" },
  { url: `${BASE}/hotel-development/owner-value/hotel-lifecycle-management`, title: "Hotel lifecycle management", category: "Operating model" },
  { url: `${BASE}/hotel-development/owner-value/powerful-loyalty-programme`, title: "IHG One Rewards owner value", category: "Commercial strategy" },
  { url: `${BASE}/hotel-development/owner-value/effective-revenue-delivery`, title: "Effective revenue delivery", category: "Commercial strategy" },
  { url: `${BASE}/hotel-development/owner-value/strong-and-distinct-brands`, title: "Strong and distinct brands", category: "Brand relationships" },
  { url: `${BASE}/hotel-development/owner-value/powerful-procurement`, title: "Powerful procurement", category: "Operating model" },
  { url: `${BASE}/hotel-development/owner-value/digital-advantage`, title: "Digital advantage", category: "Commercial strategy" },
  { url: `${BASE}/hotel-development/owner-value/sustainable-business`, title: "Sustainable business", category: "Operating model" },
  { url: `${BASE}/regions/americas`, title: "Americas development region CALA", category: "CALA / Americas" },
  { url: `${BASE}/regions/emeaa`, title: "EMEAA development region", category: "Regional" },
  { url: `${BASE}/regions/greater-china`, title: "Greater China development region", category: "Regional" },
  { url: `${BASE}/contact-us`, title: "Contact development team", category: "Leadership / outreach" },
  // compare-our-brands returns 404 on development.ihg.com — use /hotel-brands instead
  { url: `${BASE}/hotel-brands`, title: "Hotel brands portfolio", category: "Brand relationships" },
  { url: "https://design.holidayinnexpress.com/", title: "Holiday Inn Express design portal", category: "Prototype / design", brand: "Holiday Inn Express" },
  { url: "https://www.ihgplc.com/en/investors/annual-report", title: "IHG investor annual report hub", category: "Company overview" },
];

function inferTypeKey(title) {
  const t = title.toLowerCase();
  if (/prototype|prototype book|beacon/i.test(t)) return "prototype";
  if (/one.pager|one story|brand summary|fact sheet/i.test(t)) return "one-sheet";
  if (/introduction|regional/i.test(t)) return "regional";
  return "development-brochure";
}

function inferBrand(title) {
  const known = [
    ["Atwell Suites", /atwell/i],
    ["avid hotels", /avid/i],
    ["Candlewood Suites", /candlewood/i],
    ["Crowne Plaza", /crowne plaza/i],
    ["EVEN Hotels", /even hotels/i],
    ["Garner hotels", /garner/i],
    ["Holiday Inn Express", /holiday inn express/i],
    ["Holiday Inn", /holiday inn/i],
    ["Hotel Indigo", /hotel indigo|indigo/i],
    ["Iberostar", /iberostar/i],
    ["InterContinental", /intercontinental/i],
    ["Kimpton", /kimpton/i],
    ["Regent", /regent/i],
    ["Six Senses", /six senses/i],
    ["Staybridge Suites", /staybridge/i],
    ["Vignette Collection", /vignette/i],
    ["voco", /voco/i],
    ["IHG Hotels & Resorts", /^americas ihg full|^emea/i],
  ];
  for (const [brand, re] of known) {
    if (re.test(title)) return brand;
  }
  if (/ihg introduction|ihg full development/i.test(title)) return "IHG Hotels & Resorts";
  return undefined;
}

function absIhgUrl(href) {
  if (!href) return null;
  if (href.startsWith("http")) return href.split("#")[0];
  if (href.startsWith("/")) return `https://development.ihg.com${href.split("#")[0]}`;
  return href;
}

/** @returns {Promise<{ title: string, sourceUrl: string, kind: 'showpad'|'direct-pdf' }[]>} */
async function discoverResourceLinks(browser) {
  const page = await browser.newPage();
  await page.setUserAgent(UA);
  const found = new Map();

  async function collectFromCurrentView() {
    const items = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll("a[href]").forEach((a) => {
        const href = a.getAttribute("href") || "";
        const text = (a.textContent || "").trim().replace(/\s+/g, " ");
        if (!text) return;
        if (href.includes("showpad.com/share") || href.includes(".pdf")) {
          out.push({ href, text });
        }
      });
      return out;
    });
    for (const item of items) {
      const key = `${item.text}::${item.href}`;
      if (!found.has(key)) {
        found.set(key, {
          title: item.text,
          sourceUrl: absIhgUrl(item.href),
          kind: item.href.includes("showpad.com") ? "showpad" : "direct-pdf",
        });
      }
    }
  }

  await page.goto(RESOURCES_URL, { waitUntil: "networkidle2", timeout: 120000 });
  await sleep(4000);
  await collectFromCurrentView();

  // Try each region filter to surface EMEAA / Greater China-only items
  for (const region of REGIONS) {
    try {
      const changed = await page.evaluate((regionLabel) => {
        const select = document.querySelector('select[name*="region"], select[id*="region"], select');
        if (!select) return false;
        const opt = [...select.options].find(
          (o) => o.textContent.trim() === regionLabel || o.value === regionLabel,
        );
        if (!opt) return false;
        select.value = opt.value;
        select.dispatchEvent(new Event("change", { bubbles: true }));
        return true;
      }, region);
      if (changed) {
        await sleep(3000);
        await collectFromCurrentView();
      }
    } catch (_) {
      /* region filter optional */
    }
  }

  await page.close();
  return [...found.values()];
}

/** Resolve Showpad share URL → direct application/pdf /processed URL */
async function resolveShowpadPdf(browser, shareUrl) {
  const page = await browser.newPage();
  await page.setUserAgent(UA);
  let pdfUrl = null;

  page.on("response", (res) => {
    const u = res.url();
    const ct = res.headers()["content-type"] || "";
    if (res.status() !== 200 || !ct.includes("application/pdf")) return;
    if (u.includes("thumbnail")) return;
    if (u.includes("/processed") || u.includes("/download/")) {
      pdfUrl = u;
    }
  });

  try {
    await page.goto(shareUrl, { waitUntil: "networkidle2", timeout: 120000 });
    await sleep(6000);
  } catch (err) {
    await page.close();
    throw new Error(`Showpad load failed: ${err.message}`);
  }

  await page.close();
  if (!pdfUrl) throw new Error("No PDF URL captured from Showpad (may be interactive HTML-only)");
  return pdfUrl;
}

async function downloadPdfBuffer(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "application/pdf,*/*" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (!buf.slice(0, 5).toString().startsWith("%PDF")) throw new Error("Response is not a PDF");
  return buf;
}

function savePdfFile(asset, buf) {
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    brandName: asset.brand,
    typeKey: asset.typeKey,
    title: sanitizeFileName(asset.title),
    ext: ".pdf",
  });
  ensureReferenceDirectory(paths.absoluteDir);
  writeCaptureReadme(COMPANY, path.join(resolveReferenceRoot(), COMPANY));
  fs.writeFileSync(paths.absoluteFile, buf);
  appendCaptureLog(COMPANY, {
    url: asset.downloadUrl || asset.sourceUrl,
    sourceUrl: asset.sourceUrl,
    relativePath: paths.relativePath,
    brand: asset.brand,
    typeKey: asset.typeKey,
    showpad: asset.kind === "showpad",
  });
  return paths.relativePath;
}

async function discoverHtmlPages(browser) {
  const brandUrls = await discoverInternalLinks(browser, {
    seedUrl: `${BASE}/hotel-brands`,
    pathPattern: /\/hotel-brands\//i,
  });
  const brandPages = brandUrls.map((u) => ({
    url: u,
    title: slugToTitle(u),
    category: "Brand page",
    brand: slugToTitle(u),
    typeKey: "development-brochure",
  }));

  const curated = CURATED_HTML_PAGES.map((p) => ({ ...p, typeKey: "development-brochure" }));
  const merged = new Map();
  for (const p of [...curated, ...brandPages]) {
    if (!merged.has(p.url)) merged.set(p.url, p);
  }
  return [...merged.values()];
}

async function runPdfHarvest(browser, report) {
  let discovered = await discoverResourceLinks(browser);
  console.log("Discovered resource links:", discovered.length);
  report.discovered = discovered;

  const manifestPath = path.join(ROOT, "reports", "ihg-resources-discovered.json");
  fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
  fs.writeFileSync(manifestPath, JSON.stringify(discovered, null, 2));

  const jobs = discovered.slice(0, MAX).map((d) => ({
    ...d,
    typeKey: inferTypeKey(d.title),
    brand: inferBrand(d.title),
  }));

  if (!RESOURCES_ONLY && !HTML_ONLY) {
    for (const s of SUPPLEMENTAL_PDFS) {
      if (!jobs.some((j) => j.sourceUrl === s.url)) {
        jobs.push({ ...s, sourceUrl: s.url, kind: "direct-pdf", downloadUrl: s.url });
      }
    }
  }

  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i];
    console.log(`[PDF ${i + 1}/${jobs.length}]`, job.title);

    if (!APPLY) {
      console.log("  WOULD", job.kind, job.sourceUrl);
      continue;
    }

    try {
      let downloadUrl = job.downloadUrl || job.sourceUrl;
      if (job.kind === "showpad") {
        downloadUrl = await resolveShowpadPdf(browser, job.sourceUrl);
        console.log("  resolved", downloadUrl.slice(0, 100), "...");
      }
      const buf = await downloadPdfBuffer(downloadUrl);
      const relativePath = savePdfFile({ ...job, downloadUrl }, buf);
      report.downloaded.push({
        title: job.title,
        sourceUrl: job.sourceUrl,
        downloadUrl,
        relativePath,
        bytes: buf.length,
        brand: job.brand,
        typeKey: job.typeKey,
      });
      console.log("  OK", relativePath, `(${buf.length} bytes)`);
      await sleep(1500);
    } catch (err) {
      const msg = err?.message || String(err);
      report.errors.push({ title: job.title, sourceUrl: job.sourceUrl, error: msg, phase: "pdf" });
      console.warn("  FAIL PDF", msg);
      if (job.kind === "showpad" && /interactive|No PDF/i.test(msg) && !SKIP_HTML) {
        try {
          const htmlResult = await captureShowpadHtml(browser, {
            companyFolder: COMPANY,
            sourceUrl: job.sourceUrl,
            title: job.title,
            brand: job.brand,
            typeKey: job.typeKey,
          });
          report.showpadHtmlCaptured.push({ title: job.title, sourceUrl: job.sourceUrl, ...htmlResult });
          console.log("  OK Showpad HTML fallback", htmlResult.relativePath);
        } catch (htmlErr) {
          report.errors.push({
            title: job.title,
            sourceUrl: job.sourceUrl,
            error: htmlErr?.message || String(htmlErr),
            phase: "showpad-html-fallback",
          });
        }
      }
    }
  }
}

async function runHtmlHarvest(browser, report) {
  const pages = await discoverHtmlPages(browser);
  console.log("HTML pages to capture:", pages.length);
  report.htmlPagesQueued = pages.length;

  const limit = Math.min(pages.length, MAX);
  for (let i = 0; i < limit; i++) {
    const page = pages[i];
    console.log(`[HTML ${i + 1}/${limit}]`, page.title);

    if (!APPLY) {
      console.log("  WOULD", page.url);
      continue;
    }

    try {
      const result = await captureHtmlWithBrowser(browser, {
        url: page.url,
        title: page.title,
        companyFolder: COMPANY,
        typeKey: page.typeKey || "development-brochure",
        brand: page.brand,
        category: page.category,
        alsoPdf: ALSO_PDF || page.category !== "Brand page",
        alsoMhtml: page.category !== "Brand page",
        gotoTimeout: page.url.includes("/owner-value") ? 180000 : 120000,
      });
      report.htmlCaptured.push({ ...page, ...result });
      console.log("  OK", result.relativePath);
      await sleep(1200);
    } catch (err) {
      const msg = err?.message || String(err);
      report.errors.push({ url: page.url, title: page.title, error: msg, phase: "html" });
      console.warn("  FAIL HTML", msg);
    }
  }
}

async function main() {
  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    company: COMPANY,
    resourcesUrl: RESOURCES_URL,
    discovered: [],
    downloaded: [],
    htmlCaptured: [],
    showpadHtmlCaptured: [],
    htmlPagesQueued: 0,
    skipped: [],
    errors: [],
    notes: [
      "Exhaustive harvest: /resources Showpad PDFs + supplemental PDFs + sitemap/curated HTML + Showpad HTML fallbacks.",
      "FDDs not on /resources — request via development.ihg.com/contact-us.",
    ],
  };

  console.log("Launching browser for IHG exhaustive harvest…");
  const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });

  const companyDir = path.join(resolveReferenceRoot(), COMPANY);
  ensureReferenceDirectory(companyDir);
  writeCaptureReadme(COMPANY, companyDir);

  if (!HTML_ONLY) {
    await runPdfHarvest(browser, report);
  }

  if (!RESOURCES_ONLY && !SKIP_HTML) {
    await runHtmlHarvest(browser, report);
  }

  await browser.close();

  const outPath = path.join(ROOT, "reports", "ihg-reference-materials-harvest.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);
  console.log(
    "PDFs:",
    report.downloaded.length,
    "HTML:",
    report.htmlCaptured.length,
    "Showpad HTML:",
    report.showpadHtmlCaptured.length,
    "Errors:",
    report.errors.length,
  );
  if (!APPLY) console.log("Dry run — add --apply for full exhaustive harvest (PDFs + HTML + Showpad fallbacks).");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
