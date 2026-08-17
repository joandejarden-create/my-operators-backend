/**
 * Harvest public Marriott reference materials matching Partner Intelligence collection categories.
 *
 *   node scripts/harvest-marriott-reference-materials.mjs
 *   node scripts/harvest-marriott-reference-materials.mjs --apply
 *   node scripts/harvest-marriott-reference-materials.mjs --apply --skip-fdd
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import {
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  writeCaptureReadme,
  appendCaptureLog,
  resolveReferenceRoot,
  sanitizeFileName,
} from "../lib/partner-intelligence/reference-material-paths.js";
import { prepareOfflineHtml, isBlockedOrErrorPage } from "../lib/partner-intelligence/harvest-browser-capture.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASE = "https://www.hotel-development.marriott.com";
const COMPANY = "Marriott International";
const APPLY = process.argv.includes("--apply");
const SKIP_FDD = process.argv.includes("--skip-fdd");
const UA = "DealalityReferenceCapture/1.0 (+https://dealality.com)";

/** @typedef {{ url: string, title: string, typeKey: string, category: string, brand?: string, subfolder?: string }} CuratedAsset */

const CURATED_PDFS = /** @type {CuratedAsset[]} */ ([
  {
    url: "https://www.hotel-development.marriott.com/ResourceFiles/common-image/fairfield-emea-one-pager.pdf",
    title: "Fairfield EMEA one-pager",
    typeKey: "one-sheet",
    category: "Brochures / one-pagers",
    brand: "Fairfield",
  },
  {
    url: "https://www.hotel-development.marriott.com/ResourceFiles/common-image/moxy-mea-fact-sheet-v3-220622.pdf",
    title: "Moxy MEA fact sheet",
    typeKey: "one-sheet",
    category: "Brochures / one-pagers",
    brand: "Moxy",
  },
  {
    url: "https://www.hotel-development.marriott.com/resourcefiles/fdd-document/four-points-express-development-overview.pdf",
    title: "Four Points Express development overview",
    typeKey: "development-brochure",
    category: "General development presentation",
    brand: "Four Points Express",
  },
  {
    url: "https://news.marriott.com/static-assets/component-resources/newscenter/earnings/2026/2026-q1-earnings-infographic.pdf",
    title: "2026 Q1 earnings infographic",
    typeKey: "press",
    category: "Portfolio overview",
  },
  {
    url: "https://news.marriott.com/static-assets/component-resources/newscenter/earnings/2025/2025-q4-earnings-infographic.pdf",
    title: "2025 Q4 earnings infographic",
    typeKey: "press",
    category: "Portfolio overview",
  },
  {
    url: "https://news.marriott.com/static-assets/component-resources/newscenter/earnings/2025/2025-q3-earnings-infographic.pdf",
    title: "2025 Q3 earnings infographic",
    typeKey: "press",
    category: "Portfolio overview",
  },
]);

/** HTML pages worth archiving when no PDF exists */
const CURATED_HTML = /** @type {CuratedAsset[]} */ ([
  {
    url: `${BASE}/`,
    title: "Marriott development home",
    typeKey: "development-brochure",
    category: "General company overview",
  },
  {
    url: `${BASE}/hotel-development`,
    title: "Hotel development overview",
    typeKey: "development-brochure",
    category: "General development presentation",
  },
  {
    url: `${BASE}/power-of-marriott`,
    title: "Power of Marriott",
    typeKey: "development-brochure",
    category: "Commercial strategy / Bonvoy platform",
  },
  {
    url: `${BASE}/power-of-marriott/our-story`,
    title: "Power of Marriott — our story",
    typeKey: "development-brochure",
    category: "General company overview",
  },
  {
    url: `${BASE}/how-we-work-together/managed-by-marriott`,
    title: "Managed by Marriott (MxM)",
    typeKey: "development-brochure",
    category: "Operating model",
  },
  {
    url: `${BASE}/meet-the-team`,
    title: "Development team including CALA",
    typeKey: "development-brochure",
    category: "Leadership team",
  },
  {
    url: `${BASE}/brands/cityexpressbymarriott`,
    title: "City Express by Marriott — CALA midscale",
    typeKey: "regional",
    category: "CALA expansion / target markets",
    brand: "City Express",
  },
  {
    url: `${BASE}/brands/all-inclusive`,
    title: "All-inclusive brands",
    typeKey: "development-brochure",
    category: "Resorts / complex ownership",
  },
  {
    url: `${BASE}/brands/apartments`,
    title: "Apartments by Marriott Bonvoy",
    typeKey: "development-brochure",
    category: "Condo / residential / complex ownership",
  },
  {
    url: `${BASE}/global-expansion-of-fairfield-hotels`,
    title: "Fairfield global expansion",
    typeKey: "regional",
    category: "Portfolio / markets",
    brand: "Fairfield",
  },
  {
    url: `${BASE}/global-expansion-of-moxy-hotels`,
    title: "Moxy global expansion",
    typeKey: "regional",
    category: "Portfolio / markets",
    brand: "Moxy",
  },
  {
    url: `${BASE}/brands`,
    title: "Brand portfolio",
    typeKey: "development-brochure",
    category: "Brand relationship information",
  },
]);

const BRAND_SLUGS = [
  "cityexpressbymarriott", "fairfield", "moxy-hotels", "fourpointsflexbysheraton",
  "studiores", "seriesbymarriott", "autograph-collection-hotels", "tribute-portfolio",
  "luxury-collection", "marriott-hotels", "courtyard", "residence-inn", "element-hotels",
];

function normalizeUrl(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    return u.toString();
  } catch {
    return url;
  }
}

function inferCategory(typeKey, url, fileName) {
  const b = `${url} ${fileName}`.toLowerCase();
  if (/fdd/i.test(b)) return "FDD (franchise legal)";
  if (/cala|latin|caribbean|mexico|emea|mea|city.express/i.test(b)) return "CALA / regional";
  if (/one.pager|fact.sheet/i.test(b)) return "Brochures / one-pagers";
  if (/earnings|infographic/i.test(b)) return "Portfolio overview";
  if (/overview|development/i.test(b)) return "General development presentation";
  return "Development brochure";
}

async function fetchBuffer(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "*/*" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  return { buf, contentType: ct };
}

async function discoverBrochurePdfs() {
  const found = new Map();
  const pages = [
    ...CURATED_PDFS.map((c) => c.url),
    `${BASE}/how-we-work-together/resources`,
    ...BRAND_SLUGS.flatMap((s) => [`${BASE}/brands/${s}`, `${BASE}/${s}`]),
  ];
  for (const pageUrl of [...new Set(pages.filter((u) => !u.endsWith(".pdf")))]) {
    try {
      const res = await fetch(pageUrl, { headers: { "User-Agent": UA }, redirect: "follow" });
      if (!res.ok) continue;
      const html = await res.text();
      const matches = [
        ...(html.matchAll(/\/ResourceFiles\/[^\s"'<>]+\.pdf/gi)),
        ...(html.matchAll(/https?:\/\/[^\s"'<>]*ResourceFiles[^\s"'<>]*\.pdf/gi)),
      ];
      for (const m of matches) {
        const raw = m[0].startsWith("http") ? m[0] : `${BASE}${m[0]}`;
        const url = normalizeUrl(raw.split("\\")[0]);
        if (!found.has(url)) found.set(url, { foundOn: pageUrl });
      }
      await new Promise((r) => setTimeout(r, 150));
    } catch (_) {
      /* continue */
    }
  }
  return found;
}

function buildPathsForAsset(asset, ext) {
  if (asset.brand) {
    return buildReferenceMaterialPaths({
      companyFolder: COMPANY,
      brandName: asset.brand,
      typeKey: asset.typeKey,
      title: asset.title,
      ext,
    });
  }
  const typeMeta = asset.typeKey;
  return buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    typeKey: typeMeta,
    title: asset.title,
    ext,
  });
}

async function savePdf(asset, url, foundOn) {
  const { buf } = await fetchBuffer(url);
  if (!buf.slice(0, 5).toString().startsWith("%PDF")) throw new Error("Not a PDF");
  const fileName = path.basename(new URL(url).pathname);
  if (SKIP_FDD && /fdd/i.test(fileName)) return { skipped: true, reason: "skip-fdd" };

  const paths = buildPathsForAsset(
    {
      ...asset,
      title: asset.title || sanitizeFileName(fileName.replace(/\.pdf$/i, "")),
    },
    ".pdf",
  );
  ensureReferenceDirectory(paths.absoluteDir);
  writeCaptureReadme(COMPANY, path.join(resolveReferenceRoot(), COMPANY));
  fs.writeFileSync(paths.absoluteFile, buf);
  appendCaptureLog(COMPANY, {
    url,
    relativePath: paths.relativePath,
    brand: asset.brand,
    typeKey: asset.typeKey,
    category: asset.category || inferCategory(asset.typeKey, url, fileName),
    foundOn,
  });
  return { relativePath: paths.relativePath, bytes: buf.length };
}

async function saveHtml(asset) {
  const res = await fetch(asset.url, { headers: { "User-Agent": UA }, redirect: "follow" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);
  const title = $("title").text().trim() || asset.title;
  const blockReason = isBlockedOrErrorPage(title, html);
  if (blockReason) throw new Error(blockReason);
  const prepared = prepareOfflineHtml(html, asset.url);
  const cleaned = `<!DOCTYPE html>
<!-- Captured ${new Date().toISOString()} from ${asset.url} -->
<!-- Category: ${asset.category} -->
<!-- Offline viewing: styles/images load from ${new URL(asset.url).origin} when online -->
${prepared}`;

  const paths = buildPathsForAsset({ ...asset, title: sanitizeFileName(asset.title) }, ".html");
  ensureReferenceDirectory(paths.absoluteDir);
  writeCaptureReadme(COMPANY, path.join(resolveReferenceRoot(), COMPANY));
  fs.writeFileSync(paths.absoluteFile, cleaned, "utf8");
  appendCaptureLog(COMPANY, {
    url: asset.url,
    relativePath: paths.relativePath,
    typeKey: asset.typeKey,
    category: asset.category,
    format: "html",
  });
  return { relativePath: paths.relativePath, bytes: cleaned.length, pageTitle: title };
}

async function main() {
  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    company: COMPANY,
    downloaded: [],
    htmlCaptured: [],
    skipped: [],
    notFoundPublicly: [],
    errors: [],
    checklistCoverage: {},
  };

  const discovered = await discoverBrochurePdfs();
  console.log("Discovered ResourceFiles PDFs:", discovered.size);

  const pdfJobs = new Map();
  for (const c of CURATED_PDFS) pdfJobs.set(normalizeUrl(c.url), c);
  for (const [url, meta] of discovered) {
    if (pdfJobs.has(url)) continue;
    const fileName = path.basename(new URL(url).pathname);
    if (/fdd/i.test(fileName)) {
      pdfJobs.set(url, {
        url,
        title: sanitizeFileName(fileName.replace(/\.pdf$/i, "")),
        typeKey: "fdd",
        category: inferCategory("fdd", url, fileName),
        brand: undefined,
      });
    } else {
      pdfJobs.set(url, {
        url,
        title: sanitizeFileName(fileName.replace(/\.pdf$/i, "")),
        typeKey: /one.pager|fact.sheet/i.test(fileName) ? "one-sheet" : "development-brochure",
        category: inferCategory("development-brochure", url, fileName),
      });
    }
  }

  console.log("PDF jobs:", pdfJobs.size, "| HTML pages:", CURATED_HTML.length);

  for (const [url, asset] of pdfJobs) {
    const label = `${asset.category || asset.typeKey} — ${asset.title}`;
    if (!APPLY) {
      console.log("WOULD PDF", label, url);
      continue;
    }
    try {
      const result = await savePdf(asset, url, asset.url);
      if (result.skipped) {
        report.skipped.push({ url, ...asset, reason: result.reason });
        continue;
      }
      report.downloaded.push({ url, ...asset, ...result });
      console.log("OK PDF", result.relativePath);
      await new Promise((r) => setTimeout(r, 300));
    } catch (err) {
      report.errors.push({ url, ...asset, error: err.message });
      console.warn("FAIL PDF", asset.title, err.message);
    }
  }

  for (const asset of CURATED_HTML) {
    if (!APPLY) {
      console.log("WOULD HTML", asset.category, asset.url);
      continue;
    }
    try {
      const result = await saveHtml(asset);
      report.htmlCaptured.push({ ...asset, ...result });
      console.log("OK HTML", result.relativePath);
      await new Promise((r) => setTimeout(r, 200));
    } catch (err) {
      report.errors.push({ url: asset.url, ...asset, error: err.message });
      console.warn("FAIL HTML", asset.title, err.message);
    }
  }

  const brandHtmlJobs = BRAND_SLUGS.map((slug) => ({
    url: `${BASE}/brands/${slug}`,
    title: `${slug.replace(/-/g, " ")} brand page`,
    typeKey: "development-brochure",
    category: "Brand relationship information",
    brand: slug.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
  }));

  console.log("Brand HTML pages:", brandHtmlJobs.length);
  for (const asset of brandHtmlJobs) {
    if (!APPLY) {
      console.log("WOULD HTML brand", asset.url);
      continue;
    }
    try {
      const result = await saveHtml(asset);
      report.htmlCaptured.push({ ...asset, ...result });
      console.log("OK HTML brand", result.relativePath);
      await new Promise((r) => setTimeout(r, 200));
    } catch (err) {
      report.errors.push({ url: asset.url, ...asset, error: err.message });
      console.warn("FAIL HTML brand", asset.title, err.message);
    }
  }

  const categories = [
    ["General company overview or development presentation", ["General company overview", "General development presentation"]],
    ["Operating model, leadership, support infrastructure", ["Operating model", "Leadership team"]],
    ["Portfolio overview / asset mix", ["Portfolio overview", "Portfolio / markets"]],
    ["Resorts, independent, condo, complex ownership", ["Resorts / complex ownership", "Condo / residential / complex ownership"]],
    ["Commercial strategy (sales, marketing, RM, direct booking)", ["Commercial strategy / Bonvoy platform"]],
    ["Brand relationship information", ["Brand relationship information"]],
    ["Brochures, one-pagers, resources for owners", ["Brochures / one-pagers", "Development brochure"]],
    ["CALA-related thinking / target markets", ["CALA expansion / target markets", "CALA / regional"]],
  ];

  const captured = [
    ...report.downloaded.map((d) => d.category),
    ...report.htmlCaptured.map((d) => d.category),
  ];

  for (const [check, tags] of categories) {
    const hits = tags.filter((t) => captured.includes(t));
    report.checklistCoverage[check] = hits.length ? { status: "partial", captured: hits } : { status: "not_found_publicly", note: "No public PDF/deck found; may require owner outreach" };
  }

  report.notFoundPublicly = [
    "Operator / management company pitch deck (MxM overview page captured as HTML only)",
    "Case studies of operated/repositioned hotels",
    "Owner reporting / transition / pre-opening / performance improvement process docs",
    "Videos and imagery bundles (embedded on site, no direct download URLs found)",
    "Dedicated CALA regional development deck (team page + City Express captured instead)",
  ];

  const outPath = path.join(ROOT, "reports", "marriott-reference-materials-harvest.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);
  console.log("PDFs:", report.downloaded.length, "HTML:", report.htmlCaptured.length, "Errors:", report.errors.length);
  if (!APPLY) console.log("Dry run — add --apply to download.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
