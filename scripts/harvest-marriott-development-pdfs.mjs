/**
 * Discover + download public PDFs from hotel-development.marriott.com
 *
 *   node scripts/harvest-marriott-development-pdfs.mjs
 *   node scripts/harvest-marriott-development-pdfs.mjs --apply
 *   node scripts/harvest-marriott-development-pdfs.mjs --apply --register --max 50
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
import { createPartnerSource } from "../lib/partner-intelligence/airtable-source.js";
import { MAP_PARTNER_SOURCE } from "../api/lib/partner-intelligence-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASE = "https://hotel-development.marriott.com";
const COMPANY = "Marriott International";
const APPLY = process.argv.includes("--apply");
const REGISTER = process.argv.includes("--register");
const maxArg = process.argv.find((a) => a.startsWith("--max="));
const MAX = maxArg ? Number(maxArg.split("=")[1]) : 500;

const UA = "DealalityReferenceCapture/1.0 (+https://dealality.com)";

const SEED_PATHS = [
  "/",
  "/brands",
  "/how-we-work-together/resources",
  "/brands/luxury-brands",
  "/brands/premium-brands",
  "/brands/select-brands",
  "/brands/extended-stay-brands",
  "/brands/midscale-brands",
  "/brands/all-inclusive-brands",
  "/sitemap.xml",
  "/sitemap_index.xml",
  "/robots.txt",
];

/** Known brand slugs on development site (expand as discovered) */
const BRAND_SLUGS = [
  "ac-hotels",
  "aloft-hotels",
  "autograph-collection-hotels",
  "bulgari-hotels-and-resorts",
  "city-express",
  "courtyard",
  "delta-hotels",
  "edition",
  "element-hotels",
  "fairfield",
  "four-points",
  "gaylord-hotels",
  "homes-and-villas-by-marriott-bonvoy",
  "jw-marriott",
  "le-meridien",
  "luxury-collection",
  "marriott-hotels",
  "marriott-vacation-club",
  "moxy-hotels",
  "protea-hotels",
  "renaissance-hotels",
  "residence-inn",
  "ritz-carlton",
  "sheraton",
  "springhill-suites",
  "st-regis",
  "tribute-portfolio",
  "towneplace-suites",
  "w-hotels",
  "westin",
  "citizenm",
  "series-by-marriott",
  "outdoor-collection",
  "four-points-flex",
  "studio-res",
  "sonder",
  "mgm-collection",
  "marriott-executive-apartments",
  "marriott-conference-centers",
];

function absUrl(href) {
  if (!href) return null;
  if (href.startsWith("http")) return href.split("#")[0];
  if (href.startsWith("//")) return `https:${href.split("#")[0]}`;
  if (href.startsWith("/")) return `${BASE}${href.split("#")[0]}`;
  return `${BASE}/${href.split("#")[0]}`;
}

function isPdfUrl(url) {
  try {
    const p = new URL(url).pathname.toLowerCase();
    return p.endsWith(".pdf");
  } catch {
    return /\.pdf(\?|$)/i.test(url);
  }
}

function normalizePdfUrl(url) {
  try {
    const u = new URL(url);
    u.hostname = u.hostname.replace(/^hotel-development\./, "www.hotel-development.");
    u.hash = "";
    return u.toString();
  } catch {
    return url;
  }
}

/** Parse brand name from Marriott 2026 FDD filenames */
function brandFromFddFilename(fileName) {
  const base = fileName.replace(/\.pdf$/i, "");
  const m = base.match(/2026-(.+?)-fdd/i);
  if (!m) return null;
  return m[1]
    .replace(/-by-marriott$/i, "")
    .replace(/-and-jw-marriott$/i, " / JW Marriott")
    .replace(/-by-sheraton$/i, " by Sheraton")
    .split("-")
    .map((w) => (w.length <= 3 ? w.toUpperCase() : w.charAt(0).toUpperCase() + w.slice(1)))
    .join(" ")
    .replace(/^The /, "The ");
}

function inferBrandFromUrl(url, text = "") {
  const fromFdd = brandFromFddFilename(path.basename(url));
  if (fromFdd) return fromFdd;
  const blob = `${url} ${text}`.toLowerCase();
  for (const slug of BRAND_SLUGS) {
    const name = slug.replace(/-/g, " ");
    if (blob.includes(slug) || blob.includes(name)) {
      return slug
        .split("-")
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
        .join(" ")
        .replace(/Hotels/g, "Hotels")
        .replace(/And/g, "and");
    }
  }
  const m = url.match(/\/brands\/([^/]+)/i);
  if (m) {
    return m[1]
      .split("-")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ");
  }
  return null;
}

function inferType(url, fileName) {
  const b = `${url} ${fileName}`.toLowerCase();
  if (/fdd|franchise.disclosure|item\s*19/i.test(b)) return "fdd";
  if (/one.pager|one-sheet|factsheet|fact.sheet/i.test(b)) return "one-sheet";
  return "development-brochure";
}

async function fetchText(url) {
  const res = await fetch(url, { headers: { "User-Agent": UA }, redirect: "follow" });
  if (!res.ok) return { ok: false, status: res.status, url };
  const ct = res.headers.get("content-type") || "";
  const text = await res.text();
  return { ok: true, url, contentType: ct, text };
}

function extractLinks(html, pageUrl) {
  const $ = cheerio.load(html);
  const out = new Set();
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    const u = absUrl(href);
    if (u && u.includes("hotel-development.marriott.com")) out.add(u);
    if (u && isPdfUrl(u)) out.add(u);
  });
  // Raw PDF URLs in page source
  const pdfMatches = html.match(/https?:\/\/[^\s"'<>]+\.pdf[^\s"'<>]*/gi) || [];
  for (const p of pdfMatches) out.add(p.split("\\")[0]);
  const resourceMatches =
    html.match(/\/ResourceFiles\/[^\s"'<>]+\.pdf/gi) ||
    html.match(/https?:\/\/[^\s"'<>]*ResourceFiles[^\s"'<>]*\.pdf/gi) ||
    [];
  for (const r of resourceMatches) out.add(absUrl(r));
  return [...out];
}

function parseSitemap(xml) {
  const urls = [];
  const re = /<loc>([^<]+)<\/loc>/gi;
  let m;
  while ((m = re.exec(xml))) urls.push(m[1].trim());
  return urls;
}

async function discoverUrls() {
  const queue = new Set(SEED_PATHS.map((p) => `${BASE}${p}`));
  for (const slug of BRAND_SLUGS) {
    queue.add(`${BASE}/brands/${slug}`);
    queue.add(`${BASE}/${slug}`);
  }

  const visited = new Set();
  const pdfUrls = new Map(); /** url -> { foundOn } */
  const pages = [];

  while (queue.size > 0 && visited.size < 80) {
    const url = queue.values().next().value;
    queue.delete(url);
    if (visited.has(url) || !url.includes("marriott.com")) continue;
    visited.add(url);

    try {
      if (isPdfUrl(url)) {
        pdfUrls.set(normalizePdfUrl(url), { foundOn: "direct" });
        continue;
      }
      const res = await fetchText(url);
      if (!res.ok) continue;
      pages.push(url);

      if (url.endsWith(".xml") || res.contentType.includes("xml")) {
        for (const loc of parseSitemap(res.text)) {
          if (isPdfUrl(loc)) pdfUrls.set(normalizePdfUrl(loc), { foundOn: url });
          else if (loc.includes("hotel-development.marriott.com")) queue.add(loc);
        }
        continue;
      }

      for (const link of extractLinks(res.text, url)) {
        if (isPdfUrl(link)) pdfUrls.set(normalizePdfUrl(link), { foundOn: url });
        else if (link.includes("hotel-development.marriott.com") && !visited.has(link)) {
          queue.add(link);
        }
      }
      await new Promise((r) => setTimeout(r, 250));
    } catch (e) {
      console.warn("Skip", url, e.message);
    }
  }

  return { pdfUrls: [...pdfUrls.entries()], pagesVisited: visited.size, pageList: pages };
}

async function downloadPdf(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "application/pdf,*/*" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length < 500) throw new Error("File too small");
  const head = buf.slice(0, 5).toString();
  if (!head.startsWith("%PDF")) throw new Error("Not a PDF");
  return buf;
}

async function main() {
  console.log("Discovering Marriott development PDFs…");
  const { pdfUrls, pagesVisited, pageList } = await discoverUrls();
  console.log("Pages crawled:", pagesVisited);
  console.log("PDFs found:", pdfUrls.length);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    company: COMPANY,
    pagesVisited,
    pdfCount: pdfUrls.length,
    pdfs: [],
    downloaded: [],
    skipped: [],
    errors: [],
  };

  const limit = Math.min(pdfUrls.length, MAX);
  for (let i = 0; i < limit; i++) {
    const [pdfUrl, meta] = pdfUrls[i];
    const fileName = decodeURIComponent(path.basename(new URL(pdfUrl).pathname));
    const brand = inferBrandFromUrl(pdfUrl, fileName) || inferBrandFromUrl(meta.foundOn || "");
    const typeKey = inferType(pdfUrl, fileName);
    const title = sanitizeFileName(fileName.replace(/\.pdf$/i, ""));

    const entry = { pdfUrl, fileName, brand, typeKey, foundOn: meta.foundOn };
    report.pdfs.push(entry);

    if (!APPLY) {
      console.log("WOULD", brand || "?", typeKey, fileName);
      continue;
    }

    try {
      const buf = await downloadPdf(pdfUrl);
      const paths = buildReferenceMaterialPaths({
        companyFolder: COMPANY,
        brandName: brand || undefined,
        typeKey,
        title,
        ext: ".pdf",
      });
      ensureReferenceDirectory(paths.absoluteDir);
      writeCaptureReadme(COMPANY, path.join(resolveReferenceRoot(), COMPANY));
      fs.writeFileSync(paths.absoluteFile, buf);
      appendCaptureLog(COMPANY, { url: pdfUrl, relativePath: paths.relativePath, brand, typeKey });

      let sourceId = null;
      if (REGISTER) {
        const fields = {
          [MAP_PARTNER_SOURCE.sourceTitle]: title,
          [MAP_PARTNER_SOURCE.profileType]: "Brand",
          [MAP_PARTNER_SOURCE.sourceUrl]: pdfUrl,
          [MAP_PARTNER_SOURCE.localFilePath]: paths.relativePath,
          [MAP_PARTNER_SOURCE.sourceType]: paths.typeMeta.sourceType,
          [MAP_PARTNER_SOURCE.sourceOrigin]: paths.typeMeta.origin,
          [MAP_PARTNER_SOURCE.sourceQuality]: typeKey === "fdd" ? "High" : "Medium",
          [MAP_PARTNER_SOURCE.status]: "Captured",
          [MAP_PARTNER_SOURCE.visibility]: "Public",
          [MAP_PARTNER_SOURCE.verifiedSource]: "No",
          [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
          [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
          [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
          [MAP_PARTNER_SOURCE.notes]: `Marriott harvest — found on ${meta.foundOn}`,
        };
        const rec = await createPartnerSource(fields);
        sourceId = rec.id;
      }

      report.downloaded.push({ ...entry, relativePath: paths.relativePath, bytes: buf.length, sourceId });
      console.log("OK", paths.relativePath, `(${buf.length} bytes)`);
      await new Promise((r) => setTimeout(r, 400));
    } catch (err) {
      report.errors.push({ ...entry, error: err.message });
      console.warn("FAIL", fileName, err.message);
    }
  }

  const outPath = path.join(ROOT, "reports", "marriott-development-harvest.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);
  console.log("Downloaded:", report.downloaded.length, "Errors:", report.errors.length);
  if (!APPLY) console.log("Dry run — add --apply to download.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
