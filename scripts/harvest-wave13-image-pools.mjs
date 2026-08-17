#!/usr/bin/env node
/**
 * Wave 13 — harvest Accor ALL.accor.com property images → wave13-{slug}-gallery-pool.json
 *
 * Source of truth: Wave 13 source-pack propertyExamples (+ supplemental ALL hotel pages
 * where source pack only lists fairmont.com / so-hotels.com consumer URLs).
 *
 * CDN: www.ahstatic.com/photos/{code}_{type}_{seq}_p_{size}.jpg
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE13_STAGE4_APPROVED_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave13-factory-plan.js";
import { getWave13SourcePack } from "../lib/partner-intelligence/brand-explorer-wave13-source-packs-content.js";
import {
  detectVisualCategory,
  IMAGE_ROLES,
} from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { buildImageIdentity } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS = path.join(ROOT, "reports");

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

/** Supplemental ALL hotel pages when source-pack URLs are consumer sites. */
const SUPPLEMENTAL_ALL_PROPERTIES = Object.freeze({
  "fairmont-hotels-and-resorts": [
    {
      propertyName: "Fairmont Mayakoba",
      market: "Riviera Maya, Mexico",
      geographyLabel: "CALA",
      url: "https://all.accor.com/hotel/A573/index.en.shtml",
    },
    {
      propertyName: "Fairmont El San Juan Hotel",
      market: "San Juan, Puerto Rico",
      geographyLabel: "CALA",
      url: "https://all.accor.com/hotel/B6V4/index.en.shtml",
    },
    {
      propertyName: "Fairmont Royal Pavilion",
      market: "Barbados",
      geographyLabel: "CALA",
      url: "https://all.accor.com/hotel/A552/index.en.shtml",
    },
  ],
  "so-hotels-and-resorts": [
    {
      propertyName: "SO/ Paris",
      market: "Paris, France",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/A7L5/index.en.shtml",
    },
    {
      propertyName: "SO/ Berlin Das Stue",
      market: "Berlin, Germany",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/B1Y6/index.en.shtml",
    },
    {
      propertyName: "SO/ Maldives",
      market: "Maldives",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/B986/index.en.shtml",
    },
    {
      propertyName: "SO/ Bangkok",
      market: "Bangkok, Thailand",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/6835/index.en.shtml",
    },
  ],
  "mama-shelter": [
    {
      propertyName: "Mama Shelter Lisbon",
      market: "Lisbon, Portugal",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/B5I0/index.en.shtml",
    },
  ],
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function propertyKeyFromName(name) {
  return nz(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function hotelCodeFromAllUrl(url) {
  const m = nz(url).match(/all\.accor\.com\/hotel\/([A-Za-z0-9]+)/i);
  return m ? m[1].toUpperCase() : null;
}

function isUsableAhstatic(u) {
  if (!/ahstatic\.com\/photos\//i.test(u)) return false;
  if (/_p_120x90|_p_346x260|_p_74x74|_p_180x135/i.test(u)) return false;
  if (!/_p_1024x768|_p_2048x1536/i.test(u)) return false;
  if (/logo|icon|sprite|favicon|badge/i.test(u)) return false;
  return true;
}

function prefer1024(urls) {
  const byId = new Map();
  for (const u of urls) {
    const id = buildImageIdentity(u);
    const key = id.duplicateGroupId || u.replace(/_p_\d+x\d+/i, "_p_SIZED");
    const prev = byId.get(key);
    if (!prev) {
      byId.set(key, u);
      continue;
    }
    // Prefer 1024 over 2048 (same asset)
    if (/_p_1024x768/i.test(u) && /_p_2048x1536/i.test(prev)) byId.set(key, u);
  }
  return [...byId.values()];
}

function roleFromUrl(imageUrl) {
  const det = detectVisualCategory({ imageUrl });
  if (det.category && det.category !== IMAGE_ROLES.unknown) return det.category;
  return "";
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": UA,
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-US,en;q=0.9",
    },
    redirect: "follow",
  });
  const html = await res.text();
  return { status: res.status, html, finalUrl: res.url };
}

function extractAhstatic(html) {
  const found = [];
  for (const m of html.matchAll(/https?:\/\/(?:www\.)?ahstatic\.com\/photos\/[^"'\\\s<>]+/gi)) {
    let u = m[0].replace(/[),.;]+$/g, "").replace(/&amp;/g, "&");
    if (!u.startsWith("https://")) u = u.replace(/^http:/i, "https:");
    if (!u.includes("www.ahstatic.com")) {
      u = u.replace("://ahstatic.com", "://www.ahstatic.com");
    }
    if (isUsableAhstatic(u)) found.push(u.toLowerCase().includes("www.ahstatic") ? u : u);
  }
  // Normalize host
  return prefer1024(
    found.map((u) => u.replace("://ahstatic.com/", "://www.ahstatic.com/"))
  );
}

function catalogForSlug(slug) {
  const pack = getWave13SourcePack(slug);
  const fromPack = (pack?.propertyExamples || [])
    .filter((p) => /all\.accor\.com\/hotel\//i.test(nz(p.url)))
    .map((p) => ({
      propertyName: p.propertyName,
      market: p.market,
      geographyLabel: p.geographyLabel || "International Reference",
      url: p.url,
    }));
  const supplemental = SUPPLEMENTAL_ALL_PROPERTIES[slug] || [];
  const byUrl = new Map();
  for (const p of [...fromPack, ...supplemental]) {
    if (!byUrl.has(p.url)) byUrl.set(p.url, p);
  }
  return [...byUrl.values()];
}

async function harvestSlug(slug) {
  const catalog = catalogForSlug(slug);
  const pool = [];
  const propertyReports = [];

  for (const prop of catalog) {
    const code = hotelCodeFromAllUrl(prop.url);
    process.stdout.write(`[wave13-harvest] ${slug} ${prop.propertyName} (${code || "?"})... `);
    try {
      const { status, html } = await fetchHtml(prop.url);
      const images = extractAhstatic(html);
      console.log(`status=${status} images=${images.length}`);
      propertyReports.push({
        propertyName: prop.propertyName,
        url: prop.url,
        hotelCode: code,
        status,
        imageCount: images.length,
      });
      const key = propertyKeyFromName(prop.propertyName);
      const marketCity = nz(prop.market).split(",")[0].trim();
      for (const imageUrl of images) {
        const urlCode = (imageUrl.match(/ahstatic\.com\/photos\/([a-z0-9]+)_/i) || [])[1];
        // Accor ALL pages embed sibling/nearby hotel thumbs — keep property-code matches only.
        if (code && urlCode && String(code).toLowerCase() !== String(urlCode).toLowerCase()) {
          continue;
        }
        pool.push({
          propertyKey: key,
          propertyName: prop.propertyName,
          marketCity,
          geographyLabel: prop.geographyLabel || "International Reference",
          sourcePageUrl: prop.url,
          imageUrl,
          label: "property",
          role: roleFromUrl(imageUrl) || undefined,
          hotelCode: code,
        });
      }
    } catch (err) {
      console.log(`ERR ${err.message}`);
      propertyReports.push({
        propertyName: prop.propertyName,
        url: prop.url,
        hotelCode: code,
        error: err.message,
        imageCount: 0,
      });
    }
    await sleep(450);
  }

  // Prefer rows with role metadata; keep uniqueness by duplicateGroupId
  const seen = new Set();
  const deduped = [];
  for (const row of pool) {
    const id = buildImageIdentity(row.imageUrl, { propertyName: row.propertyName });
    const g = id.duplicateGroupId || row.imageUrl;
    if (seen.has(g)) continue;
    seen.add(g);
    deduped.push(row);
  }

  return { slug, catalogCount: catalog.length, pool: deduped, propertyReports };
}

async function main() {
  const argv = process.argv.slice(2);
  const idx = argv.indexOf("--brands");
  const brands =
    idx >= 0 && argv[idx + 1]
      ? argv[idx + 1]
          .split(",")
          .map((s) => s.trim().toLowerCase())
          .filter((s) => WAVE13_STAGE4_APPROVED_SLUGS.includes(s))
      : [...WAVE13_STAGE4_APPROVED_SLUGS];

  fs.mkdirSync(FIXTURES, { recursive: true });
  fs.mkdirSync(REPORTS, { recursive: true });

  const report = {
    version: "wave13-image-harvest-v1",
    generatedAt: new Date().toISOString(),
    brands: {},
  };

  for (const slug of brands) {
    const result = await harvestSlug(slug);
    const fixturePath = path.join(FIXTURES, `wave13-${slug}-gallery-pool.json`);
    fs.writeFileSync(fixturePath, `${JSON.stringify(result.pool, null, 2)}\n`);
    console.log(`Wrote ${fixturePath} (${result.pool.length} distinct)`);
    report.brands[slug] = {
      fixturePath: path.relative(ROOT, fixturePath).replace(/\\/g, "/"),
      distinctCount: result.pool.length,
      catalogCount: result.catalogCount,
      properties: result.propertyReports,
      roleCounts: result.pool.reduce((acc, r) => {
        const k = r.role || "untyped";
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
    };
  }

  const out = path.join(REPORTS, "brand-explorer-wave13-image-harvest.json");
  fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Wrote ${out}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
