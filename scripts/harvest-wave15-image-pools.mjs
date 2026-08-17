#!/usr/bin/env node
/**
 * Wave 15 — harvest official Hilton brand/property images → fixtures/wave15-{slug}-gallery-pool.json
 *
 * Sources tried, in order:
 *   1. Wave 15 tab-factory openings + supplemental Americas / CALA property pages
 *   2. Wave 15 source-pack brand pages + imageSourceHints (stories.hilton.com etc.)
 *   3. Hilton brand landing pages under hilton.com/en/brands/
 *
 * Extracted image hosts: hilton.com/im/, assets.hiltonstatic.com,
 * hiltonstatic.com, stories.hilton.com/uploads, stories-editor.hilton.com/wp-content/uploads.
 * Sibling / wrong-brand imagery is rejected per slug.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE15_STAGE5_APPROVED_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave15-factory-plan.js";
import { getWave15SourcePack } from "../lib/partner-intelligence/brand-explorer-wave15-source-packs-content.js";
import { getWave15SupplementalOpenings } from "../lib/partner-intelligence/brand-explorer-wave15-image-supplemental.js";
import { getWave15BrandContent } from "../lib/partner-intelligence/brand-explorer-wave15-tab-factory-content.js";
import {
  detectVisualCategory,
  IMAGE_ROLES,
} from "../lib/partner-intelligence/brand-explorer-image-role-match.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS = path.join(ROOT, "reports");

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

/**
 * Per-slug Hilton brand landing pages (hilton.com/en/brands/) and Stories
 * From Hilton per-brand hubs. These are surfaced during harvest so brand
 * imagery is discoverable even when property pages 403 to bots.
 */
const BRAND_LANDING_PAGES = Object.freeze({
  "hilton-hotels-and-resorts": [
    "https://www.hilton.com/en/brands/hilton-hotels-resorts/",
    "https://stories.hilton.com/brands/hilton-hotels-and-resorts",
  ],
  "homewood-suites-by-hilton": [
    "https://www.hilton.com/en/brands/homewood-suites/",
    "https://stories.hilton.com/brands/homewood-suites-by-hilton",
  ],
  "home2-suites-by-hilton": [
    "https://www.hilton.com/en/brands/home2-suites/",
    "https://stories.hilton.com/brands/home2-suites-by-hilton",
  ],
  "tru-by-hilton": [
    "https://www.hilton.com/en/brands/tru-by-hilton/",
    "https://stories.hilton.com/brands/tru-by-hilton",
  ],
  "doubletree-by-hilton": [
    "https://www.hilton.com/en/brands/doubletree/",
    "https://stories.hilton.com/brands/doubletree-by-hilton",
  ],
  "hampton-by-hilton": [
    "https://www.hilton.com/en/brands/hampton-by-hilton/",
    "https://stories.hilton.com/brands/hampton-by-hilton",
  ],
  "hilton-garden-inn": [
    "https://www.hilton.com/en/brands/hilton-garden-inn/",
    "https://stories.hilton.com/brands/hilton-garden-inn",
  ],
  "spark-by-hilton": [
    "https://www.hilton.com/en/brands/spark-by-hilton/",
    "https://stories.hilton.com/brands/spark-by-hilton",
  ],
});

/**
 * Sibling rejection per slug — mirrors the runtime rejector but tuned for
 * URL/property-key patterns encountered while scraping brand landing pages.
 */
const SIBLING_RE = Object.freeze({
  "hilton-hotels-and-resorts":
    /doubletree|curio|tapestry|signia|conrad|waldorf|hampton|homewood|home2|home-2|\/tru-|spark|embassy|garden-inn|motto|canopy|tempo/i,
  "homewood-suites-by-hilton":
    /home2|home-2|hampton|spark|garden-inn|tru-by-hilton|embassy|doubletree|curio|tapestry|hilton-hotels/i,
  "home2-suites-by-hilton":
    /homewood|hampton|spark|tru-by-hilton|embassy|doubletree|curio|tapestry|garden-inn|hilton-hotels/i,
  "tru-by-hilton":
    /spark|hampton|home2|home-2|homewood|garden-inn|doubletree|curio|tapestry|embassy|hilton-hotels/i,
  "doubletree-by-hilton":
    /curio|tapestry|embassy|signia|waldorf|conrad|hilton-hotels|homewood|home2|home-2|hampton|garden-inn|tru-by-hilton|spark/i,
  "hampton-by-hilton":
    /tru-by-hilton|spark|garden-inn|home2|home-2|homewood|doubletree|curio|tapestry|hilton-hotels/i,
  "hilton-garden-inn":
    /hampton|hilton-hotels|doubletree|homewood|home2|home-2|tru-by-hilton|spark|curio|tapestry|embassy/i,
  "spark-by-hilton":
    /tru-by-hilton|hampton|home2|home-2|homewood|garden-inn|doubletree|curio|tapestry|hilton-hotels/i,
});

/**
 * Per-slug brand hint regex — Hilton property codes carry a two-letter brand
 * suffix (see wave15-image-materialization for reference).
 */
const BRAND_HINT_RE = Object.freeze({
  "hilton-hotels-and-resorts":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}(?:hh|hf|fhh|chh|bchh)-|\/im\/en\/[a-z0-9]{5,8}(?:hh|hf|fhh|chh|bchh)\//i,
  "homewood-suites-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}hw-|\/im\/en\/[a-z0-9]{5,8}hw\/|homewood-suites/i,
  "home2-suites-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}ht-|\/im\/en\/[a-z0-9]{5,8}ht\/|home2-suites|home-2-suites/i,
  "tru-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}ru-|\/im\/en\/[a-z0-9]{5,8}ru\/|tru-by-hilton|tru-[a-z]+-(?:downtown|airport|midtown|scottsdale|atlanta|miami|nashville|opryland|salt-river|west-brickell|galleria|ballpark|college-park|blue-lagoon|lawrenceville|kennesaw)/i,
  "doubletree-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}dt-|\/im\/en\/[a-z0-9]{5,8}dt\/|doubletree-by-hilton|doubletree/i,
  "hampton-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}hx-|\/im\/en\/[a-z0-9]{5,8}hx\/|hampton-by-hilton|hampton-inn|hampton-[a-z]+-(?:airport|panama|bogota|usaquen|guanacaste)/i,
  "hilton-garden-inn":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}gi-|\/im\/en\/[a-z0-9]{5,8}gi\/|hilton-garden-inn|garden-inn/i,
  "spark-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}pe-|\/im\/en\/[a-z0-9]{5,8}pe\/|spark-by-hilton|spark-[a-z]+-(?:nashville|opryland|atlanta|duluth|cumberland|ballpark|midtown|toronto|markham|oshawa)/i,
});

function decode(s) {
  return String(s || "")
    .replace(/\\u002D/g, "-")
    .replace(/\\u002F/g, "/")
    .replace(/\\\//g, "/")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/%20/g, " ");
}

function cleanUrl(u) {
  return decode(u).replace(/[),.;]+$/g, "").trim();
}

function isJunk(u) {
  return /logo|chiclet|favicon|sprite|icon|badge|pixel|1x1|open-graph|brand-refresh|fmt=png-alpha|getty|placeholder/i.test(
    u
  );
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function stripSizeSuffix(u) {
  return String(u || "")
    .replace(/\?.*$/, "")
    .replace(/-\d{2,4}x\d{2,4}(?=\.(?:jpg|jpeg|png|webp))/i, "")
    .toLowerCase();
}

function dedupeNear(urls) {
  const seen = new Set();
  const out = [];
  for (const u of urls) {
    const key = stripSizeSuffix(u);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(u);
  }
  return out;
}

function extractHiltonImages(html) {
  const decoded = decode(html);
  // hilton.com/im/en/PROPERTY-CODE/photo-id/filename.jpg
  const fromIm = [
    ...decoded.matchAll(
      /https?:\/\/(?:www\.)?hilton\.com\/im\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
    ),
  ]
    .map((m) => cleanUrl(m[0]))
    .filter((u) => !isJunk(u));

  const fromHiltonStatic = [
    ...decoded.matchAll(
      /https?:\/\/(?:assets\.)?hiltonstatic\.com\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
    ),
  ]
    .map((m) => cleanUrl(m[0]))
    .filter((u) => !isJunk(u));

  const fromStoriesUploads = [
    ...decoded.matchAll(
      /https?:\/\/stories(?:-editor)?\.hilton\.com\/(?:wp-content\/)?uploads\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
    ),
  ]
    .map((m) => cleanUrl(m[0]))
    .filter((u) => !isJunk(u));

  const fromCacheHilton = [
    ...decoded.matchAll(
      /https?:\/\/cache\.hilton\.com\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
    ),
  ]
    .map((m) => cleanUrl(m[0]))
    .filter((u) => !isJunk(u));

  return dedupeNear([
    ...fromIm,
    ...fromHiltonStatic,
    ...fromStoriesUploads,
    ...fromCacheHilton,
  ]);
}

function inferRole(imageUrl, sourcePageUrl) {
  const det = detectVisualCategory({ imageUrl, sourcePageUrl });
  if (det.category && det.category !== IMAGE_ROLES.unknown) return det.category;
  const u = imageUrl.toLowerCase();
  if (/exterior|aerial|arrival|entrance|front|facade/i.test(u)) return IMAGE_ROLES.exterior_arrival;
  if (/lobby|public|lounge|bar|reception|social/i.test(u)) return IMAGE_ROLES.public_space_lobby;
  if (/guest.?room|suite|bedroom|king|queen|studio|nks|nkqt|kitchen/i.test(u)) return IMAGE_ROLES.guest_room_suite;
  if (/restaurant|dining|breakfast|fb|food|market|coffee|grille/i.test(u)) return IMAGE_ROLES.food_beverage_experience;
  if (/pool|spa|wellness|fitness|gym/i.test(u)) return IMAGE_ROLES.wellness_pool_spa;
  return IMAGE_ROLES.property_setting;
}

async function fetchHtml(url) {
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": UA,
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
      },
      redirect: "follow",
    });
    return { status: res.status, html: decode(await res.text()), finalUrl: res.url };
  } catch (err) {
    return { status: 0, html: "", finalUrl: url, error: String(err?.message || err) };
  }
}

function propertyPagesForSlug(slug) {
  let contentOpenings = [];
  try {
    contentOpenings = getWave15BrandContent(slug)?.openings || [];
  } catch {
    contentOpenings = [];
  }
  let pack = null;
  try {
    pack = getWave15SourcePack(slug);
  } catch {
    pack = null;
  }
  const supplemental = getWave15SupplementalOpenings(slug);
  const out = [];
  const seen = new Set();
  const push = (r) => {
    if (!r?.url || !r?.propertyName) return;
    if (!/hilton\.com\/en\/hotels\/[a-z0-9-]+/i.test(r.url)) return;
    const key = slugify(r.propertyName);
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      propertyName: r.propertyName,
      url: r.url,
      marketCity: r.marketCity || (r.market || "").split(",")[0].trim(),
      geographyLabel: r.geographyLabel || "International Reference",
    });
  };
  for (const r of contentOpenings) push(r);
  for (const r of pack?.propertyExamples || []) push(r);
  for (const r of supplemental) push(r);
  return out;
}

function extractHiltonPropertyLinks(html) {
  return [
    ...new Set(
      [...html.matchAll(/https?:\/\/(?:www\.)?hilton\.com\/en\/hotels\/[a-z0-9-]+\//gi)].map(
        (m) => cleanUrl(m[0])
      )
    ),
  ];
}

async function harvestBrand(slug) {
  const sibling = SIBLING_RE[slug] || /$a/;
  const hint = BRAND_HINT_RE[slug] || /$a/;
  const pool = [];
  const probes = [];
  const push = (ex, imageUrl, label = "property") => {
    const u = cleanUrl(imageUrl);
    if (!u || isJunk(u)) return;
    const lower = u.toLowerCase();
    // Sibling test: reject if sibling matches AND brand hint does NOT.
    if (sibling.test(lower) && !hint.test(lower)) return;
    pool.push({
      propertyKey: slugify(ex.propertyName || "brand"),
      propertyName: ex.propertyName || "",
      marketCity: ex.marketCity || "",
      geographyLabel: ex.geographyLabel || "International Reference",
      sourcePageUrl: ex.url || "",
      imageUrl: u,
      label,
      role: inferRole(u, ex.url || ""),
    });
  };

  let pack = null;
  try {
    pack = getWave15SourcePack(slug);
  } catch {
    pack = null;
  }
  const officialBrandName = pack?.officialBrandName || slug;

  // 1) Wave 15 tab-factory + supplemental property pages
  for (const ex of propertyPagesForSlug(slug)) {
    const { status, html, error } = await fetchHtml(ex.url);
    const imgs = extractHiltonImages(html);
    probes.push({ pageUrl: ex.url, status, count: imgs.length, error });
    for (const u of imgs.slice(0, 40)) push(ex, u, "property");
  }

  // 2) Brand landing pages + Stories From Hilton hubs
  for (const site of BRAND_LANDING_PAGES[slug] || []) {
    const { status, html, finalUrl, error } = await fetchHtml(site);
    const imgs = extractHiltonImages(html);
    const propertyLinks = extractHiltonPropertyLinks(html).filter(
      (u) => !sibling.test(u) || hint.test(u)
    );
    probes.push({
      pageUrl: finalUrl || site,
      status,
      count: imgs.length,
      hotelLinks: propertyLinks.length,
      error,
    });
    const brandEx = {
      propertyName: `${officialBrandName} (brand photography)`,
      marketCity: "",
      geographyLabel: "International Reference",
      url: finalUrl || site,
    };
    for (const u of imgs.slice(0, 40)) push(brandEx, u, "brand_site");

    // Follow up to 4 property links surfaced from the brand hub (extra
    // hilton.com hotel imagery beyond seeded openings).
    for (const hotelUrl of propertyLinks.slice(0, 4)) {
      const { status: st, html: h2 } = await fetchHtml(hotelUrl);
      const imgs2 = extractHiltonImages(h2);
      probes.push({ pageUrl: hotelUrl, status: st, count: imgs2.length });
      const code = (hotelUrl.match(/\/hotels\/([a-z0-9-]+)\//i) || [])[1] || "";
      const nameGuess = code
        .replace(/^[a-z]{3,5}[a-z]{2}-?/, "")
        .split("-")
        .map((w) => (w ? w.charAt(0).toUpperCase() + w.slice(1) : ""))
        .join(" ")
        .trim();
      const ex = {
        propertyName: nameGuess || code,
        marketCity: "",
        geographyLabel: "International Reference",
        url: hotelUrl,
      };
      for (const u of imgs2.slice(0, 20)) push(ex, u, "hotel_link");
    }
  }

  // 3) Wave 15 source-pack imageSourceHints (harvest seeds only)
  for (const hint of pack?.imageSourceHints || []) {
    if (!hint?.url) continue;
    const { status, html } = await fetchHtml(hint.url);
    const imgs = extractHiltonImages(html);
    probes.push({ pageUrl: hint.url, status, count: imgs.length, source: "imageSourceHints" });
    const ex = {
      propertyName: hint.label || `${officialBrandName} (source hint)`,
      marketCity: "",
      geographyLabel: "International Reference",
      url: hint.url,
    };
    for (const u of imgs.slice(0, 20)) push(ex, u, "source_hint");
  }

  // Dedupe pool by near-duplicate key per property
  const seen = new Set();
  const deduped = [];
  for (const row of pool) {
    const key = `${row.propertyKey}::${stripSizeSuffix(row.imageUrl)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }

  return { slug, pool: deduped, probes };
}

async function main() {
  const argv = process.argv.slice(2);
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? String(argv[brandsIdx + 1])
          .split(",")
          .map((s) => s.trim().toLowerCase())
          .filter(Boolean)
      : [...WAVE15_STAGE5_APPROVED_SLUGS];

  fs.mkdirSync(FIXTURES, { recursive: true });
  fs.mkdirSync(REPORTS, { recursive: true });
  const summary = {
    version: "wave15-image-harvest-v1",
    generatedAt: new Date().toISOString(),
    brands: [],
  };

  for (const slug of brands) {
    console.log(`[harvest-wave15] ${slug}`);
    const { pool, probes } = await harvestBrand(slug);
    const out = path.join(FIXTURES, `wave15-${slug}-gallery-pool.json`);
    fs.writeFileSync(out, `${JSON.stringify(pool, null, 2)}\n`);
    summary.brands.push({
      slug,
      fixture: out,
      poolCount: pool.length,
      probes,
    });
    console.log(`  wrote ${pool.length} rows → ${out}`);
  }

  const reportPath = path.join(REPORTS, "brand-explorer-wave15-image-harvest.json");
  fs.writeFileSync(reportPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(`Wrote ${reportPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
