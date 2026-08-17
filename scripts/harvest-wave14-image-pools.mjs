#!/usr/bin/env node
/**
 * Wave 14 — harvest official Marriott brand/property images → fixtures/wave14-{slug}-gallery-pool.json
 *
 * Prefer brand microsites + marriott.com hotel overview links (property pages often 403 to bots).
 * CDN URLs are still recorded for Airtable attachment fetch.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE14_STAGE5_APPROVED_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave14-factory-plan.js";
import { getWave14SourcePack } from "../lib/partner-intelligence/brand-explorer-wave14-source-packs-content.js";
import { getWave14SupplementalOpenings } from "../lib/partner-intelligence/brand-explorer-wave14-image-supplemental.js";
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

const BRAND_MICROSITES = Object.freeze({
  "marriott-hotels": ["https://marriott-hotels.marriott.com/"],
  sheraton: ["https://sheraton.marriott.com/"],
  westin: ["https://westin.marriott.com/"],
  "residence-inn-by-marriott": [
    "https://www.marriott.com/brands/residence-inn.mi",
    "https://residence-inn.marriott.com/",
    "https://residenceinn.marriott.com/",
  ],
  "springhill-suites-by-marriott": ["https://springhillsuites.marriott.com/"],
  "towneplace-suites-by-marriott": ["https://towneplacesuites.marriott.com/"],
  "aloft-hotels": ["https://aloft-hotels.marriott.com/"],
  "four-points-flex-by-sheraton": [
    "https://www.marriott.com/brands/four-points-flex.mi",
    // Live brand page is Getty-heavy; Wayback + development gallery carry Flex property photography.
    "https://web.archive.org/web/20250809060527/https://www.marriott.com/brands/four-points-flex.mi",
    "https://web.archive.org/web/20240419053204/https://www.hotel-development.marriott.com/brands/fourpointsexpress",
  ],
  studiores: ["https://www.marriott.com/brands/studiores.mi"],
});

/** Official brand-site / development photography hosts (non-Scene7). */
const BRAND_SITE_IMAGE_HOST_RE =
  /(?:marriott-hotels|sheraton|westin|springhillsuites|towneplacesuites|aloft-hotels)\.marriott\.com\/wp-content\/uploads\/|hotel-development\.marriott\.com\/resourcefiles\//i;

const SIBLING_RE = Object.freeze({
  "marriott-hotels":
    /jw-marriott|sheraton|westin|renaissance|autograph|tribute|w-hotel|st-regis|residence-inn|springhill|towneplace|aloft|moxy|courtyard|fairfield|four-points|element|studiores|ac-hotel/i,
  sheraton: /jw-marriott|westin|four-points|residence-inn|aloft|moxy|courtyard|renaissance|autograph|tribute|w-hotel|springhill|towneplace|studiores/i,
  westin: /sheraton|jw-marriott|w-hotel|renaissance|autograph|tribute|aloft|moxy|residence-inn|four-points/i,
  "residence-inn-by-marriott":
    /towneplace|studiores|element|apartments-by-marriott|springhill|fairfield|courtyard|aloft|moxy/i,
  "springhill-suites-by-marriott":
    /residence-inn|towneplace|fairfield|courtyard|studiores|element|aloft|moxy/i,
  "towneplace-suites-by-marriott":
    /residence-inn|studiores|springhill|element|apartments-by-marriott|fairfield|courtyard|aloft/i,
  "aloft-hotels": /moxy|ac-hotel|four-points|element|w-hotel|courtyard|autograph|tribute|residence-inn/i,
  "four-points-flex-by-sheraton":
    /four-points-by-sheraton|\/fp[a-z]{2}-|sheraton-(?!.*flex)|marriott-hotels|westin|aloft|moxy|courtyard/i,
  studiores: /residence-inn|towneplace|element|apartments-by-marriott|springhill|fairfield|courtyard|aloft/i,
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
  return decode(u)
    .replace(/[),.;]+$/g, "")
    .trim();
}

function isJunk(u) {
  return /logo|chiclet|favicon|sprite|icon|badge|pixel|1x1|open-graph|brand-refresh|fmt=png-alpha|getty/i.test(
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
    .replace(/:Wide-Hor|:Feature-Hor|:Pano-Hor|:Square/gi, "")
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

function stripWaybackPrefix(u) {
  // https://web.archive.org/web/2024...im_/https://cache.marriott.com/...
  const m = String(u || "").match(
    /https?:\/\/web\.archive\.org\/web\/\d+(?:im_)?\/(https?:\/\/.+)$/i
  );
  return m ? m[1] : u;
}

function extractMarriott(html) {
  const decoded = decode(html);
  const fromCache = [
    ...decoded.matchAll(/(?:https:)?\/\/cache\.marriott\.com\/[^"'\\\s<>]+/gi),
  ]
    .map((m) => cleanUrl(stripWaybackPrefix(m[0].startsWith("//") ? `https:${m[0]}` : m[0])))
    .filter((u) => !isJunk(u))
    .filter((u) => /marriott-renditions|marriotts7prod/i.test(u))
    .filter((u) => !/getty/i.test(u));

  const fromBrandSite = [
    ...decoded.matchAll(
      /https:\/\/(?:marriott-hotels|sheraton|westin|springhillsuites|towneplacesuites|aloft-hotels)\.marriott\.com\/wp-content\/uploads\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
    ),
  ]
    .map((m) => cleanUrl(m[0]))
    .filter((u) => !isJunk(u));

  const fromDevGallery = [
    ...decoded.matchAll(
      /https:\/\/(?:www\.)?hotel-development\.marriott\.com\/resourcefiles\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
    ),
  ]
    .map((m) => cleanUrl(stripWaybackPrefix(m[0])))
    .filter((u) => !isJunk(u));

  // Scene7 asset ids embedded without full URL (Wayback / JSON blobs)
  const fromIds = [
    ...decoded.matchAll(/marriotts7prod\/(xf-(?!getty)[a-z0-9-]+)/gi),
  ].map(
    (m) =>
      `https://cache.marriott.com/is/image/marriotts7prod/${m[1]}:Wide-Hor?wid=1600&fit=constrain`
  );

  return dedupeNear([...fromCache, ...fromBrandSite, ...fromDevGallery, ...fromIds]);
}

function extractHotelLinks(html) {
  return [
    ...new Set(
      [...html.matchAll(/https:\/\/www\.marriott\.com\/en-us\/hotels\/([a-z0-9-]+)\/overview\/?/gi)].map(
        (m) => cleanUrl(m[0]).replace(/\/$/, "") + "/"
      )
    ),
  ];
}

function inferRole(imageUrl, sourcePageUrl) {
  const det = detectVisualCategory({ imageUrl, sourcePageUrl });
  if (det.category && det.category !== IMAGE_ROLES.unknown) return det.category;
  const u = imageUrl.toLowerCase();
  if (/exterior|aerial|arrival|entrance|front/i.test(u)) return IMAGE_ROLES.exterior_arrival;
  if (/lobby|public|lounge|bar|reception/i.test(u)) return IMAGE_ROLES.public_space_lobby;
  if (/guest.?room|suite|bedroom|king|queen/i.test(u)) return IMAGE_ROLES.guest_room_suite;
  if (/restaurant|dining|breakfast|fb|food|kitchen/i.test(u)) return IMAGE_ROLES.food_beverage_experience;
  if (/pool|spa|wellness|fitness/i.test(u)) return IMAGE_ROLES.wellness_pool_spa;
  return IMAGE_ROLES.property_setting;
}

async function fetchHtml(url) {
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
}

function propertyPagesForSlug(slug) {
  const pack = getWave14SourcePack(slug);
  const supplemental = getWave14SupplementalOpenings(slug);
  const out = [];
  const seen = new Set();
  for (const r of [...(pack?.propertyExamples || []), ...supplemental]) {
    if (!r?.url || !r?.propertyName) continue;
    if (!/\/hotels\/[a-z0-9-]+\/overview/i.test(r.url)) continue;
    const key = slugify(r.propertyName);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      propertyName: r.propertyName,
      url: r.url,
      marketCity: r.marketCity || (r.market || "").split(",")[0].trim(),
      geographyLabel: r.geographyLabel || "International Reference",
    });
  }
  return out;
}

function geographyFromHotelCode(code, fallback = "International Reference") {
  const c = String(code || "").toLowerCase();
  // crude CALA airport/city codes seen on brand pages
  if (/^(cun|mex|pvr|mid|cul|baq|sjo|sju|gdl|mty|bog|lim|scl|eze|gru)/i.test(c)) {
    return "CALA";
  }
  return fallback;
}

async function harvestBrand(slug) {
  const sibling = SIBLING_RE[slug] || /$a/;
  const pool = [];
  const probes = [];
  const push = (ex, imageUrl, label = "property") => {
    const u = cleanUrl(imageUrl);
    if (!u || isJunk(u)) return;
    if (sibling.test(u) && !/flex|studiores|marriott-cancun|marriott-reforma|marriott-culiacan|barranquilla-marriott/i.test(u)) {
      // allow when property name clearly matches brand
      if (!(ex.propertyName && sibling.test(ex.propertyName) === false)) {
        if (sibling.test(u)) return;
      }
    }
    if (sibling.test(u) && !new RegExp(slug.split("-")[0], "i").test(ex.propertyName || "")) {
      return;
    }
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

  // 1) Brand microsites
  for (const site of BRAND_MICROSITES[slug] || []) {
    const { status, html, finalUrl } = await fetchHtml(site);
    const imgs = extractMarriott(html);
    const hotelLinks = extractHotelLinks(html);
    probes.push({ pageUrl: finalUrl || site, status, count: imgs.length, hotelLinks: hotelLinks.length });
    const brandEx = {
      propertyName: `${getWave14SourcePack(slug)?.officialBrandName || slug} (brand photography)`,
      marketCity: "",
      geographyLabel: "International Reference",
      url: finalUrl || site,
    };
    for (const u of imgs.slice(0, 40)) push(brandEx, u, "brand_site");

    // Follow hotel links from brand site (up to 6)
    for (const hotelUrl of hotelLinks.slice(0, 6)) {
      const code = (hotelUrl.match(/\/hotels\/([a-z0-9-]+)\//i) || [])[1] || "";
      const nameGuess = code
        .replace(/^[a-z]{3,5}-/, "")
        .split("-")
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
        .join(" ");
      // Skip sibling hotel codes for Flex / StudioRes / etc.
      if (slug === "four-points-flex-by-sheraton" && !/flex|express/i.test(hotelUrl + nameGuess)) {
        continue;
      }
      if (slug === "studiores" && !/studiores/i.test(hotelUrl)) continue;
      if (slug === "springhill-suites-by-marriott" && !/springhill/i.test(hotelUrl)) continue;
      if (slug === "towneplace-suites-by-marriott" && !/towneplace/i.test(hotelUrl)) continue;
      if (slug === "residence-inn-by-marriott" && !/residence-inn/i.test(hotelUrl)) continue;
      if (slug === "aloft-hotels" && !/aloft/i.test(hotelUrl)) continue;
      if (slug === "sheraton" && !/sheraton/i.test(hotelUrl)) continue;
      if (slug === "westin" && !/westin/i.test(hotelUrl)) continue;
      if (slug === "marriott-hotels" && !/marriott/i.test(hotelUrl)) continue;
      if (slug === "marriott-hotels" && /jw-|sheraton|westin|residence|springhill|towneplace|aloft|courtyard|fairfield|autograph|tribute|renaissance/i.test(hotelUrl)) {
        continue;
      }

      const { status: st, html: h2 } = await fetchHtml(hotelUrl);
      const imgs2 = extractMarriott(h2);
      probes.push({ pageUrl: hotelUrl, status: st, count: imgs2.length });
      const ex = {
        propertyName: nameGuess || code,
        marketCity: "",
        geographyLabel: geographyFromHotelCode(code),
        url: hotelUrl,
      };
      for (const u of imgs2.slice(0, 18)) push(ex, u, "hotel_link");
    }
  }

  // 2) Source-pack + supplemental property overview pages
  for (const ex of propertyPagesForSlug(slug)) {
    const { status, html } = await fetchHtml(ex.url);
    const imgs = extractMarriott(html);
    probes.push({ pageUrl: ex.url, status, count: imgs.length });
    for (const u of imgs.slice(0, 20)) push(ex, u, "property");
  }

  // Dedupe pool by near-duplicate key
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
      : [...WAVE14_STAGE5_APPROVED_SLUGS];

  fs.mkdirSync(FIXTURES, { recursive: true });
  fs.mkdirSync(REPORTS, { recursive: true });
  const summary = { version: "wave14-image-harvest-v1", generatedAt: new Date().toISOString(), brands: [] };

  for (const slug of brands) {
    console.log(`[harvest-wave14] ${slug}`);
    const { pool, probes } = await harvestBrand(slug);
    const out = path.join(FIXTURES, `wave14-${slug}-gallery-pool.json`);
    fs.writeFileSync(out, `${JSON.stringify(pool, null, 2)}\n`);
    summary.brands.push({
      slug,
      fixture: out,
      poolCount: pool.length,
      probes,
    });
    console.log(`  wrote ${pool.length} rows → ${out}`);
  }

  const reportPath = path.join(REPORTS, "brand-explorer-wave14-image-harvest.json");
  fs.writeFileSync(reportPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(`Wrote ${reportPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
