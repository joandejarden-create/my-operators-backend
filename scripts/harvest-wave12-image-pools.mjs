#!/usr/bin/env node
/**
 * Wave 12 — harvest official property image candidates into fixture pools.
 *
 * Sources by family:
 * - IHG (voco/avid/HIE): digital.ihg.com property-prefixed assets from hoteldetail
 * - EVEN: development.ihg.com EVEN property photography (hoteldetail is stock-only)
 * - Marriott: cache.marriott.com from overview + photos pages
 * - Hilton: stories.hilton.com / stories-editor + Wayback hilton.com/im when available
 * - Bunkhouse: bunkhousehotels.com property microsites (login CDN)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";
import { getWave12SourcePack } from "../lib/partner-intelligence/brand-explorer-wave12-source-packs-content.js";
import { getWave12TabFactorySeed } from "../lib/partner-intelligence/brand-explorer-wave12-tab-factory-seeds.js";
import { inferHiltonCanopyPropertyFromUrl } from "../lib/partner-intelligence/brand-explorer-gallery-selection.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS = path.join(ROOT, "reports");

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

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

function absUrl(u, base) {
  const c = cleanUrl(u);
  if (!c) return "";
  if (c.startsWith("http")) return c;
  try {
    return new URL(c, base).toString();
  } catch {
    return "";
  }
}

function isJunk(u) {
  return /logo|chiclet|primary_logo|nav-logo|app-web|favicon|sprite|icon|badge|pixel|1x1|stays\?|fmt=png-alpha|open-graph-home|brand-refresh|hilton_black|404-image|default\.png|wp-includes|michelinkey|whale|robe-1|screenshot-2023|canopy-1\.png$/i.test(
    u
  );
}

function familyOf(slug) {
  if (slug === "even-hotels") return "even";
  if (/voco|avid|holiday-inn/.test(slug)) return "ihg";
  if (/courtyard|ac-hotels|city-express|moxy/.test(slug)) return "marriott";
  if (/canopy|motto|tempo/.test(slug)) return "hilton";
  if (/bunkhouse/.test(slug)) return "bunkhouse";
  return "other";
}

function brandPrefixRe(slug) {
  const map = {
    "even-hotels": /even/i,
    "voco-hotels": /voco/i,
    "avid-hotels": /avid/i,
    "holiday-inn-express": /holiday-inn-express|hiex/i,
    "courtyard-by-marriott": /courtyard|cy-/i,
    "ac-hotels-by-marriott": /ac-hotel|\/ac-|gdlac|ox-|ac-/i,
    "city-express-by-marriott": /city-express|cxp|cyx/i,
    "moxy-hotels": /moxy|ox-|tqoox|atldx/i,
    "canopy-by-hilton": /canopy/i,
    "motto-by-hilton": /motto/i,
    "tempo-by-hilton": /tempo/i,
    "bunkhouse-hotels": /bunkhouse|saint-cecilia|san-cristobal|san-fernando|nick_simonite|hscb|hsf_/i,
  };
  return map[slug] || /./;
}

function siblingRejectRe(slug) {
  const map = {
    "even-hotels": /voco|avid|hotel-indigo|holiday-inn(?!-express)|kimpton|vignette|intercontinental|iberostar/i,
    "voco-hotels": /hotel-indigo|kimpton|vignette|holiday-inn|avid|even/i,
    "avid-hotels": /holiday-inn|even|voco|hotel-indigo/i,
    "holiday-inn-express": /holiday-inn(?!-express)|avid|even|voco|club-vacations/i,
    "courtyard-by-marriott": /ac-hotel|moxy|city-express|autograph|tribute|w-hotel|st-regis/i,
    "ac-hotels-by-marriott": /moxy|autograph|tribute|courtyard|city-express|aloft/i,
    "city-express-by-marriott": /courtyard|ac-hotel|moxy|autograph|fairfield|residence-inn/i,
    "moxy-hotels": /ac-hotel|autograph|tribute|aloft|courtyard|city-express/i,
    "canopy-by-hilton": /curio|tapestry|tempo|motto|waldorf|conrad|hilton-garden/i,
    "motto-by-hilton": /canopy|tempo|curio|tapestry|conrad|waldorf/i,
    "tempo-by-hilton": /canopy|motto|curio|tapestry|hilton-garden|conrad/i,
    "bunkhouse-hotels": /hyatt-regency|park-hyatt|andaz|thompson|unbound|caption/i,
  };
  return map[slug] || /$a/;
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
  const html = decode(await res.text());
  return { status: res.status, html, finalUrl: res.url };
}

function stripSizeSuffix(u) {
  return String(u || "")
    .replace(/-\d{2,4}x\d{2,4}(?=\.(?:jpg|jpeg|png|webp))/i, "")
    .replace(/\?.*$/, "")
    // Collapse stories.hilton.com/uploads ↔ stories-editor.hilton.com/wp-content/uploads duplicates
    .replace(
      /^https:\/\/stories\.hilton\.com\/uploads\//i,
      "https://stories-editor.hilton.com/wp-content/uploads/"
    )
    .toLowerCase();
}

function dedupeNear(urls) {
  const seen = new Set();
  const out = [];
  for (const u of urls) {
    const key = stripSizeSuffix(u).toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(u);
  }
  return out;
}

function extractIhgPropertyImages(html, slug) {
  const ids = [
    ...html.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/([^"'\\\s<>?]+)/gi),
  ].map((m) => m[1]);
  const prefix = brandPrefixRe(slug);
  const sibling = siblingRejectRe(slug);
  return [
    ...new Set(
      ids
        .filter((id) => prefix.test(id))
        .filter((id) => !sibling.test(id))
        .filter((id) => !/getty|istock|family-at|snorkeling|maldives|stays|chiclet|learning|portal|logo/i.test(id))
        .map((id) => `https://digital.ihg.com/is/image/ihg/${id}`)
    ),
  ];
}

function extractMarriott(html) {
  return [
    ...new Set(
      [...html.matchAll(/https:\/\/cache\.marriott\.com\/[^"'\\\s<>]+/gi)]
        .map((m) => cleanUrl(m[0]))
        .filter((u) => !isJunk(u))
        .filter((u) => /marriott-renditions|marriotts7prod/i.test(u))
    ),
  ];
}

function extractHiltonStories(html, slug) {
  const brand = brandPrefixRe(slug);
  const sibling = siblingRejectRe(slug);
  const out = [];
  for (const m of html.matchAll(
    /https:\/\/(?:stories-editor\.hilton\.com|stories\.hilton\.com)\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi
  )) {
    const u = cleanUrl(m[0]).replace(/&amp;/g, "&");
    if (isJunk(u)) continue;
    if (!brand.test(u)) continue;
    if (sibling.test(u)) continue;
    // Prefer full-size (drop tiny crop query variants)
    const bare = u.replace(/\?.*$/, "");
    out.push(bare);
  }
  for (const m of html.matchAll(
    /https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi
  )) {
    const u = cleanUrl(m[0]);
    if (!isJunk(u)) out.push(u);
  }
  return dedupeNear(out);
}

function extractBunkhouse(html, pageUrl) {
  const out = [];
  for (const m of html.matchAll(
    /(?:https:\/\/login\.bunkhousehotels\.com|\/wp-content\/uploads)\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi
  )) {
    let u = absUrl(m[0], pageUrl);
    if (!u || isJunk(u)) continue;
    if (/-\d{2,4}x\d{2,4}\./i.test(u)) continue; // skip resized variants
    out.push(u);
  }
  return dedupeNear(out);
}

function extractEvenDev(html, base) {
  const out = [];
  for (const m of html.matchAll(
    /(?:src|content)=["']([^"']*(?:even-hotel|EVEN_|EVEN-)[^"']+\.(?:jpg|jpeg|png|webp))["']/gi
  )) {
    const u = absUrl(m[1], base);
    if (!u || isJunk(u) || /logo/i.test(u)) continue;
    out.push(u);
  }
  // posters in escaped JSON
  for (const m of html.matchAll(
    /https:\\\/\\\/development\.ihg\.com\\\/sites\\\/ihgplc\\\/files\\\/[^"'\\\s<>]+EVEN[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
  )) {
    out.push(cleanUrl(m[0].replace(/\\\//g, "/")));
  }
  return dedupeNear(out);
}

async function urlExists(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": UA, Range: "bytes=0-128", Accept: "*/*" },
      redirect: "follow",
    });
    if (!(res.ok || res.status === 206)) return false;
    const ct = res.headers.get("content-type") || "";
    if (/html|xml|json|text\/plain/i.test(ct) && !/image/i.test(ct)) {
      const buf = Buffer.from(await res.arrayBuffer());
      if (/Problem parsing|Unable to find|Access Denied|<!DOCTYPE/i.test(buf.toString("utf8"))) {
        return false;
      }
    }
    return true;
  } catch {
    return false;
  }
}

async function cdxSnapshots(pageUrl, limit = 8) {
  try {
    const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
      pageUrl
    )}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=${limit}`;
    const r = await fetch(api, {
      headers: { "User-Agent": UA, Accept: "application/json" },
    });
    const t = await r.text();
    if (!t.startsWith("[")) return [];
    return JSON.parse(t).slice(1);
  } catch {
    return [];
  }
}

function propertyPagesForSlug(slug) {
  const pack = getWave12SourcePack(slug);
  const seed = getWave12TabFactorySeed(slug);
  const examples = [
    ...(pack?.propertyExamples || []),
    ...(seed?.supplementalOpenings || []).map((s) => ({
      propertyName: s.propertyName,
      url: s.url,
      market: s.market,
      marketCity: s.marketCity,
      geographyLabel: s.geographyLabel,
      matchKey: s.propertyName,
    })),
  ];

  // Correct Bunkhouse microsite paths (source pack used legacy /hotels/ URLs)
  if (slug === "bunkhouse-hotels") {
    for (const ex of examples) {
      if (/hotel-san-cristobal/i.test(ex.url || "")) {
        ex.url = "https://www.bunkhousehotels.com/hotel-san-cristobal";
      } else if (/hotel-san-fernando/i.test(ex.url || "")) {
        ex.url = "https://www.bunkhousehotels.com/hotel-san-fernando";
      } else if (/hotel-saint-cecilia/i.test(ex.url || "")) {
        ex.url = "https://www.bunkhousehotels.com/hotel-saint-cecilia";
      }
    }
  }

  const seen = new Set();
  const out = [];
  for (const ex of examples) {
    const key = String(ex.matchKey || ex.propertyName || "").toLowerCase();
    if (!key || seen.has(key) || !ex.url) continue;
    seen.add(key);
    out.push(ex);
  }
  return out;
}

function hiltonStoryPages(slug) {
  if (slug === "canopy-by-hilton") {
    return [
      "https://stories.hilton.com/brands/canopy",
      "https://stories.hilton.com/canopy-by-hilton-fact-sheet",
      "https://stories.hilton.com/releases/canopy-by-hilton-announces-san-francisco-debut",
      "https://web.archive.org/web/20251006112516/https://stories.hilton.com/releases/canopy-by-hilton-announces-san-francisco-debut",
    ];
  }
  if (slug === "motto-by-hilton") {
    return [
      "https://stories.hilton.com/brands/motto",
      "https://stories.hilton.com/releases/motto-by-hilton-debuts-in-mexico-this-fall-with-the-opening-of-motto-by-hilton-tulum",
      "https://stories.hilton.com/releases/motto-by-hilton-makes-international-debut-mexico-netherlands",
    ];
  }
  if (slug === "tempo-by-hilton") {
    return [
      "https://stories.hilton.com/brands/tempo",
      "https://stories.hilton.com/releases/hilton-lifestyle-brand-lands-in-music-city-tempo-by-hilton-nashville-downtown-is-now-open",
      "https://stories.hilton.com/releases/hilton-launches-elevated-approachable-lifestyle-brand-with-tempo-by-hilton",
    ];
  }
  return [];
}

async function expandHiltonFromCdx(slug) {
  const prefix =
    slug === "canopy-by-hilton"
      ? "stories.hilton.com/releases/canopy"
      : slug === "motto-by-hilton"
        ? "stories.hilton.com/releases/motto"
        : slug === "tempo-by-hilton"
          ? "stories.hilton.com/releases/tempo"
          : null;
  if (!prefix) return [];
  const rows = await cdxSnapshots(`https://${prefix}`, 15);
  // cdxSnapshots expects full URL — use prefix via custom call
  try {
    const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
      prefix
    )}&matchType=prefix&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=25`;
    const r = await fetch(api, {
      headers: { "User-Agent": UA, Accept: "application/json" },
    });
    const t = await r.text();
    if (!t.startsWith("[")) return [];
    const j = JSON.parse(t).slice(1);
    const uniqueOrig = [];
    const seen = new Set();
    for (const row of j) {
      const orig = row[1];
      if (seen.has(orig)) continue;
      seen.add(orig);
      uniqueOrig.push(row);
      if (uniqueOrig.length >= 8) break;
    }
    const out = [];
    for (const row of uniqueOrig) {
      const wa = `https://web.archive.org/web/${row[0]}/${row[1]}`;
      const { html } = await fetchHtml(wa);
      out.push(...extractHiltonStories(html, slug));
    }
    return dedupeNear(out);
  } catch {
    return [];
  }
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function harvestBrand(slug) {
  const family = familyOf(slug);
  const pages = propertyPagesForSlug(slug);
  const pool = [];
  const probes = [];
  const brandRe = brandPrefixRe(slug);
  const sibling = siblingRejectRe(slug);

  const pushPool = (ex, imageUrl, label = "property") => {
    const u = cleanUrl(imageUrl);
    if (!u || isJunk(u)) return;
    if (sibling.test(u) && !brandRe.test(u)) return;
    pool.push({
      propertyKey: slugify(ex.propertyName || ex.matchKey || "brand"),
      propertyName: ex.propertyName || "",
      marketCity: (ex.marketCity || ex.market || "").split(",")[0].trim(),
      geographyLabel: ex.geographyLabel || "International Reference",
      sourcePageUrl: ex.url || ex.sourcePageUrl || "",
      imageUrl: u,
      label,
    });
  };

  if (family === "even") {
    const { status, html, finalUrl } = await fetchHtml(
      "https://development.ihg.com/hotel-brands/even-hotels"
    );
    probes.push({
      propertyName: "EVEN Hotels (IHG Development)",
      pageUrl: finalUrl,
      status,
      count: 0,
    });
    const imgs = extractEvenDev(html, finalUrl);
    probes[0].count = imgs.length;
    // Map known property alts to openings when possible
    const fallbackEx = pages[0] || {
      propertyName: "EVEN Hotel New York - Times Square South",
      marketCity: "New York",
      geographyLabel: "International Reference",
      url: "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail",
    };
    for (const u of imgs) {
      let ex = fallbackEx;
      if (/MIASW|Miami|Doral/i.test(u)) {
        ex =
          pages.find((p) => /miami/i.test(p.propertyName)) || {
            propertyName: "EVEN Hotels Miami - Doral Area",
            marketCity: "Miami",
            geographyLabel: "International Reference",
            url: "https://www.ihg.com/evenhotels/hotels/us/en/sweetwater/miasw/hoteldetail",
          };
      } else if (/Orlando/i.test(u)) {
        ex = {
          propertyName: "EVEN Hotel Orlando International Airport",
          marketCity: "Orlando",
          geographyLabel: "International Reference",
          url: "https://www.ihg.com/evenhotels/hotels/us/en/orlando/orlmr/hoteldetail",
        };
      } else if (/Long.?Island|nycis/i.test(u)) {
        ex = {
          propertyName: "EVEN Hotel Long Island City - New York",
          marketCity: "New York",
          geographyLabel: "International Reference",
          url: "https://www.ihg.com/evenhotels/hotels/us/en/long-island-city/nycis/hoteldetail",
        };
      } else if (/Bozeman|BZNBD/i.test(u)) {
        ex = {
          propertyName: "EVEN Hotel Bozeman - Yellowstone Airport",
          marketCity: "Bozeman",
          geographyLabel: "International Reference",
          url: "https://development.ihg.com/hotel-brands/even-hotels",
        };
      } else if (/Austin|Lobby\.jpg/i.test(u)) {
        ex = {
          propertyName: "EVEN Hotel Austin Uptown near the Domain",
          marketCity: "Austin",
          geographyLabel: "International Reference",
          url: "https://development.ihg.com/hotel-brands/even-hotels",
        };
      }
      pushPool(ex, u);
    }
  }

  for (const ex of pages) {
    if (family === "even") break; // already handled
    const urlsToTry = [ex.url];
    if (family === "marriott" && /\/overview\/?$/i.test(ex.url)) {
      urlsToTry.push(ex.url.replace(/\/overview\/?$/i, "/photos/"));
    }
    if (family === "hilton") {
      urlsToTry.push(ex.url.endsWith("/") ? ex.url : `${ex.url}/`);
    }

    let candidates = [];
    let status = 0;
    for (const pageUrl of urlsToTry) {
      const { status: st, html, finalUrl } = await fetchHtml(pageUrl);
      status = st;
      if (family === "ihg") candidates.push(...extractIhgPropertyImages(html, slug));
      if (family === "marriott") candidates.push(...extractMarriott(html));
      if (family === "hilton") candidates.push(...extractHiltonStories(html, slug));
      if (family === "bunkhouse") candidates.push(...extractBunkhouse(html, finalUrl));
    }

    // Hilton: Wayback fallback for hilton.com/im assets
    if (family === "hilton" && candidates.length < 6) {
      const snaps = await cdxSnapshots(ex.url, 6);
      for (const row of snaps.slice(0, 3)) {
        const wa = `https://web.archive.org/web/${row[0]}/${row[1]}`;
        const { html } = await fetchHtml(wa);
        candidates.push(...extractHiltonStories(html, slug));
      }
    }

    candidates = dedupeNear(candidates).slice(0, 24);
    probes.push({
      propertyName: ex.propertyName,
      pageUrl: ex.url,
      status,
      count: candidates.length,
    });
    for (const imageUrl of candidates) pushPool(ex, imageUrl);
  }

  // Hilton brand story pages (rich official photography)
  if (family === "hilton") {
    for (const storyUrl of hiltonStoryPages(slug)) {
      const { status, html } = await fetchHtml(storyUrl);
      const imgs = extractHiltonStories(html, slug);
      probes.push({
        propertyName: `Stories — ${slug}`,
        pageUrl: storyUrl,
        status,
        count: imgs.length,
      });
      const fallback = pages[0] || {
        propertyName: slug,
        marketCity: "",
        geographyLabel: "International Reference",
        url: storyUrl,
      };
      for (const u of imgs) {
        let ex = null;
        const lower = u.toLowerCase();
        for (const p of pages) {
          const tokens = String(p.propertyName || "")
            .toLowerCase()
            .split(/[^a-z0-9]+/)
            .filter((t) => t.length > 3);
          if (tokens.some((t) => lower.includes(t))) {
            ex = p;
            break;
          }
        }
        // Prefer URL-stem inference over dumping unmatched stories onto pages[0]
        if (!ex && slug === "canopy-by-hilton") {
          const inferred = inferHiltonCanopyPropertyFromUrl(u);
          if (inferred) {
            ex = {
              propertyName: inferred.propertyName,
              marketCity: inferred.marketCity,
              geographyLabel: inferred.geographyLabel,
              propertyKey: inferred.propertyKey,
              url: storyUrl,
            };
          }
        }
        if (!ex) {
          // Keep unmatched Hilton story images only with generic brand attribution
          ex = {
            propertyName: `Canopy by Hilton (brand photography)`,
            marketCity: "",
            geographyLabel: "International Reference",
            url: storyUrl,
          };
        }
        pushPool({ ...ex, url: storyUrl }, u, "stories");
      }
    }
    if (pool.length < 18) {
      const extra = await expandHiltonFromCdx(slug);
      probes.push({
        propertyName: `CDX stories expand — ${slug}`,
        pageUrl: "web.archive.org/cdx",
        status: 200,
        count: extra.length,
      });
      const fallback = pages[0] || {
        propertyName: slug,
        marketCity: "",
        geographyLabel: "International Reference",
        url: "https://stories.hilton.com/",
      };
      for (const u of extra) pushPool(fallback, u, "stories-cdx");
    }
  }

  // Optional HEAD filter for small pools / bunkhouse / even
  if (["even", "hilton", "bunkhouse"].includes(family) || pool.length < 18) {
    const verified = [];
    const seen = new Set();
    for (const row of pool) {
      const key = stripSizeSuffix(row.imageUrl).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      if (await urlExists(row.imageUrl)) verified.push(row);
      if (verified.length >= 36) break;
    }
    pool.length = 0;
    pool.push(...verified);
  } else {
    // Soft dedupe without HEAD for large IHG/Marriott pools
    const seen = new Set();
    const deduped = [];
    for (const row of pool) {
      const key = stripSizeSuffix(row.imageUrl).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(row);
      if (deduped.length >= 36) break;
    }
    pool.length = 0;
    pool.push(...deduped);
  }

  const fixturePath = path.join(FIXTURES, `wave12-${slug}-gallery-pool.json`);
  fs.writeFileSync(fixturePath, `${JSON.stringify(pool, null, 2)}\n`, "utf8");

  return {
    slug,
    family,
    poolCount: pool.length,
    distinctUrls: new Set(pool.map((p) => p.imageUrl)).size,
    properties: new Set(pool.map((p) => p.propertyKey)).size,
    fixturePath: path.relative(ROOT, fixturePath).replace(/\\/g, "/"),
    probes,
  };
}

async function main() {
  fs.mkdirSync(FIXTURES, { recursive: true });
  fs.mkdirSync(REPORTS, { recursive: true });
  const only = process.argv.includes("--only")
    ? process.argv[process.argv.indexOf("--only") + 1]?.split(",").map((s) => s.trim()).filter(Boolean)
    : null;
  const brands = [];
  for (const slug of WAVE12_SLUGS) {
    if (only && !only.includes(slug)) continue;
    console.log(`[wave12-harvest] ${slug}`);
    const result = await harvestBrand(slug);
    console.log(`  pool=${result.poolCount} distinct=${result.distinctUrls}`);
    brands.push(result);
  }
  const report = {
    generatedAt: new Date().toISOString(),
    brands,
  };
  const out = path.join(REPORTS, "brand-explorer-wave12-image-harvest.json");
  fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log("Wrote", out);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
