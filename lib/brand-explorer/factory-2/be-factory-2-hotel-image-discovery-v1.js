/**
 * Factory 2.0 — automated same-brand hotel image discovery ($0 paid).
 *
 * Discovers authentic hotel photographs from:
 * 1) Existing same-candidate blocks (gallery / openings / scenarios)
 * 2) Durable discovered pools under data/brand-explorer/factory-2-0/image-pools/
 * 3) Official brand / hotel pages (og:image + twitter:image) via free HTTP
 *
 * Never invents images. Never substitutes sibling-brand photography.
 * Soft-blocks certification when verified imagery cannot be obtained.
 *
 * Contract: BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import {
  isPeopleOrLifestyleMarketingImage,
  isLikelyHotelPropertyPhotograph,
} from "../be-permanent-six-image-gallery-standard-v1.js";
import { isSameBrandImage } from "../be-permanent-same-brand-image-affinity-v1.js";
import {
  ACTIVE_BRAND_WEBSITE_CORRECTIONS,
  isParentCompanyHomepage,
  normalizeBrandWebsiteUrl,
  brandWebsiteDisplayLabel,
} from "../active-brand-website-corrections.js";

export const BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1 =
  "BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1";

const UA =
  "DealalityBrandExplorerBot/1.0 (+https://dealality.com; factory image discovery; $0 research)";
const FETCH_TIMEOUT_MS = 12000;
const MAX_PROPERTY_PAGES = 10;
const ROLE_CYCLE = [
  "Exterior",
  "Guest Room",
  "Lobby / Public Space",
  "F&B",
  "Pool / Wellness",
  "Arrival",
];

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function imagePoolPath(repoRoot, slug) {
  return join(repoRoot, "data/brand-explorer/factory-2-0/image-pools", `${slug}.json`);
}

export function loadDiscoveredImagePool(repoRoot, slug) {
  const abs = imagePoolPath(repoRoot, slug);
  if (!existsSync(abs)) return null;
  try {
    return JSON.parse(readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

export function saveDiscoveredImagePool(repoRoot, slug, pool) {
  const dir = join(repoRoot, "data/brand-explorer/factory-2-0/image-pools");
  mkdirSync(dir, { recursive: true });
  writeFileSync(imagePoolPath(repoRoot, slug), JSON.stringify(pool, null, 2) + "\n");
}

function decodeMeta(v) {
  return nz(v)
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function metaContent(html, property) {
  const re1 = new RegExp(
    `<meta[^>]+property=["']${property}["'][^>]+content=["']([^"']+)["']`,
    "i"
  );
  const re2 = new RegExp(
    `<meta[^>]+content=["']([^"']+)["'][^>]+property=["']${property}["']`,
    "i"
  );
  const re3 = new RegExp(
    `<meta[^>]+name=["']${property}["'][^>]+content=["']([^"']+)["']`,
    "i"
  );
  const m = html.match(re1) || html.match(re2) || html.match(re3);
  return m ? decodeMeta(m[1]) : "";
}

function absolutize(href, baseUrl) {
  try {
    return new URL(href, baseUrl).toString();
  } catch {
    return "";
  }
}

export async function fetchText(url, opts = {}) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), opts.timeoutMs || FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": UA,
        Accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
      },
    });
    if (!res.ok) return { ok: false, status: res.status, text: "", finalUrl: url };
    const text = await res.text();
    return { ok: true, status: res.status, text, finalUrl: res.url || url };
  } catch (err) {
    return { ok: false, status: 0, text: "", error: String(err?.message || err), finalUrl: url };
  } finally {
    clearTimeout(t);
  }
}

export async function urlLooksReachable(url) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      signal: controller.signal,
      headers: { "User-Agent": UA },
    });
    if (res.ok) return true;
    // Some CDNs reject HEAD — try GET range
    if (res.status === 405 || res.status === 403) {
      const g = await fetch(url, {
        method: "GET",
        redirect: "follow",
        signal: controller.signal,
        headers: { "User-Agent": UA, Range: "bytes=0-0" },
      });
      return g.ok || g.status === 206;
    }
    return false;
  } catch {
    return false;
  } finally {
    clearTimeout(t);
  }
}

function acceptImage(slug, brandName, imageUrl, meta = {}) {
  if (!/^https?:\/\//i.test(nz(imageUrl))) return false;
  if (/logo|favicon|wordmark|sprite|1x1|pixel|placeholder/i.test(imageUrl)) return false;
  if (isPeopleOrLifestyleMarketingImage(imageUrl, meta)) return false;
  // Prefer hotel cues; allow CDN urls with opaque paths when brand affinity passes
  // and role is explicitly hotel_property.
  const hotelish =
    isLikelyHotelPropertyPhotograph(imageUrl, { ...meta, role: meta.role || "hotel_property" }) ||
    /airtableusercontent|cloudinary|scene7|akamai|imgix|hyatt\.com\/.*\/media|hilton\.com\/im\//i.test(
      imageUrl
    );
  if (!hotelish) return false;
  if (!isSameBrandImage(slug, imageUrl, { ...meta, title: brandName })) {
    // Soft-pass when URL host clearly matches brand website domain tokens
    const brandTok = nz(brandName)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 3)
      .slice(0, 3);
    const hay = `${imageUrl} ${meta.sourcePageUrl || ""} ${meta.propertyName || ""}`.toLowerCase();
    if (!brandTok.some((t) => hay.includes(t))) return false;
  }
  return true;
}

function extractImgSrcCandidates(html, pageUrl) {
  const out = [];
  const re = /<img[^>]+src=["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html))) {
    const abs = absolutize(m[1], pageUrl) || m[1];
    if (!/^https?:\/\//i.test(abs)) continue;
    if (/\.svg(\?|$)/i.test(abs)) continue;
    if (/logo|favicon|sprite|icon|badge|appstore|play-store/i.test(abs)) continue;
    if (
      !/(marriott|hilton|hyatt|ihg|accor|wyndham|choice|cloudinary|scene7|akamai|imgix|shangri-la|dusit|melia|barcelo|minor|rotana|panpacific|fairmont|sofitel|raffles)/i.test(
        abs
      )
    ) {
      continue;
    }
    out.push({ url: abs, source: "img-src", pageUrl });
    if (out.length >= 24) break;
  }
  return out;
}

function extractOgCandidates(html, pageUrl) {
  const out = [];
  for (const prop of ["og:image", "og:image:url", "twitter:image", "twitter:image:src"]) {
    const u = metaContent(html, prop);
    if (u) out.push({ url: absolutize(u, pageUrl) || u, source: prop, pageUrl });
  }
  // JSON-LD image fields
  const ldBlocks = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const block of ldBlocks) {
    const raw = block.replace(/^[\s\S]*?>/, "").replace(/<\/script>$/i, "");
    try {
      const j = JSON.parse(raw);
      const stack = Array.isArray(j) ? j : [j];
      for (const node of stack) {
        const img = node?.image || node?.photo || node?.thumbnailUrl;
        const urls = Array.isArray(img) ? img : img ? [img] : [];
        for (const item of urls) {
          const u = typeof item === "string" ? item : item?.url;
          if (u) out.push({ url: absolutize(u, pageUrl) || u, source: "json-ld", pageUrl });
        }
      }
    } catch {
      /* ignore */
    }
  }
  out.push(...extractImgSrcCandidates(html, pageUrl));
  return out;
}

function extractHotelPageLinks(html, pageUrl, brandName) {
  const links = new Set();
  const re = /href=["']([^"']+)["']/gi;
  let m;
  const brandBits = nz(brandName)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 3)
    .slice(0, 2);
  while ((m = re.exec(html))) {
    const abs = absolutize(m[1], pageUrl);
    if (!abs || !/^https?:\/\//i.test(abs)) continue;
    if (/#|javascript:|mailto:|tel:/i.test(abs)) continue;
    const path = abs.toLowerCase();
    const hotelish =
      /\/hotels?\//i.test(path) ||
      /\/hotel\//i.test(path) ||
      /\/destinations?\//i.test(path) ||
      /\/resorts?\//i.test(path) ||
      /\/properties?\//i.test(path) ||
      /\/en\/hotels\//i.test(path) ||
      /\/en-us\/hotels\//i.test(path) ||
      /choicehotels\.com\/.+\//i.test(path) ||
      /all\.accor\.com\/hotel\//i.test(path);
    if (!hotelish) continue;
    if (/careers|investors|press|newsroom|privacy|terms|login|account/i.test(path)) continue;
    // Prefer links that mention brand tokens when present
    if (brandBits.length && !brandBits.some((b) => path.includes(b))) {
      // still allow generic hotel paths on the brand's own host
      try {
        const baseHost = new URL(pageUrl).hostname.replace(/^www\./, "");
        const linkHost = new URL(abs).hostname.replace(/^www\./, "");
        if (linkHost !== baseHost && !linkHost.endsWith(baseHost) && !baseHost.endsWith(linkHost)) {
          continue;
        }
      } catch {
        continue;
      }
    }
    links.add(abs.split("#")[0]);
    if (links.size >= MAX_PROPERTY_PAGES * 3) break;
  }
  return [...links].slice(0, MAX_PROPERTY_PAGES);
}

export function harvestImagesFromBlocks(blocks, slug, brandName) {
  const cards = [];
  const seen = new Set();
  for (const b of blocks || []) {
    const imageUrl = nz(b.imageUrl);
    if (!/^https?:\/\//i.test(imageUrl)) continue;
    const key = imageUrl.replace(/[?#].*$/, "");
    if (seen.has(key)) continue;
    const meta = {
      title: b.title,
      caption: b.body,
      propertyName: b.title,
      sourcePageUrl: b.source || b.summaryUrl,
      role: "hotel_property",
    };
    if (!acceptImage(slug, brandName, imageUrl, meta)) continue;
    seen.add(key);
    cards.push({
      imageUrl,
      title: nz(b.title) || `${brandName} · Hotel Photography`,
      body: nz(b.body) || "Verified hotel property photography",
      role: ROLE_CYCLE[cards.length % ROLE_CYCLE.length],
      propertyName: nz(b.title) || brandName,
      sourcePageUrl: meta.sourcePageUrl || null,
      source: "candidate-block-harvest",
    });
  }
  return cards;
}

export function resolveOfficialBrandWebsite({ brandName, sources = {}, airtableWebsite = "" } = {}) {
  const mapped = ACTIVE_BRAND_WEBSITE_CORRECTIONS[brandName];
  const candidates = [
    mapped,
    sources.brandPage,
    sources.website,
    airtableWebsite,
  ]
    .map((u) => normalizeBrandWebsiteUrl(u || ""))
    .filter(Boolean);
  for (const url of candidates) {
    if (!isParentCompanyHomepage(url, "", brandName)) {
      return {
        url,
        displayLabel: brandWebsiteDisplayLabel(url),
        parentHomepage: false,
        source: mapped === url || normalizeBrandWebsiteUrl(mapped) === url ? "corrections-map" : "pack-or-airtable",
      };
    }
  }
  // Last resort: still return first candidate but mark parent homepage
  if (candidates[0]) {
    return {
      url: candidates[0],
      displayLabel: brandWebsiteDisplayLabel(candidates[0]),
      parentHomepage: isParentCompanyHomepage(candidates[0], "", brandName),
      source: "fallback-parent-or-unknown",
    };
  }
  return { url: "", displayLabel: "", parentHomepage: true, source: "missing" };
}

/**
 * Discover up to `need` hotel images for a brand. $0 paid — free HTTP only.
 */
export async function discoverBrandHotelImages(repoRoot, brand, opts = {}) {
  const slug = nz(brand.slug);
  const brandName = nz(brand.brandName) || slug;
  const need = opts.need || 6;
  const cards = [];
  const seen = new Set();
  const evidence = [];
  const push = (card, via) => {
    const key = nz(card.imageUrl).replace(/[?#].*$/, "");
    if (!key || seen.has(key)) return;
    if (!acceptImage(slug, brandName, card.imageUrl, card)) return;
    seen.add(key);
    cards.push({ ...card, discoveryVia: via });
    evidence.push({ via, imageUrl: card.imageUrl, sourcePageUrl: card.sourcePageUrl || null });
  };

  // 1) Existing durable pool
  const existing = loadDiscoveredImagePool(repoRoot, slug);
  if (existing?.images?.length) {
    for (const img of existing.images) push(img, "durable-pool");
  }

  // 2) Harvest from candidate blocks
  for (const img of harvestImagesFromBlocks(brand.blocks || [], slug, brandName)) {
    push(img, "block-harvest");
  }

  // 3) Network discovery from official brand / seed pages
  const website = resolveOfficialBrandWebsite({
    brandName,
    sources: brand.sources || {},
    airtableWebsite: brand.airtableWebsite,
  });
  const seedPages = [
    website.url,
    ...(brand.seedPropertyUrls || []),
    ...((brand.sources && [brand.sources.brandPage, brand.sources.news]) || []),
  ]
    .map((u) => normalizeBrandWebsiteUrl(u || ""))
    .filter(Boolean);
  const uniqueSeeds = [...new Set(seedPages)];

  // Also try common brand-page variants (www / non-www / trailing slash)
  const expandedSeeds = [];
  for (const seed of uniqueSeeds) {
    expandedSeeds.push(seed);
    try {
      const u = new URL(seed);
      if (!u.hostname.startsWith("www.")) {
        const www = new URL(seed);
        www.hostname = `www.${u.hostname}`;
        expandedSeeds.push(www.toString());
      }
    } catch {
      /* ignore */
    }
  }

  for (const seed of [...new Set(expandedSeeds)]) {
    if (cards.length >= need) break;
    if (isParentCompanyHomepage(seed, "", brandName)) {
      // Skip bare parent roots — never harvest parent marketing as brand photography
      continue;
    }
    const page = await fetchText(seed);
    evidence.push({ via: "fetch-seed", url: seed, ok: page.ok, status: page.status });
    if (!page.ok) continue;

    for (const c of extractOgCandidates(page.text, page.finalUrl || seed)) {
      if (cards.length >= need) break;
      const reachable = opts.skipHeadCheck ? true : await urlLooksReachable(c.url);
      if (!reachable) continue;
      push(
        {
          imageUrl: c.url,
          title: `${brandName} · Hotel Photography | International Reference`,
          body: `Official page imagery from ${c.source} · verify property identity before customer publish`,
          role: ROLE_CYCLE[cards.length % ROLE_CYCLE.length],
          propertyName: brandName,
          sourcePageUrl: c.pageUrl,
          source: `factory-2-0-image-discovery:${c.source}`,
        },
        "seed-og"
      );
    }

    const hotelLinks = extractHotelPageLinks(page.text, page.finalUrl || seed, brandName);
    for (const hotelUrl of hotelLinks) {
      if (cards.length >= need) break;
      const hp = await fetchText(hotelUrl);
      evidence.push({ via: "fetch-hotel", url: hotelUrl, ok: hp.ok, status: hp.status });
      if (!hp.ok) continue;
      for (const c of extractOgCandidates(hp.text, hp.finalUrl || hotelUrl)) {
        if (cards.length >= need) break;
        const reachable = opts.skipHeadCheck ? true : await urlLooksReachable(c.url);
        if (!reachable) continue;
        let propertyName = brandName;
        const title = metaContent(hp.text, "og:title") || metaContent(hp.text, "twitter:title");
        if (title) propertyName = title.split("|")[0].split("–")[0].trim() || brandName;
        push(
          {
            imageUrl: c.url,
            title: `${propertyName} · ${ROLE_CYCLE[cards.length % ROLE_CYCLE.length]} | ${brandName}`,
            body: `Hotel property photography · ${propertyName}`,
            role: ROLE_CYCLE[cards.length % ROLE_CYCLE.length],
            propertyName,
            sourcePageUrl: c.pageUrl,
            source: `factory-2-0-image-discovery:hotel-og`,
          },
          "hotel-og"
        );
      }
    }
  }

  const pool = {
    contract: BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1,
    slug,
    brandName,
    brandWebsite: website,
    generatedAt: new Date().toISOString(),
    researchSpendUsd: 0,
    imageCount: cards.length,
    images: cards.slice(0, Math.max(need, 6)),
    evidence: evidence.slice(0, 80),
    certificationBlocker:
      cards.length < need
        ? `image_discovery_insufficient:${cards.length}/${need}`
        : null,
  };
  if (opts.persist !== false) saveDiscoveredImagePool(repoRoot, slug, pool);
  return pool;
}

/**
 * Convert discovery pool images into gallery-card shape for remediator / builder.
 */
export function poolToGalleryCards(pool, brandName, count = 6) {
  return (pool?.images || []).slice(0, count).map((img, i) => ({
    imageUrl: img.imageUrl,
    title:
      img.title ||
      `${img.propertyName || brandName} · ${ROLE_CYCLE[i % ROLE_CYCLE.length]} | ${brandName}`,
    body: img.body || "Verified hotel property photography",
    role: img.role || ROLE_CYCLE[i % ROLE_CYCLE.length],
    propertyName: img.propertyName || brandName,
    source: img.source || "factory-2-0-image-discovery",
    sourcePageUrl: img.sourcePageUrl || null,
  }));
}
