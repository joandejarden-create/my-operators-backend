/**
 * Phase 4H — Conservative property-level discovery path analysis (no deep crawl).
 */

import { isUrlAllowedByRobots, extractSitemapLocs } from "./sources/brand-directory.js";
import { getSourceRiskLevel } from "./source-policy.js";
import { SOURCE_TYPES } from "./fields.js";

const USER_AGENT = "DealalityPropertyPathAnalysis/1.0 (research; contact-dealality)";
const FETCH_TIMEOUT_MS = 15000;

const BOOKING_PATH_BLOCK =
  /\/(book|booking|reserve|reservation|checkout|availability|rates?|rooms?\/book)/i;
const BOOKING_HOST_BLOCK = /book\.|reservations\.|api\./i;

const LOCATOR_HINT =
  /find[-_]?a[-_]?hotel|hotel[-_]?search|search[-_]?hotels?|hotel[-_]?locator|find[-_]?hotel|\/hotels\?|\/search\b|property[-_]?finder/i;

const REGION_HINT =
  /\/(caribbean|latin-america|latam|international|countries|regions|global)\b|region=/i;

const PROPERTY_HINT =
  /\/hotels?\/[a-z0-9][a-z0-9-]{2,}/i;

const SITEMAP_HINT = /sitemap/i;

export const DISCOVERY_METHODS = {
  MANUAL_REVIEW: "manual_review",
  SITEMAP_REVIEW: "sitemap_review",
  LOCATOR_PAGE_REVIEW: "locator_page_review",
  PERMISSIONED_EXPORT: "permissioned_export_needed",
  NOT_RECOMMENDED: "not_recommended",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeOrigin(url) {
  try {
    return new URL(url).origin;
  } catch {
    return "";
  }
}

function safeHostname(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function resolveHref(href, baseUrl) {
  if (!href || /^#|javascript:|mailto:|tel:/i.test(href)) return null;
  try {
    return new URL(href, baseUrl).href;
  } catch {
    return null;
  }
}

function isBlockedEndpoint(url) {
  try {
    const u = new URL(url);
    if (BOOKING_HOST_BLOCK.test(u.hostname)) return true;
    if (BOOKING_PATH_BLOCK.test(u.pathname)) return true;
    if (/\.(jpg|jpeg|png|gif|webp|svg|css|js|woff2?)$/i.test(u.pathname)) return true;
  } catch {
    return true;
  }
  return false;
}

function classifyLink(url) {
  if (isBlockedEndpoint(url)) return "blocked";
  if (SITEMAP_HINT.test(url)) return "sitemap";
  if (PROPERTY_HINT.test(url)) return "property";
  if (LOCATOR_HINT.test(url)) return "locator";
  if (REGION_HINT.test(url)) return "region";
  return "other";
}

async function fetchText(url, fetchFn = globalThis.fetch) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetchFn(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, Accept: "text/html,text/xml,application/xml" },
      redirect: "follow",
    });
    clearTimeout(timer);
    const contentType = (res.headers.get("content-type") || "").toLowerCase();
    if (!res.ok) {
      return { ok: false, status: res.status, text: "", contentType };
    }
    if (!/html|xml|text/.test(contentType) && contentType) {
      return { ok: false, status: res.status, text: "", contentType, skipped: "non-text" };
    }
    const text = await res.text();
    return { ok: true, status: res.status, text, contentType, finalUrl: res.url };
  } catch (err) {
    clearTimeout(timer);
    return { ok: false, error: err.message || String(err), text: "" };
  }
}

/**
 * @returns {{ status: string, sitemapUrls: string[], disallowSamples: string[], notes: string }}
 */
export async function analyzeRobots(origin, fetchFn = globalThis.fetch) {
  if (!origin) {
    return { status: "no_origin", sitemapUrls: [], disallowSamples: [], notes: "" };
  }
  const robotsUrl = `${origin}/robots.txt`;
  try {
    const res = await fetchFn(robotsUrl, {
      headers: { "User-Agent": USER_AGENT },
    });
    if (!res.ok) {
      return {
        status: "robots_not_found",
        sitemapUrls: [],
        disallowSamples: [],
        notes: `HTTP ${res.status}`,
      };
    }
    const text = await res.text();
    const sitemapUrls = [...text.matchAll(/^Sitemap:\s*(.+)$/gim)].map((m) => m[1].trim());
    const disallows = [...text.matchAll(/^Disallow:\s*(.+)$/gim)]
      .map((m) => m[1].trim())
      .filter(Boolean);
    const blocksAll = disallows.some((d) => d === "/");
    return {
      status: blocksAll ? "disallow_root" : "robots_loaded",
      sitemapUrls,
      disallowSamples: disallows.slice(0, 8),
      notes: blocksAll ? "Disallow: / present" : `${disallows.length} disallow rules`,
    };
  } catch (err) {
    return {
      status: "robots_fetch_failed",
      sitemapUrls: [],
      disallowSamples: [],
      notes: err.message || String(err),
    };
  }
}

function extractLinksFromHtml(html, baseUrl) {
  const links = new Set();
  const re = /href\s*=\s*["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const abs = resolveHref(m[1], baseUrl);
    if (abs) links.add(abs);
  }
  return [...links];
}

function inferSourceUrlType(sourceUrl, linkSummary) {
  if (LOCATOR_HINT.test(sourceUrl)) return "hotel_locator_page";
  if (linkSummary.locator.length) return "brand_homepage_with_locator_links";
  if (linkSummary.region.length) return "brand_homepage_with_region_links";
  return "brand_homepage";
}

function recommendMethod(ctx) {
  const {
    fetchOk,
    robotsStatus,
    directPropertyCount,
    locatorUrl,
    sitemapUrl,
    sitemapHotelUrlCount,
  } = ctx;

  if (robotsStatus === "disallow_root") return DISCOVERY_METHODS.NOT_RECOMMENDED;
  if (!fetchOk) return DISCOVERY_METHODS.MANUAL_REVIEW;

  if (directPropertyCount >= 3) return DISCOVERY_METHODS.LOCATOR_PAGE_REVIEW;
  if (sitemapUrl && sitemapHotelUrlCount > 0) return DISCOVERY_METHODS.SITEMAP_REVIEW;
  if (locatorUrl) return DISCOVERY_METHODS.LOCATOR_PAGE_REVIEW;
  if (sitemapUrl) return DISCOVERY_METHODS.SITEMAP_REVIEW;

  return DISCOVERY_METHODS.PERMISSIONED_EXPORT;
}

/**
 * One optional sitemap GET — no index recursion.
 */
async function probeSitemapOnce(sitemapUrl, fetchFn) {
  const allowed = await isUrlAllowedByRobots(sitemapUrl, fetchFn);
  if (!allowed) {
    return { url: sitemapUrl, allowed: false, hotelUrls: [], hotelUrlCount: 0 };
  }
  const res = await fetchText(sitemapUrl, fetchFn);
  if (!res.ok || !res.text) {
    return { url: sitemapUrl, allowed: true, fetchOk: false, hotelUrls: [], hotelUrlCount: 0 };
  }
  const locs = extractSitemapLocs(res.text);
  const hotelUrls = locs
    .filter((u) => PROPERTY_HINT.test(u) || /\/[a-z]{2}\//i.test(u))
    .filter((u) => !isBlockedEndpoint(u))
    .slice(0, 25);
  return {
    url: sitemapUrl,
    allowed: true,
    fetchOk: true,
    hotelUrls,
    hotelUrlCount: hotelUrls.length,
  };
}

/**
 * @param {object} seed
 * @param {object} options
 */
export async function analyzeBrandPropertyPath(seed, options = {}) {
  const fetchFn = options.fetchFn || globalThis.fetch;
  const checkSitemap = options.fetchSitemap !== false;
  const delayMs = options.delayMs ?? 400;

  const brand = seed.brand || "";
  const parentCompany = seed.parentCompany || "";
  const sourceUrl = String(seed.sourceUrl || "").trim();
  const domain = safeHostname(sourceUrl);
  const origin = safeOrigin(sourceUrl);

  const base = {
    brand,
    parentCompany,
    sourceUrl,
    domain,
    brandStatus: seed.brandStatus || "",
    priorityRank: seed.priorityRank ?? "",
    sourceRiskLevel: getSourceRiskLevel(SOURCE_TYPES.BRAND_DIRECTORY),
  };

  if (!sourceUrl) {
    return {
      ...base,
      sourceUrlType: "missing_url",
      robotsStatus: "n/a",
      discoveredLocatorUrl: "",
      discoveredSitemapUrl: "",
      directPropertyUrlCount: 0,
      directPropertyUrls: [],
      recommendedDiscoveryMethod: DISCOVERY_METHODS.MANUAL_REVIEW,
      notes: "No source URL on seed.",
    };
  }

  const robots = await analyzeRobots(origin, fetchFn);
  await sleep(delayMs);

  const pathAllowed = await isUrlAllowedByRobots(sourceUrl, fetchFn);
  if (!pathAllowed) {
    return {
      ...base,
      sourceUrlType: "brand_homepage",
      robotsStatus: robots.status,
      discoveredLocatorUrl: "",
      discoveredSitemapUrl: robots.sitemapUrls[0] || "",
      directPropertyUrlCount: 0,
      directPropertyUrls: [],
      recommendedDiscoveryMethod: DISCOVERY_METHODS.NOT_RECOMMENDED,
      notes: `robots.txt disallows fetching source path. ${robots.notes}`,
    };
  }

  const page = await fetchText(sourceUrl, fetchFn);
  await sleep(delayMs);

  const linkSummary = { property: [], locator: [], region: [], sitemap: [], other: [] };
  let discoveredLocatorUrl = "";
  let discoveredSitemapUrl = robots.sitemapUrls[0] || "";

  if (page.ok && page.text) {
    const baseForLinks = page.finalUrl || sourceUrl;
    for (const link of extractLinksFromHtml(page.text, baseForLinks)) {
      if (!link.includes(domain) && !link.includes("choicehotels.com")) continue;
      const kind = classifyLink(link);
      if (kind === "blocked") continue;
      if (kind === "property") linkSummary.property.push(link);
      else if (kind === "locator") {
        linkSummary.locator.push(link);
        if (!discoveredLocatorUrl) discoveredLocatorUrl = link;
      } else if (kind === "region") linkSummary.region.push(link);
      else if (kind === "sitemap") {
        linkSummary.sitemap.push(link);
        if (!discoveredSitemapUrl) discoveredSitemapUrl = link;
      } else linkSummary.other.push(link);
    }

    const metaSitemap = [...page.text.matchAll(/https?:\/\/[^\s"'<>]+sitemap[^\s"'<>]*/gi)].map(
      (m) => m[0]
    );
    if (metaSitemap.length && !discoveredSitemapUrl) {
      discoveredSitemapUrl = metaSitemap[0];
    }
  }

  if (checkSitemap && !discoveredSitemapUrl && origin) {
    const standard = `${origin}/sitemap.xml`;
    const allowedSm = await isUrlAllowedByRobots(standard, fetchFn);
    if (allowedSm) discoveredSitemapUrl = standard;
  }

  let sitemapProbe = { hotelUrlCount: 0, hotelUrls: [] };
  if (checkSitemap && discoveredSitemapUrl) {
    sitemapProbe = await probeSitemapOnce(discoveredSitemapUrl, fetchFn);
    await sleep(delayMs);
  }

  const directPropertyUrls = [...new Set(linkSummary.property)].slice(0, 10);
  const directPropertyUrlCount = linkSummary.property.length;

  const sourceUrlType = inferSourceUrlType(sourceUrl, linkSummary);
  const recommendedDiscoveryMethod = recommendMethod({
    fetchOk: page.ok,
    robotsStatus: robots.status,
    directPropertyCount: directPropertyUrlCount + sitemapProbe.hotelUrlCount,
    locatorUrl: discoveredLocatorUrl,
    sitemapUrl: discoveredSitemapUrl,
    sitemapHotelUrlCount: sitemapProbe.hotelUrlCount,
  });

  const notes = [
    page.ok ? `Fetched ${page.status || 200} (${page.contentType || "html"})` : `Fetch failed: ${page.error || page.status}`,
    robots.notes,
    linkSummary.locator.length ? `${linkSummary.locator.length} locator link(s) on page` : "No locator links on brand page",
    discoveredSitemapUrl ? `Sitemap candidate: ${discoveredSitemapUrl}` : "No sitemap reference",
    sitemapProbe.hotelUrlCount
      ? `${sitemapProbe.hotelUrlCount} hotel-like URL(s) in sitemap sample`
      : "Sitemap not probed or no hotel URLs in sample",
    "No booking/rate endpoints fetched. Single-page inspection only.",
  ]
    .filter(Boolean)
    .join(" ");

  return {
    ...base,
    sourceUrlType,
    robotsStatus: robots.status,
    discoveredLocatorUrl,
    discoveredSitemapUrl,
    directPropertyUrlCount,
    directPropertyUrls,
    sitemapHotelUrlSampleCount: sitemapProbe.hotelUrlCount,
    recommendedDiscoveryMethod,
    notes,
  };
}

/**
 * @param {Array<object>} seeds
 */
export async function analyzeAllBrandPropertyPaths(seeds, options = {}) {
  const rows = [];
  for (let i = 0; i < seeds.length; i++) {
    rows.push(await analyzeBrandPropertyPath(seeds[i], options));
    if (i < seeds.length - 1 && options.delayMs !== 0) {
      await sleep(options.delayMs ?? 400);
    }
  }

  const byMethod = {};
  for (const r of rows) {
    byMethod[r.recommendedDiscoveryMethod] = (byMethod[r.recommendedDiscoveryMethod] || 0) + 1;
  }

  return {
    rows,
    summary: {
      brandsAnalyzed: rows.length,
      withLocatorUrl: rows.filter((r) => r.discoveredLocatorUrl).length,
      withSitemapUrl: rows.filter((r) => r.discoveredSitemapUrl).length,
      withDirectPropertyUrls: rows.filter((r) => r.directPropertyUrlCount > 0).length,
      totalDirectPropertyLinksOnPages: rows.reduce((s, r) => s + r.directPropertyUrlCount, 0),
      sitemapHotelUrlSamples: rows.reduce((s, r) => s + (r.sitemapHotelUrlSampleCount || 0), 0),
      byRecommendedDiscoveryMethod: byMethod,
    },
  };
}

export const PROPERTY_PATH_CSV_COLUMNS = [
  "brand",
  "parentCompany",
  "sourceUrl",
  "domain",
  "sourceUrlType",
  "robotsStatus",
  "discoveredLocatorUrl",
  "discoveredSitemapUrl",
  "directPropertyUrlCount",
  "directPropertyUrls",
  "recommendedDiscoveryMethod",
  "sourceRiskLevel",
  "notes",
];

export function propertyPathToCsvRow(r) {
  return {
    brand: r.brand,
    parentCompany: r.parentCompany,
    sourceUrl: r.sourceUrl,
    domain: r.domain,
    sourceUrlType: r.sourceUrlType,
    robotsStatus: r.robotsStatus,
    discoveredLocatorUrl: r.discoveredLocatorUrl,
    discoveredSitemapUrl: r.discoveredSitemapUrl,
    directPropertyUrlCount: r.directPropertyUrlCount,
    directPropertyUrls: (r.directPropertyUrls || []).join("; "),
    recommendedDiscoveryMethod: r.recommendedDiscoveryMethod,
    sourceRiskLevel: r.sourceRiskLevel,
    notes: r.notes,
  };
}
