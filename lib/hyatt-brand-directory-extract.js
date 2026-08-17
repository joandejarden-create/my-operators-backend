/**
 * Hyatt property directory from official hyatt.com hotel URLs.
 *
 * Live hyatt.com is often Kasada/429-blocked from server IPs. Prefer:
 * 1) Cached official sitemap XML (local file)
 * 2) Wayback Machine id_ snapshot of https://www.hyatt.com/sitemap.xml
 * 3) Wayback CDX prefix harvest for /en-US/hotel/{cala-segment}/…
 *
 * URL shape: https://www.hyatt.com/{locale}/hotel/{region}/{slug}/{propertyCode}
 * Property ID = trailing property code (e.g. MEXHR, SERPC).
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import {
  CENSUS_COUNTRY_TO_SITEMAP_SLUG,
  SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL,
} from "./marriott-brand-directory-extract.js";
import { COUNTRY_CONFIG_LIST } from "./radar-buildout/country-configs.js";
import { normalizeCountry, normalizeText } from "./independent-census/match-current-census.js";

/**
 * CALA URL path segments for hyatt.com /hotel/{segment}/…
 * Note: lib/brand-sitemap/cala-url-segments.js currently builds a Set of `true`
 * via Object.values — do not reuse that Set for membership checks.
 */
export const HYATT_CALA_REGION_SEGMENTS = new Set([
  ...Object.values(CENSUS_COUNTRY_TO_SITEMAP_SLUG),
  "dominican-republic",
  "mexico",
  "jamaica",
  "panama",
  "costa-rica",
  "colombia",
  "peru",
  "brazil",
  "chile",
  "ecuador",
  "venezuela",
  "guatemala",
  "honduras",
  "el-salvador",
  "bahamas",
  "barbados",
  "trinidad-and-tobago",
  "puerto-rico",
  "argentina",
  "aruba",
  "turks-and-caicos-islands",
  "virgin-islands-us",
  "guyana",
  "curacao",
  "saint-lucia",
  "belize",
  "antigua-and-barbuda",
  "cayman-islands",
  "suriname",
  "paraguay",
  "uruguay",
  "bolivia",
  "haiti",
  "grenada",
  "bermuda",
  "sint-maarten",
  "saint-kitts-and-nevis",
  // Extra CALA segments seen on hyatt.com / census (not always in Marriott slug map)
  "nicaragua",
  "turks-and-caicos",
]);

export const HYATT_ORIGIN = "https://www.hyatt.com";
export const HYATT_SITEMAP_URL = `${HYATT_ORIGIN}/sitemap.xml`;
export const HYATT_WAYBACK_SITEMAP_ID =
  "https://web.archive.org/web/20240126122711id_/https://www.hyatt.com/sitemap.xml";

export const HYATT_CONTENT_SOURCE = "hyatt_official_sitemap";
export const HYATT_CDX_SOURCE = "hyatt_wayback_cdx";

export const HYATT_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "application/xml,text/xml,text/html,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

const SUBPAGE_SEGMENTS = new Set([
  "area-attractions",
  "maps-parking-transportation",
  "rooms",
  "dining",
  "meetings",
  "spa",
  "offers",
  "gallery",
  "reviews",
  "photos-reviews",
  "wellness",
  "experiences",
  "location",
  "about",
  "photo-gallery",
  "activities",
  "weddings",
  "events",
  "amenities",
  "facilities",
  "overview",
  "home",
]);

/**
 * Inclusive Collection + leisure brand slug tokens for targeted Wayback CDX.
 * Harvested only from official hyatt.com URLs (never invented).
 */
export const HYATT_INCLUSIVE_BRAND_CDX_FILTERS = [
  "secrets-",
  "dreams-",
  "breathless-",
  "sunscape-",
  "zoetry-",
  "impression-",
  "now-",
  "hyatt-ziva-",
  "hyatt-zilara-",
  "hyatt-vivid-",
  "thompson-",
  "andaz-",
];

/** Classic / lifestyle tokens mainly for global CDX (region×brand would explode query count). */
export const HYATT_CLASSIC_BRAND_CDX_FILTERS = [
  "park-hyatt-",
  "grand-hyatt-",
  "hyatt-place-",
  "hyatt-regency-",
  "hyatt-centric-",
];

/** Priority CALA regions for Inclusive / leisure CDX top-up (high unmatched density). */
export const HYATT_INCLUSIVE_CDX_PRIORITY_REGIONS = [
  "mexico",
  "dominican-republic",
  "jamaica",
  "costa-rica",
  "panama",
  "colombia",
  "aruba",
  "curacao",
  "saint-lucia",
  "belize",
  "nicaragua",
  "bahamas",
  "puerto-rico",
  "turks-and-caicos-islands",
  "cayman-islands",
  "guyana",
];

const CALA_COUNTRY_SET = new Set(COUNTRY_CONFIG_LIST.map((c) => c.toLowerCase()));

/** Title-case census labels keyed by lowercase. */
const CENSUS_COUNTRY_TITLE = Object.fromEntries(
  COUNTRY_CONFIG_LIST.map((c) => [c.toLowerCase(), c])
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} country
 */
export function isCalaCountry(country) {
  const key = normalizeCountry(country) || String(country || "").trim().toLowerCase();
  if (!key) return false;
  if (CALA_COUNTRY_SET.has(key)) return true;
  const slug = CENSUS_COUNTRY_TO_SITEMAP_SLUG[key];
  return Boolean(slug && HYATT_CALA_REGION_SEGMENTS.has(slug));
}

/**
 * @param {string} regionSlug
 */
const HYATT_REGION_SLUG_ALIASES = {
  "turks-and-caicos": "turks-and-caicos-islands",
};

/**
 * @param {string} regionSlug
 */
export function hyattRegionToCensusCountry(regionSlug) {
  const slugRaw = String(regionSlug || "")
    .trim()
    .toLowerCase();
  if (!slugRaw) return "";
  const slug = HYATT_REGION_SLUG_ALIASES[slugRaw] || slugRaw;
  const fromMarriott = SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL[slug];
  if (fromMarriott) {
    return CENSUS_COUNTRY_TITLE[fromMarriott] || titleCaseSlug(fromMarriott);
  }
  if (slug === "nicaragua") return CENSUS_COUNTRY_TITLE.nicaragua || "Nicaragua";
  if (slug === "turks-and-caicos-islands") {
    return (
      CENSUS_COUNTRY_TITLE["turks and caicos islands"] ||
      CENSUS_COUNTRY_TITLE["turks & caicos"] ||
      "Turks and Caicos Islands"
    );
  }
  const guess = titleCaseSlug(slug);
  return CENSUS_COUNTRY_TITLE[guess.toLowerCase()] || guess;
}

function titleCaseSlug(slug) {
  return String(slug || "")
    .split(/[-\s]+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/**
 * Infer display name from property slug (never invented amenities — name guess only for matching).
 * @param {string} slug
 */
export function nameFromHyattSlug(slug) {
  return titleCaseSlug(String(slug || "").replace(/_/g, "-"));
}

/**
 * @param {string} url
 */
export function hyattPropertyIdFromUrl(url) {
  const parsed = parseHyattHotelUrl(url);
  return parsed?.propertyId || "";
}

/**
 * @param {string} url
 * @returns {{ locale: string, region: string, slug: string, propertyId: string, propertyUrl: string, isSubpage: boolean } | null}
 */
export function parseHyattHotelUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    const host = u.hostname.replace(/^www\./i, "").toLowerCase();
    if (host !== "hyatt.com") return null;

    const parts = u.pathname.split("/").filter(Boolean);
    const hotelIdx = parts.findIndex((p) => p.toLowerCase() === "hotel");
    if (hotelIdx < 0 || parts.length < hotelIdx + 4) return null;

    const locale = hotelIdx >= 1 ? parts[hotelIdx - 1] : "en-US";
    const region = parts[hotelIdx + 1].toLowerCase();
    const slug = parts[hotelIdx + 2];
    const codeRaw = parts[hotelIdx + 3];
    const rest = parts.slice(hotelIdx + 4);

    if (!/^[a-z0-9]{3,8}$/i.test(codeRaw)) return null;
    if (SUBPAGE_SEGMENTS.has(codeRaw.toLowerCase())) return null;

    const isSubpage = rest.length > 0;
    if (isSubpage && !SUBPAGE_SEGMENTS.has(rest[0].toLowerCase()) && rest.length > 1) {
      // Unusual path — still accept if code looks like a property id.
    }

    const propertyId = codeRaw.toUpperCase();
    const propertyUrl = `${HYATT_ORIGIN}/en-US/hotel/${region}/${slug}/${codeRaw.toLowerCase()}`;

    return {
      locale,
      region,
      slug,
      propertyId,
      propertyUrl,
      isSubpage,
    };
  } catch {
    return null;
  }
}

/**
 * @param {string} xml
 */
export function extractSitemapLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

/**
 * @param {string[]} locs
 * @param {{ calaOnly?: boolean }} [opts]
 */
export function parseHyattPropertyRowsFromLocs(locs, opts = {}) {
  const calaOnly = opts.calaOnly !== false;
  /** @type {Map<string, object>} */
  const byId = new Map();

  for (const loc of locs) {
    const parsed = parseHyattHotelUrl(loc);
    if (!parsed) continue;
    if (calaOnly && !HYATT_CALA_REGION_SEGMENTS.has(parsed.region)) continue;

    const censusCountry = hyattRegionToCensusCountry(parsed.region);
    const row = {
      propertyId: parsed.propertyId,
      propertyUrl: parsed.propertyUrl,
      regionSlug: parsed.region,
      slug: parsed.slug,
      name: nameFromHyattSlug(parsed.slug),
      country: censusCountry,
      censusCountry,
      localeSeen: parsed.locale,
      isCala: isCalaCountry(censusCountry) || HYATT_CALA_REGION_SEGMENTS.has(parsed.region),
      source: HYATT_CONTENT_SOURCE,
    };

    const prev = byId.get(parsed.propertyId);
    if (!prev) {
      byId.set(parsed.propertyId, row);
      continue;
    }
    // Prefer en-US canonical when duplicates appear across locales.
    if (parsed.locale === "en-US" && prev.localeSeen !== "en-US") {
      byId.set(parsed.propertyId, row);
    }
  }

  return [...byId.values()].sort(
    (a, b) =>
      String(a.regionSlug).localeCompare(String(b.regionSlug)) ||
      String(a.slug).localeCompare(String(b.slug))
  );
}

/**
 * @param {string} xml
 * @param {{ calaOnly?: boolean }} [opts]
 */
export function parseHyattSitemapPropertyRows(xml, opts = {}) {
  return parseHyattPropertyRowsFromLocs(extractSitemapLocs(xml), opts);
}

/**
 * @param {string} filePath
 * @param {{ calaOnly?: boolean }} [opts]
 */
export function loadHyattSitemapFromFile(filePath, opts = {}) {
  if (!existsSync(filePath)) {
    return { ok: false, error: `missing_file:${filePath}`, propertyRows: [] };
  }
  const xml = readFileSync(filePath, "utf8");
  const propertyRows = parseHyattSitemapPropertyRows(xml, opts);
  return {
    ok: true,
    source: filePath,
    locCount: extractSitemapLocs(xml).length,
    propertyRows,
  };
}

/**
 * Try live hyatt.com sitemap, then Wayback id_ snapshot.
 * @param {{ delayMs?: number, waybackUrl?: string, fetchFn?: typeof fetch }} [opts]
 */
export async function fetchHyattSitemapXml(opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const delayMs = opts.delayMs ?? 2000;
  const attempts = [
    { label: "live", url: HYATT_SITEMAP_URL },
    { label: "wayback", url: opts.waybackUrl || HYATT_WAYBACK_SITEMAP_ID },
  ];

  /** @type {object[]} */
  const errors = [];

  for (const attempt of attempts) {
    try {
      const res = await fetchFn(attempt.url, {
        redirect: "follow",
        headers: HYATT_FETCH_HEADERS,
      });
      const text = await res.text();
      const blocked =
        res.status === 429 ||
        res.status === 403 ||
        /KPSDK|access denied|just a moment|captcha/i.test(text.slice(0, 2000));
      if (!res.ok || blocked || !/<loc>/i.test(text)) {
        errors.push({
          label: attempt.label,
          status: res.status,
          blocked,
          len: text.length,
        });
        await sleep(delayMs);
        continue;
      }
      return { ok: true, label: attempt.label, url: attempt.url, xml: text, errors };
    } catch (err) {
      errors.push({ label: attempt.label, error: err?.message || String(err) });
      await sleep(delayMs);
    }
  }

  return { ok: false, errors, xml: "" };
}

/**
 * @param {string} cdxUrl
 * @param {{ fetchFn?: typeof fetch, fetchTimeoutMs?: number, label?: string }} opts
 */
async function fetchCdxOriginalUrls(cdxUrl, opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const fetchTimeoutMs = opts.fetchTimeoutMs ?? 25000;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), fetchTimeoutMs);
  try {
    const res = await fetchFn(cdxUrl, {
      headers: { "User-Agent": "DealalityResearch/1.0 (hyatt census)" },
      signal: ctrl.signal,
    });
    const text = await res.text();
    let rows = [];
    try {
      rows = JSON.parse(text);
    } catch {
      rows = [];
    }
    const urls = Array.isArray(rows)
      ? rows
          .slice(1)
          .map((r) => (Array.isArray(r) ? r[0] : ""))
          .filter(Boolean)
      : [];
    const seen = new Set();
    const uniqueUrls = [];
    for (const u of urls) {
      if (seen.has(u)) continue;
      seen.add(u);
      uniqueUrls.push(u);
    }
    return { status: res.status, uniqueUrls, error: null };
  } catch (err) {
    return { status: 0, uniqueUrls: [], error: err?.message || String(err) };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Supplement directory via Wayback CDX for CALA region prefixes.
 * @param {{ regions?: string[], delayMs?: number, limitPerRegion?: number, fetchTimeoutMs?: number, fetchFn?: typeof fetch, originalFilter?: string }} [opts]
 */
export async function harvestHyattCalaFromWaybackCdx(opts = {}) {
  const delayMs = opts.delayMs ?? 1200;
  const limitPerRegion = opts.limitPerRegion ?? 500;
  const fetchTimeoutMs = opts.fetchTimeoutMs ?? 25000;
  const regions = opts.regions?.length
    ? opts.regions
    : [...HYATT_CALA_REGION_SEGMENTS].sort();

  /** @type {string[]} */
  const locs = [];
  /** @type {object[]} */
  const fetchLog = [];

  for (const region of regions) {
    let cdx = `https://web.archive.org/cdx/search/cdx?url=www.hyatt.com/en-US/hotel/${encodeURIComponent(
      region
    )}/*&output=json&fl=original&collapse=urlkey&limit=${limitPerRegion}&filter=statuscode:200`;
    if (opts.originalFilter) {
      cdx += `&filter=original:${encodeURIComponent(opts.originalFilter)}`;
    }
    const result = await fetchCdxOriginalUrls(cdx, {
      fetchFn: opts.fetchFn,
      fetchTimeoutMs,
      label: region,
    });
    if (result.error) {
      fetchLog.push({ region, error: result.error, filter: opts.originalFilter || null });
      if (opts.onProgress) opts.onProgress(`${region}: ERROR ${result.error}`);
    } else {
      locs.push(...result.uniqueUrls);
      fetchLog.push({
        region,
        status: result.status,
        urlCount: result.uniqueUrls.length,
        filter: opts.originalFilter || null,
      });
      if (opts.onProgress) {
        opts.onProgress(
          `${region}${opts.originalFilter ? ` [${opts.originalFilter}]` : ""}: ${result.uniqueUrls.length} urls (http ${result.status})`
        );
      }
    }
    await sleep(delayMs);
  }

  const propertyRows = parseHyattPropertyRowsFromLocs(locs, { calaOnly: true }).map((r) => ({
    ...r,
    source: HYATT_CDX_SOURCE,
  }));

  return { ok: true, fetchLog, locCount: locs.length, propertyRows };
}

/**
 * Targeted Wayback CDX for Inclusive Collection / leisure brands on hyatt.com.
 * Region×brand queries recover URLs truncated by generic region CDX limits;
 * global brand queries catch properties in regions not yet enumerated.
 *
 * @param {{
 *   regions?: string[],
 *   brandFilters?: string[],
 *   delayMs?: number,
 *   limitPerQuery?: number,
 *   fetchTimeoutMs?: number,
 *   includeGlobalBrandPass?: boolean,
 *   fetchFn?: typeof fetch,
 *   onProgress?: (msg: string) => void,
 * }} [opts]
 */
export async function harvestHyattInclusiveBrandCdx(opts = {}) {
  const delayMs = opts.delayMs ?? 1000;
  const limitPerQuery = opts.limitPerQuery ?? 400;
  const fetchTimeoutMs = opts.fetchTimeoutMs ?? 35000;
  const regions = opts.regions?.length
    ? opts.regions
    : HYATT_INCLUSIVE_CDX_PRIORITY_REGIONS;
  const brandFilters = opts.brandFilters?.length
    ? opts.brandFilters
    : HYATT_INCLUSIVE_BRAND_CDX_FILTERS;
  const includeGlobal = opts.includeGlobalBrandPass !== false;

  /** @type {string[]} */
  const locs = [];
  /** @type {object[]} */
  const fetchLog = [];

  // Pass 1: priority region × inclusive/leisure brand token
  for (const region of regions) {
    for (const brand of brandFilters) {
      const filter = `.*${brand}.*`;
      const cdx = `https://web.archive.org/cdx/search/cdx?url=www.hyatt.com/en-US/hotel/${encodeURIComponent(
        region
      )}/*&output=json&fl=original&collapse=urlkey&limit=${limitPerQuery}&filter=statuscode:200&filter=original:${encodeURIComponent(filter)}`;
      const result = await fetchCdxOriginalUrls(cdx, {
        fetchFn: opts.fetchFn,
        fetchTimeoutMs,
      });
      const label = `${region}/${brand.replace(/-$/, "")}`;
      if (result.error) {
        fetchLog.push({ region, brand, error: result.error });
        if (opts.onProgress) opts.onProgress(`${label}: ERROR ${result.error}`);
      } else {
        locs.push(...result.uniqueUrls);
        fetchLog.push({
          region,
          brand,
          status: result.status,
          urlCount: result.uniqueUrls.length,
        });
        if (opts.onProgress && result.uniqueUrls.length) {
          opts.onProgress(`${label}: ${result.uniqueUrls.length} urls`);
        }
      }
      await sleep(delayMs);
    }
  }

  // Pass 2: global brand filters (CALA filter applied at parse time)
  if (includeGlobal) {
    const globalBrands = [
      ...brandFilters,
      ...(opts.globalExtraBrandFilters || []),
    ];
    const seenBrand = new Set();
    for (const brand of globalBrands) {
      if (seenBrand.has(brand)) continue;
      seenBrand.add(brand);
      const filter = `.*${brand}.*`;
      const cdx = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
        "www.hyatt.com/en-US/hotel/*"
      )}&output=json&fl=original&collapse=urlkey&limit=${limitPerQuery}&filter=statuscode:200&filter=original:${encodeURIComponent(filter)}`;
      const result = await fetchCdxOriginalUrls(cdx, {
        fetchFn: opts.fetchFn,
        fetchTimeoutMs,
      });
      const label = `global/${brand.replace(/-$/, "")}`;
      if (result.error) {
        fetchLog.push({ region: "*", brand, error: result.error });
        if (opts.onProgress) opts.onProgress(`${label}: ERROR ${result.error}`);
      } else {
        locs.push(...result.uniqueUrls);
        fetchLog.push({
          region: "*",
          brand,
          status: result.status,
          urlCount: result.uniqueUrls.length,
        });
        if (opts.onProgress) {
          opts.onProgress(`${label}: ${result.uniqueUrls.length} urls`);
        }
      }
      await sleep(delayMs);
    }
  }

  const propertyRows = parseHyattPropertyRowsFromLocs(locs, { calaOnly: true }).map((r) => ({
    ...r,
    source: HYATT_CDX_SOURCE,
  }));

  return { ok: true, fetchLog, locCount: locs.length, propertyRows };
}

/**
 * Merge property rows by Property ID (prefer sitemap over CDX; keep first non-empty name).
 * Flags propertyId collisions where slug brand families disagree.
 * @param {...object[]} lists
 */
export function mergeHyattDirectoryRows(...lists) {
  /** @type {Map<string, object>} */
  const byId = new Map();
  /** @type {object[]} */
  const propertyIdConflicts = [];
  const sourceRank = { [HYATT_CONTENT_SOURCE]: 2, [HYATT_CDX_SOURCE]: 1 };

  function brandKey(slugOrName) {
    const s = String(slugOrName || "").toLowerCase();
    if (/hyatt-regency|hyatt regency/.test(s)) return "hyatt-regency";
    if (/hyatt-place|hyatt place/.test(s)) return "hyatt-place";
    if (/hyatt-house|hyatt house/.test(s)) return "hyatt-house";
    if (/hyatt-centric|hyatt centric/.test(s)) return "hyatt-centric";
    if (/grand-hyatt|grand hyatt/.test(s)) return "grand-hyatt";
    if (/park-hyatt|park hyatt/.test(s)) return "park-hyatt";
    if (/andaz/.test(s)) return "andaz";
    if (/thompson/.test(s)) return "thompson";
    if (/alila/.test(s)) return "alila";
    if (/ziva/.test(s)) return "ziva";
    if (/zilara/.test(s)) return "zilara";
    if (/secrets/.test(s)) return "secrets";
    if (/dreams?/.test(s)) return "dreams";
    if (/breathless/.test(s)) return "breathless";
    if (/sunscape/.test(s)) return "sunscape";
    if (/zoetry/.test(s)) return "zoetry";
    if (/vivid/.test(s)) return "vivid";
    if (/impression/.test(s)) return "impression";
    if (/(^|-)now-/.test(s) || /\bnow\b/.test(s)) return "now";
    return "other";
  }

  for (const list of lists) {
    for (const row of list || []) {
      const id = String(row.propertyId || "").toUpperCase();
      if (!id) continue;
      const prev = byId.get(id);
      if (!prev) {
        byId.set(id, { ...row, propertyId: id });
        continue;
      }
      const prevBrand = brandKey(prev.slug || prev.name);
      const nextBrand = brandKey(row.slug || row.name);
      if (prevBrand !== "other" && nextBrand !== "other" && prevBrand !== nextBrand) {
        propertyIdConflicts.push({
          propertyId: id,
          keepSlug: prev.slug,
          conflictSlug: row.slug,
          keepUrl: prev.propertyUrl,
          conflictUrl: row.propertyUrl,
        });
        // Keep first; do not overwrite with conflicting brand URL.
        continue;
      }
      const prevRank = sourceRank[prev.source] || 0;
      const nextRank = sourceRank[row.source] || 0;
      if (nextRank > prevRank) {
        byId.set(id, {
          ...row,
          propertyId: id,
          name: row.name || prev.name,
        });
      } else if (!prev.name && row.name) {
        prev.name = row.name;
      }
    }
  }

  const propertyRows = [...byId.values()].sort(
    (a, b) =>
      String(a.regionSlug).localeCompare(String(b.regionSlug)) ||
      String(a.slug).localeCompare(String(b.slug))
  );
  return { propertyRows, propertyIdConflicts };
}

/**
 * @param {string} html
 */
export function parseHyattAmenitiesFromHtml(html) {
  /** @type {string[]} */
  const amenities = [];
  const parseErrors = [];

  for (const block of String(html || "").match(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  ) || []) {
    const inner = block.replace(/<\/?script[^>]*>/gi, "").trim();
    try {
      const json = JSON.parse(inner);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;
        const feats = obj.amenityFeature;
        const list = Array.isArray(feats) ? feats : feats ? [feats] : [];
        for (const feat of list) {
          const name = normalizeText(feat?.name || "");
          const val = String(feat?.value ?? "true").toLowerCase();
          if (name && val !== "false") amenities.push(name);
        }
      }
    } catch (err) {
      parseErrors.push(err?.message || String(err));
    }
  }

  // Conservative secondary: data-amenity or amenity list items only when clearly labeled.
  if (!amenities.length) {
    for (const m of String(html || "").matchAll(
      /(?:amenity|amenities)[^>]*>[\s\S]{0,80}?>([^<]{3,80})</gi
    )) {
      const name = normalizeText(m[1]);
      if (name && !/amenit/i.test(name) && name.length < 60) amenities.push(name);
    }
  }

  const uniq = [...new Set(amenities)].sort((a, b) => a.localeCompare(b));
  return {
    amenities: uniq,
    amenitiesText: uniq.join("; "),
    parseErrors,
  };
}

/**
 * Probe a single official Hyatt property page for amenities (no invention).
 * @param {string} propertyUrl
 * @param {{ delayMs?: number, fetchFn?: typeof fetch }} [opts]
 */
export async function fetchHyattHotelAmenities(propertyUrl, opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const parsed = parseHyattHotelUrl(propertyUrl);
  if (!parsed) {
    return {
      ok: false,
      status: 0,
      blocked: false,
      amenities: [],
      amenitiesText: "",
      error: "invalid_hyatt_url",
      source: null,
    };
  }

  try {
    const res = await fetchFn(parsed.propertyUrl, {
      redirect: "follow",
      headers: {
        ...HYATT_FETCH_HEADERS,
        Accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
      },
    });
    const html = await res.text();
    const blocked =
      res.status === 429 ||
      res.status === 403 ||
      /KPSDK|access denied|just a moment|captcha/i.test(html.slice(0, 2000));

    if (!res.ok || blocked) {
      return {
        ok: false,
        status: res.status,
        blocked,
        amenities: [],
        amenitiesText: "",
        error: blocked ? "blocked_or_rate_limited" : `http_${res.status}`,
        source: null,
        htmlLength: html.length,
      };
    }

    const parsedAmenities = parseHyattAmenitiesFromHtml(html);
    return {
      ok: Boolean(parsedAmenities.amenitiesText),
      status: res.status,
      blocked: false,
      amenities: parsedAmenities.amenities,
      amenitiesText: parsedAmenities.amenitiesText,
      parseErrors: parsedAmenities.parseErrors,
      source: parsedAmenities.amenitiesText ? "hyatt_hotel_page_jsonld" : null,
      htmlLength: html.length,
      propertyUrl: parsed.propertyUrl,
      propertyId: parsed.propertyId,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      blocked: false,
      amenities: [],
      amenitiesText: "",
      error: err?.message || String(err),
      source: null,
    };
  }
}

/**
 * Write directory extract JSON.
 * @param {string} outPath
 * @param {object} payload
 */
export function writeHyattDirectoryExtract(outPath, payload) {
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(payload, null, 2) + "\n", "utf8");
}
