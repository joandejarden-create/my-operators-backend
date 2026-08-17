/**
 * Paginated Cvent country results harvester → venue URL inventory.
 * Disk-cached under reports/cvent-venue-cache/country-results/.
 */

import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { extractCventVenueUrls } from "./census-cvent-venue-client.js";
import {
  buildCventCountryResultsUrl,
  CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
} from "./census-cvent-latam-country-registry.js";

export const CVENT_COUNTRY_RESULTS_HARVESTER_VERSION =
  "census-cvent-country-results-harvester-v2";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");
const CACHE_DIR = join(ROOT, "reports", "cvent-venue-cache", "country-results");

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const PAGE_SIZE = 25;

/** URL path segments treated as lodging inventory for census. */
export const CVENT_HOTEL_PATH_TYPES = Object.freeze([
  "hotel",
  "resort",
  "boutique-hotel",
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function cachePath(kind, key) {
  const h = createHash("sha1").update(`${kind}:${key}`).digest("hex").slice(0, 20);
  return join(CACHE_DIR, `${kind}-${h}.json`);
}

function readCache(path) {
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function writeCache(path, payload) {
  mkdirSync(CACHE_DIR, { recursive: true });
  writeFileSync(path, JSON.stringify(payload, null, 2));
}

async function fetchHtml(url, { timeoutMs = 35000, retries = 4 } = {}) {
  let last = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        signal: ctrl.signal,
        redirect: "follow",
        headers: {
          "User-Agent": UA,
          Accept: "text/html,application/xhtml+xml",
          "Accept-Language": "en-US,en;q=0.9",
        },
      });
      const text = await res.text();
      last = {
        ok: res.ok,
        status: res.status,
        bytes: text.length,
        text,
        finalUrl: res.url,
      };
      if (res.status !== 429 && res.status !== 503) return last;
      const backoff = Math.min(30000, 2500 * Math.pow(2, attempt));
      await sleep(backoff);
    } catch (e) {
      last = {
        ok: false,
        status: 0,
        bytes: 0,
        text: "",
        error: String(e?.message || e),
      };
      await sleep(1500 * (attempt + 1));
    } finally {
      clearTimeout(t);
    }
  }
  return last;
}

/**
 * @param {string} html
 */
export function parseCventResultsMeta(html) {
  const text = String(html || "");
  const totalMatch = text.match(/\\"totalCount\\":(\d+)/);
  const pageMatch = text.match(/\\"currentPage\\":(\d+)/);
  const ofMatch = text.match(
    /(\d+)-(\d+)\s+of\s+(\d+)\s+events and meeting venues/i
  );
  return {
    totalCount: totalMatch
      ? Number(totalMatch[1])
      : ofMatch
        ? Number(ofMatch[3])
        : null,
    currentPage: pageMatch
      ? Number(pageMatch[1])
      : ofMatch
        ? Number(ofMatch[1]) === 1
          ? 1
          : null
        : null,
    pageSize: PAGE_SIZE,
    ofLabel: ofMatch ? ofMatch[0] : null,
  };
}

/**
 * Classify a venue URL as hotel-like vs skip.
 * @param {string} url
 */
export function classifyCventVenueUrl(url) {
  const u = String(url || "").toLowerCase();
  const m = u.match(/\/venues\/[^/]+\/([a-z0-9-]+)\//i);
  const pathType = m ? m[1] : null;
  const hotelLike = Boolean(
    pathType && CVENT_HOTEL_PATH_TYPES.includes(pathType)
  );
  return {
    url,
    pathType,
    hotelLike,
    skip_reason: hotelLike
      ? null
      : pathType
        ? `non_hotel_type:${pathType}`
        : "unknown_path_type",
  };
}

/**
 * Extract venue UUID from Cvent venue URL.
 * @param {string} url
 */
export function extractCventVenueUuid(url) {
  const m = String(url || "").match(/venue-([a-f0-9-]{36})/i);
  return m ? m[1].toLowerCase() : null;
}

/**
 * Probe one country results page (page 1) for totalCount / viability.
 * @param {{ country: string, slug: string }} country
 * @param {{ throttleMs?: number, useCache?: boolean }} [opts]
 */
export async function probeCventCountry(country, opts = {}) {
  const throttleMs = Number(opts.throttleMs ?? 1100);
  const useCache = opts.useCache !== false;
  const path = cachePath("probe", country.slug);
  if (useCache) {
    const hit = readCache(path);
    if (hit?.probed_at && hit.ok) return { ...hit, from_cache: true };
  }

  const url = buildCventCountryResultsUrl(country.slug, {
    page: 1,
    term: country.country,
  });
  const page = await fetchHtml(url);
  await sleep(throttleMs);
  const meta = parseCventResultsMeta(page.text);
  const urls = extractCventVenueUrls(page.text);
  const payload = {
    version: CVENT_COUNTRY_RESULTS_HARVESTER_VERSION,
    registry_version: CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
    probed_at: new Date().toISOString(),
    country: country.country,
    slug: country.slug,
    url,
    ok: page.status === 200 && Number(meta.totalCount) > 0,
    status: page.status,
    bytes: page.bytes,
    error: page.error || null,
    totalCount: meta.totalCount,
    sample_urls: urls.slice(0, 5),
    sample_url_count: urls.length,
  };
  if (page.status === 404 || page.status === 429) payload.ok = false;
  if (useCache && payload.ok) writeCache(path, payload);
  return { ...payload, from_cache: false };
}

/**
 * Harvest all venue URLs for one country (paginate p=1..N).
 * @param {{ country: string, slug: string }} country
 * @param {{ throttleMs?: number, useCache?: boolean, maxPages?: number, totalCountHint?: number }} [opts]
 */
export async function harvestCventCountryVenueUrls(country, opts = {}) {
  const throttleMs = Number(opts.throttleMs ?? 2000);
  const useCache = opts.useCache !== false;
  const path = cachePath("harvest", country.slug);
  if (useCache) {
    const hit = readCache(path);
    if (hit?.urls && Array.isArray(hit.urls) && hit.complete === true) {
      return { ...hit, from_cache: true };
    }
  }

  const allUrls = new Set();
  const pages = [];
  let totalCount = opts.totalCountHint ?? null;
  let pageNum = 1;
  const maxPages = Number(opts.maxPages || 250);
  let stoppedEarly = false;

  while (pageNum <= maxPages) {
    const url = buildCventCountryResultsUrl(country.slug, {
      page: pageNum,
      term: country.country,
    });
    let page = await fetchHtml(url);
    await sleep(throttleMs);
    let meta = parseCventResultsMeta(page.text);
    if (meta.totalCount != null) totalCount = meta.totalCount;
    let found = extractCventVenueUrls(page.text);

    const expectedPages =
      totalCount != null && totalCount > 0
        ? Math.ceil(totalCount / PAGE_SIZE)
        : null;

    if (
      found.length === 0 &&
      pageNum > 1 &&
      expectedPages != null &&
      pageNum <= expectedPages
    ) {
      await sleep(Math.max(throttleMs * 3, 6000));
      page = await fetchHtml(url, { retries: 6 });
      await sleep(throttleMs);
      meta = parseCventResultsMeta(page.text);
      if (meta.totalCount != null) totalCount = meta.totalCount;
      found = extractCventVenueUrls(page.text);
    }

    for (const u of found) allUrls.add(u.split(/[?#]/)[0]);
    pages.push({
      p: pageNum,
      status: page.status,
      urls: found.length,
      totalCount: meta.totalCount,
      ofLabel: meta.ofLabel,
      error: page.error || null,
    });

    if (!page.ok && page.status === 404) {
      stoppedEarly = true;
      break;
    }

    const expectedNow =
      totalCount != null && totalCount > 0
        ? Math.ceil(totalCount / PAGE_SIZE)
        : null;

    if (found.length === 0 && pageNum > 1) {
      if (expectedNow != null && pageNum < expectedNow) stoppedEarly = true;
      break;
    }

    if (expectedNow != null && pageNum >= expectedNow) break;
    if (expectedNow == null && found.length < 5 && pageNum > 1) break;
    pageNum += 1;
  }

  const classified = [...allUrls].map(classifyCventVenueUrl);
  const hotelUrls = classified.filter((c) => c.hotelLike).map((c) => c.url);
  const skipped = classified.filter((c) => !c.hotelLike);

  const expectedPagesFinal =
    totalCount != null && totalCount > 0
      ? Math.ceil(totalCount / PAGE_SIZE)
      : null;
  const pagesOk =
    !stoppedEarly &&
    (expectedPagesFinal == null || pages.length >= expectedPagesFinal);

  const payload = {
    version: CVENT_COUNTRY_RESULTS_HARVESTER_VERSION,
    harvested_at: new Date().toISOString(),
    country: country.country,
    slug: country.slug,
    totalCount,
    pages_fetched: pages.length,
    pages,
    url_count: allUrls.size,
    hotel_url_count: hotelUrls.length,
    skipped_non_hotel_count: skipped.length,
    urls: hotelUrls,
    skipped_non_hotel: skipped.slice(0, 50),
    complete: Boolean(pagesOk),
    stopped_early: stoppedEarly,
  };
  if (useCache) writeCache(path, payload);
  return { ...payload, from_cache: false };
}

/**
 * Probe many countries; return viable list with totals.
 * @param {Array<{ country: string, slug: string }>} countries
 * @param {{ throttleMs?: number, useCache?: boolean }} [opts]
 */
export async function probeCventCountries(countries, opts = {}) {
  const results = [];
  const throttleMs = Number(opts.throttleMs ?? 2000);
  for (const c of countries) {
    results.push(await probeCventCountry(c, { ...opts, throttleMs }));
  }
  const viable = results.filter((r) => r.ok && Number(r.totalCount) > 0);
  return {
    version: CVENT_COUNTRY_RESULTS_HARVESTER_VERSION,
    registry_version: CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
    probed_at: new Date().toISOString(),
    scanned: results.length,
    viable_count: viable.length,
    total_venues_reported: viable.reduce(
      (s, r) => s + (Number(r.totalCount) || 0),
      0
    ),
    results,
    viable: viable.map((r) => ({
      country: r.country,
      slug: r.slug,
      totalCount: r.totalCount,
      status: r.status,
    })),
  };
}
