/**
 * Accor property directory from all.accor.com sitemap-fh (verified URLs).
 */

import {
  accorCountryCodeIsCala,
  ACCOR_COUNTRY_CODE_TO_LABEL,
} from "./brand-sitemap/cala-url-segments.js";
import { normalizeCountry } from "./independent-census/match-current-census.js";

export const ACCOR_ORIGIN = "https://all.accor.com";
export const ACCOR_HOTEL_SITEMAP_EN = `${ACCOR_ORIGIN}/sitemap-fh.en.xml`;

export const ACCOR_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

export const ACCOR_CONTENT_SOURCE = "accor_hotel_page";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} xml
 */
export function extractSitemapLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

/**
 * @param {string} url
 */
export function accorHotelCodeFromUrl(url) {
  const m = String(url || "").match(/\/hotel\/([0-9A-Za-z]+)\//i);
  return m ? m[1].toUpperCase() : "";
}

/** JSON-LD amenityFeature may be an array or a single object. */
export function accorAmenityFeatures(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === "object") return [value];
  return [];
}

/**
 * @param {string} html
 */
export function parseAccorHotelMetadataFromHtml(html) {
  /** @type {object|null} */
  let hotel = null;
  for (const block of String(html || "").match(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi) || []) {
    const inner = block.replace(/<\/?script[^>]*>/gi, "").trim();
    try {
      const json = JSON.parse(inner);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) {
          hotel = obj;
          break;
        }
      }
    } catch {
      /* skip */
    }
    if (hotel) break;
  }

  if (!hotel) return null;

  const cc = String(hotel.address?.addressCountry || "").trim().toUpperCase();
  const countryLabel = ACCOR_COUNTRY_CODE_TO_LABEL[cc] || hotel.address?.addressCountry || "";

  /** @type {string[]} */
  const amenities = [];
  for (const feat of accorAmenityFeatures(hotel.amenityFeature)) {
    const name = String(feat?.name || "").trim();
    const val = String(feat?.value ?? "true").toLowerCase();
    if (name && val !== "false") amenities.push(name);
  }
  amenities.sort((a, b) => a.localeCompare(b));

  return {
    name: String(hotel.name || "").replace(/\s*-\s*ALL\s*$/i, "").trim(),
    city: String(hotel.address?.addressLocality || "").trim(),
    country: countryLabel,
    countryCode: cc,
    latitude: hotel.geo?.latitude ?? null,
    longitude: hotel.geo?.longitude ?? null,
    amenities,
    amenitiesText: amenities.join("; "),
  };
}

/**
 * @param {object} opts
 * @param {boolean} [opts.calaOnly]
 * @param {boolean} [opts.fetchMetadata]
 * @param {number} [opts.delayMs]
 * @param {number} [opts.maxFetch]
 * @param {typeof fetch} [opts.fetchFn]
 */
export async function extractAccorPropertyUrls(opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const calaOnly = opts.calaOnly !== false;
  const fetchMetadata = opts.fetchMetadata !== false;
  const delayMs = opts.delayMs ?? 150;
  const maxFetch = opts.maxFetch ?? null;

  const smRes = await fetchFn(ACCOR_HOTEL_SITEMAP_EN, { headers: ACCOR_FETCH_HEADERS });
  if (!smRes.ok) {
    return { ok: false, error: `Accor sitemap HTTP ${smRes.status}` };
  }
  const locs = [
    ...new Set(
      extractSitemapLocs(await smRes.text())
        .filter((u) => /\/hotel\/[^/]+\/index\.en\.shtml/i.test(u))
        .map((u) => u.split("?")[0])
    ),
  ];

  /** @type {object[]} */
  const rows = [];
  let fetched = 0;
  let calaCount = 0;

  for (const propertyUrl of locs) {
    if (maxFetch != null && fetched >= maxFetch && fetchMetadata) break;

    const propertyId = accorHotelCodeFromUrl(propertyUrl);
    const base = {
      propertyUrl,
      propertyId,
      source: "accor_sitemap",
      sourceName: "Accor ALL Hotel Sitemap",
      sourceType: "brand_directory",
    };

    if (!fetchMetadata) {
      rows.push({
        ...base,
        inferredHotelName: "",
        city: "",
        country: "",
        calaFilterStatus: "uncertain",
      });
      continue;
    }

    fetched++;
    if (fetched % 250 === 0) {
      console.log(`  [Accor metadata ${fetched}/${locs.length}] CALA rows: ${rows.length}`);
    }
    let pageRes;
    try {
      pageRes = await fetchFn(propertyUrl, {
        headers: ACCOR_FETCH_HEADERS,
        redirect: "follow",
      });
    } catch (err) {
      continue;
    }
    if (!pageRes.ok) continue;
    const meta = parseAccorHotelMetadataFromHtml(await pageRes.text());
    if (!meta?.name) continue;

    const isCala = accorCountryCodeIsCala(meta.countryCode);
    if (calaOnly && !isCala) continue;

    if (isCala) calaCount++;
    rows.push({
      ...base,
      inferredHotelName: meta.name,
      city: meta.city,
      country: meta.country,
      countryCode: meta.countryCode,
      countryNorm: normalizeCountry(meta.country),
      latitude: meta.latitude,
      longitude: meta.longitude,
      amenities: meta.amenities || [],
      amenitiesText: meta.amenitiesText || "",
      calaFilterStatus: isCala ? "included" : "excluded_non_cala",
    });

    if (delayMs > 0) await sleep(delayMs);
  }

  return {
    ok: true,
    sitemapUrl: ACCOR_HOTEL_SITEMAP_EN,
    sitemapLocsTotal: locs.length,
    metadataFetched: fetched,
    propertyRows: rows,
    summary: {
      totalPropertyUrls: rows.length,
      calaIncluded: calaCount,
      calaOnly,
      fetchMetadata,
    },
  };
}
