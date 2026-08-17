/**
 * Wyndham property directory from wyndhamhotels.com sitemaps (verified URLs).
 */

import { wyndhamUrlLooksCala, ACCOR_COUNTRY_CODE_TO_LABEL } from "./brand-sitemap/cala-url-segments.js";
import { SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL } from "./marriott-brand-directory-extract.js";
import { normalizeCountry } from "./independent-census/match-current-census.js";

/** Map JSON-LD addressCountry (name or code) to census country label when possible */
const WYNDHAM_COUNTRY_ALIASES = {
  ...ACCOR_COUNTRY_CODE_TO_LABEL,
  Mexico: "Mexico",
  Brazil: "Brazil",
  "Dominican Republic": "Dominican Republic",
  "Puerto Rico": "Puerto Rico",
  "Costa Rica": "Costa Rica",
  Colombia: "Colombia",
  Jamaica: "Jamaica",
  Panama: "Panama",
  Argentina: "Argentina",
  Chile: "Chile",
  Peru: "Peru",
  Ecuador: "Ecuador",
  Guatemala: "Guatemala",
  Honduras: "Honduras",
  "El Salvador": "El Salvador",
  Uruguay: "Uruguay",
  Paraguay: "Paraguay",
  Bolivia: "Bolivia",
  Venezuela: "Venezuela",
  Bahamas: "Bahamas",
  Barbados: "Barbados",
  Aruba: "Aruba",
  Curaçao: "Curacao",
  Curacao: "Curacao",
  Bermuda: "Bermuda",
  Haiti: "Haiti",
  Belize: "Belize",
};

const CALA_COUNTRY_NORMS = new Set(
  Object.values(WYNDHAM_COUNTRY_ALIASES).map((c) => normalizeCountry(c)).filter(Boolean)
);

export const WYNDHAM_ORIGIN = "https://www.wyndhamhotels.com";
export const WYNDHAM_SITEMAP_INDEX = `${WYNDHAM_ORIGIN}/sitemap.xml`;

export const WYNDHAM_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "application/xml,text/xml,text/html,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

/**
 * @param {string} html
 */
export function parseWyndhamHotelMetadataFromHtml(html) {
  for (const block of String(html || "").match(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  ) || []) {
    const inner = block.replace(/<\/?script[^>]*>/gi, "").trim();
    try {
      const json = JSON.parse(inner);
      const arr = Array.isArray(json) ? json : json?.["@graph"] ? json["@graph"] : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;
        const cc = String(obj.address?.addressCountry || "").trim();
        const countryLabel =
          WYNDHAM_COUNTRY_ALIASES[cc] ||
          (cc.length === 2 ? ACCOR_COUNTRY_CODE_TO_LABEL[cc.toUpperCase()] : cc) ||
          "";
        return {
          name: String(obj.name || "").trim(),
          city: String(obj.address?.addressLocality || "").trim(),
          country: countryLabel,
          countryNorm: normalizeCountry(countryLabel),
          latitude: obj.geo?.latitude ?? null,
          longitude: obj.geo?.longitude ?? null,
          identifier: String(obj.identifier || "").trim(),
          description: String(obj.description || "").trim(),
          url: String(obj.url || "").trim(),
        };
      }
    } catch {
      /* skip */
    }
  }
  return null;
}

const US_STATE_SUFFIXES = new Set([
  "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
  "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
  "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
  "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
  "new-hampshire", "new-jersey", "new-mexico", "new-york", "north-carolina", "north-dakota",
  "ohio", "oklahoma", "oregon", "pennsylvania", "rhode-island", "south-carolina",
  "south-dakota", "tennessee", "texas", "utah", "vermont", "virginia", "washington",
  "west-virginia", "wisconsin", "wyoming", "district-of-columbia",
]);

function wyndhamCountryIsCala(countryNorm) {
  return Boolean(countryNorm && CALA_COUNTRY_NORMS.has(countryNorm));
}

function regionSlugLooksUsState(regionSlug) {
  const parts = String(regionSlug || "").toLowerCase().split("-");
  if (parts.length < 2) return false;
  const suffix = parts.slice(-2).join("-");
  if (US_STATE_SUFFIXES.has(suffix)) return true;
  return US_STATE_SUFFIXES.has(parts[parts.length - 1]);
}

function wyndhamUrlCandidateForMetadata(regionSlug, propertyUrl) {
  if (regionSlugLooksUsState(regionSlug)) return false;
  if (wyndhamUrlLooksCala(propertyUrl)) return true;
  const lower = String(propertyUrl || "").toLowerCase();
  const calaHints =
    /puerto-rico|san-juan|santo-domingo|punta-cana|cancun|playa-del-carmen|riviera-maya|los-cabos|mexico-city|guadalajara|monterrey|bogota|medellin|cartagena|buenos-aires|rio-de-janeiro|sao-paulo|lima-peru|san-jose-costa|panama-city-panama|nassau|freeport|barbados|aruba|curacao|jamaica|montego-bay|kingston-jamaica|santiago-chile|quito|guayaquil|guatemala-city|san-salvador|tegucigalpa|belize-city|georgetown-guyana|port-of-spain|trinidad-tobago|bermuda|cayman|turks|caicos|caracas|maracaibo|montevideo|asuncion|la-paz|santa-cruz-bolivia/i;
  return calaHints.test(lower);
}

/**
 * @param {string} xml
 */
export function extractSitemapLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

function titleFromSlug(slug) {
  return String(slug || "")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

/**
 * @param {string} url
 */
export function parseWyndhamPropertyUrl(url) {
  try {
    const u = new URL(url);
    if (!u.hostname.includes("wyndhamhotels.com")) return null;
    const segments = u.pathname.split("/").filter(Boolean);
    if (segments.length < 4) return null;
    const page = segments[segments.length - 1]?.toLowerCase();
    if (page !== "overview") return null;

    const brandSlug = segments[0];
    const regionSlug = segments[1];
    const propertySlug = segments[2];
    const brandName = titleFromSlug(brandSlug.replace(/-/g, " "));
    const inferredCountry = SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL[regionSlug.toLowerCase()] || "";

    return {
      propertyUrl: u.href.split("?")[0],
      brandSlug,
      brandName,
      regionSlug,
      citySlug: propertySlug,
      propertySlug,
      inferredHotelName: `${brandName} ${titleFromSlug(propertySlug)}`.trim(),
      inferredCountry,
      source: "wyndham_sitemap",
    };
  } catch {
    return null;
  }
}

/**
 * @param {object} opts
 * @param {boolean} [opts.calaOnly]
 * @param {boolean} [opts.fetchMetadata]
 * @param {number} [opts.delayMs]
 * @param {number} [opts.maxProperties]
 * @param {number} [opts.maxMetadataFetch]
 * @param {typeof fetch} [opts.fetchFn]
 */
export async function extractWyndhamPropertyUrls(opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const calaOnly = opts.calaOnly !== false;
  const fetchMetadata = opts.fetchMetadata !== false;
  const delayMs = opts.delayMs ?? 100;
  const maxProperties = opts.maxProperties ?? null;
  const maxMetadataFetch = opts.maxMetadataFetch ?? null;

  let indexRes = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    indexRes = await fetchFn(WYNDHAM_SITEMAP_INDEX, { headers: WYNDHAM_FETCH_HEADERS });
    if (indexRes.ok) break;
    await sleep(2000 * (attempt + 1));
  }
  if (!indexRes?.ok) {
    return { ok: false, error: `Wyndham sitemap index HTTP ${indexRes?.status || "unknown"}` };
  }
  const indexXml = await indexRes.text();
  const childSitemaps = extractSitemapLocs(indexXml).filter((u) => /properties/i.test(u));

  /** @type {object[]} */
  const candidateUrls = [];
  const seen = new Set();

  for (const childUrl of childSitemaps) {
    if (maxProperties != null && candidateUrls.length >= maxProperties) break;

    const childRes = await fetchFn(childUrl, { headers: WYNDHAM_FETCH_HEADERS });
    if (!childRes.ok) continue;
    const childXml = await childRes.text();
    const locs = extractSitemapLocs(childXml).filter((u) => /\/overview\/?$/i.test(u));

    for (const loc of locs) {
      if (maxProperties != null && candidateUrls.length >= maxProperties) break;
      if (seen.has(loc)) continue;
      seen.add(loc);
      candidateUrls.push(loc);
    }

    if (delayMs > 0) await sleep(delayMs);
  }

  /** @type {object[]} */
  const rows = [];
  let metadataFetched = 0;

  for (const loc of candidateUrls) {
    if (maxMetadataFetch != null && metadataFetched >= maxMetadataFetch) break;

    const parsed = parseWyndhamPropertyUrl(loc);
    if (!parsed) continue;

    if (fetchMetadata && !wyndhamUrlCandidateForMetadata(parsed.regionSlug, loc)) {
      continue;
    }

    const base = {
      ...parsed,
      propertyId: parsed.propertySlug.slice(0, 20).toUpperCase(),
      sourceName: "Wyndham Hotels Sitemap",
      sourceType: "brand_directory",
    };

    if (!fetchMetadata) {
      rows.push({ ...base, calaFilterStatus: "uncertain" });
      continue;
    }

    metadataFetched++;
    const pageRes = await fetchFn(parsed.propertyUrl, { headers: WYNDHAM_FETCH_HEADERS });
    if (!pageRes.ok) continue;
    const meta = parseWyndhamHotelMetadataFromHtml(await pageRes.text());
    if (!meta?.name) continue;

    const isCala = wyndhamCountryIsCala(meta.countryNorm);
    if (calaOnly && !isCala) continue;

    rows.push({
      ...base,
      inferredHotelName: meta.name,
      city: meta.city || base.citySlug.replace(/-/g, " "),
      country: meta.country,
      countryNorm: meta.countryNorm,
      latitude: meta.latitude,
      longitude: meta.longitude,
      identifier: meta.identifier || null,
      calaFilterStatus: isCala ? "included" : "excluded_non_cala",
    });

    if (delayMs > 0) await sleep(delayMs);
  }

  return {
    ok: true,
    sitemapIndexUrl: WYNDHAM_SITEMAP_INDEX,
    childSitemapsScanned: childSitemaps.length,
    candidateOverviewUrls: candidateUrls.length,
    metadataFetched,
    propertyRows: rows,
    summary: {
      totalPropertyUrls: rows.length,
      calaOnly,
      fetchMetadata,
    },
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
