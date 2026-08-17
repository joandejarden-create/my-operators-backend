/**
 * Marriott CALA country hotel-sitemap discovery for Autopilot source_discovery.
 *
 * Discovery source: official marriott.com /en-us/hotel-sitemap/{country}-hotel-sitemap
 * HQV GraphQL is enrichment-only — never required for listing discovery.
 * Deprecated /en/hotels/{country}.sitemap-hotels.xml must never be used.
 */

import {
  censusCountryToSitemapSlug,
  countrySitemapUrl,
  fetchMarriottCountrySitemapPage,
  marshaFromMarriottWebsite,
  normalizeMarriottDirectoryHotel,
  MARRIOTT_ORIGIN,
  MARRIOTT_SITEMAP_INDEX,
  CENSUS_COUNTRY_TO_SITEMAP_SLUG,
} from "../marriott-brand-directory-extract.js";
import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";
import {
  mapMarriottMexicoBrand,
  inferCityFromMarriottTitle,
} from "./clean-census/marriott-mexico-discovery.js";

export const MARRIOTT_DISCOVERY_ADAPTER_VERSION =
  "census-autopilot-marriott-discovery-adapter-v1";

/** Official listing entrypoints only — never HQV, never deprecated XML. */
export const MARRIOTT_DISCOVERY_SOURCE = Object.freeze({
  type: "official_country_hotel_sitemap",
  origin: MARRIOTT_ORIGIN,
  master_index: MARRIOTT_SITEMAP_INDEX,
  url_template: `${MARRIOTT_ORIGIN}/en-us/hotel-sitemap/{country-slug}-hotel-sitemap`,
  property_url_pattern: `${MARRIOTT_ORIGIN}/en-us/hotels/{MARSHA5}-{slug}/overview`,
  property_id_pattern: "/hotels/([A-Za-z0-9]{5})-",
  hqv_required_for_discovery: false,
  deprecated_blocked_patterns: [
    "/en/hotels/{country}.sitemap-hotels.xml",
    "*.sitemap-hotels.xml",
  ],
});

/**
 * Short country codes for Property Identity Key (ind_marriott_{cc}_{marsha}).
 * Costa Rica = cr, Colombia = co — never use first-two-letter of label.
 */
export const MARRIOTT_DISCOVERY_COUNTRY_SHORT = Object.freeze({
  Mexico: "mx",
  "Dominican Republic": "do",
  "Costa Rica": "cr",
  Colombia: "co",
  Panama: "pa",
  Jamaica: "jm",
  Peru: "pe",
  Brazil: "br",
  Chile: "cl",
  Ecuador: "ec",
  Guatemala: "gt",
  Honduras: "hn",
  "El Salvador": "sv",
  Bahamas: "bs",
  Barbados: "bb",
  "Trinidad and Tobago": "tt",
  "Puerto Rico": "pr",
  Argentina: "ar",
  Aruba: "aw",
  Belize: "bz",
  Bolivia: "bo",
  Uruguay: "uy",
  Paraguay: "py",
  Haiti: "ht",
  Nicaragua: "ni",
  Venezuela: "ve",
  "Cayman Islands": "ky",
  Grenada: "gd",
  Suriname: "sr",
  Guyana: "gy",
  Curacao: "cw",
  "Curaçao": "cw",
});

/** Priority countries live-probed for Autopilot (2026-08-05 code probe). */
export const MARRIOTT_CALA_PRIORITY_COUNTRIES = Object.freeze([
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Colombia",
  "Panama",
]);

/**
 * True when URL is the deprecated Marriott sitemap-hotels.xml pattern (404).
 * @param {string} url
 */
export function isDeprecatedMarriottSitemapHotelsXml(url) {
  return /marriott\.com\/.*\.sitemap-hotels\.xml/i.test(String(url || ""));
}

/**
 * Canonical Census country label from slug or free text.
 * @param {string} countryOrSlug
 */
export function resolveMarriottDiscoveryCountryLabel(countryOrSlug) {
  const raw = String(countryOrSlug || "").trim();
  if (!raw) return "";
  if (COUNTRY_CONFIGS[raw]) return raw;
  const slug = censusCountryToSitemapSlug(raw) || raw.toLowerCase().replace(/\s+/g, "-");
  for (const [label, s] of Object.entries(CENSUS_COUNTRY_TO_SITEMAP_SLUG)) {
    if (s === slug) {
      for (const radar of Object.keys(COUNTRY_CONFIGS || {})) {
        if (radar.toLowerCase() === label) return radar;
      }
      return label.replace(/\b\w/g, (c) => c.toUpperCase());
    }
  }
  for (const radar of Object.keys(COUNTRY_CONFIGS || {})) {
    if (radar.toLowerCase() === raw.toLowerCase()) return radar;
  }
  return raw;
}

/**
 * @param {string} countryLabel
 */
export function marriottDiscoveryCountryShort(countryLabel) {
  const label = resolveMarriottDiscoveryCountryLabel(countryLabel);
  if (MARRIOTT_DISCOVERY_COUNTRY_SHORT[label]) {
    return MARRIOTT_DISCOVERY_COUNTRY_SHORT[label];
  }
  const slug = censusCountryToSitemapSlug(label);
  if (slug === "costa-rica") return "cr";
  if (slug === "colombia") return "co";
  if (slug === "dominican-republic") return "do";
  if (slug === "mexico") return "mx";
  if (slug === "panama") return "pa";
  return String(slug || "xx")
    .replace(/[^a-z]/gi, "")
    .slice(0, 2)
    .toLowerCase() || "xx";
}

/**
 * Readiness for one country — never invents missing sitemap slugs.
 * @param {string} countryLabel
 * @param {{ probedCountries?: string[] }} [opts]
 */
export function classifyMarriottCountryDiscoveryReadiness(countryLabel, opts = {}) {
  const label = resolveMarriottDiscoveryCountryLabel(countryLabel);
  const slug = censusCountryToSitemapSlug(label);
  const probed = new Set(
    (opts.probedCountries || MARRIOTT_CALA_PRIORITY_COUNTRIES).map((c) =>
      resolveMarriottDiscoveryCountryLabel(c)
    )
  );

  if (!slug) {
    return {
      country: label || countryLabel,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "No Marriott hotel-sitemap slug mapped for this country — do not guess URL",
      sitemap_url: null,
      hqv_required: false,
    };
  }

  const sitemapUrl = countrySitemapUrl(slug);
  if (isDeprecatedMarriottSitemapHotelsXml(sitemapUrl)) {
    return {
      country: label,
      readiness: "blocked",
      ready: false,
      adapter: null,
      note: "Resolved URL matched deprecated sitemap-hotels.xml — blocked",
      sitemap_url: sitemapUrl,
      hqv_required: false,
    };
  }

  if (probed.has(label)) {
    return {
      country: label,
      readiness: "supported",
      ready: true,
      adapter: "ensureMarriottCalaCountrySitemapCache",
      note: `Official country hotel-sitemap (live-probed): ${sitemapUrl}`,
      sitemap_url: sitemapUrl,
      hqv_required: false,
    };
  }

  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensureMarriottCalaCountrySitemapCache",
    note: `Official hotel-sitemap URL pattern available (${sitemapUrl}); not re-probed in Autopilot wiring task`,
    sitemap_url: sitemapUrl,
    hqv_required: false,
  };
}

/**
 * Countries to crawl for a discovery run.
 * @param {{ country?: string|null, countries?: string[]|null, regionCountries?: string[] }} [opts]
 */
export function listMarriottDiscoveryCountries(opts = {}) {
  if (opts.country) {
    const label = resolveMarriottDiscoveryCountryLabel(opts.country);
    const ready = classifyMarriottCountryDiscoveryReadiness(label);
    return ready.ready ? [label] : [];
  }
  if (opts.countries?.length) {
    return opts.countries
      .map((c) => resolveMarriottDiscoveryCountryLabel(c))
      .filter((c) => classifyMarriottCountryDiscoveryReadiness(c).ready);
  }
  const region = opts.regionCountries?.length
    ? opts.regionCountries
    : MARRIOTT_CALA_PRIORITY_COUNTRIES;
  return region
    .map((c) => resolveMarriottDiscoveryCountryLabel(c))
    .filter((c) => classifyMarriottCountryDiscoveryReadiness(c).ready);
}

/**
 * Fetch + normalize Marriott hotels for one or more CALA countries.
 * Cache key: `${country}|${marsha}` → row. Also returns byMarsha for single-country lookups.
 *
 * @param {object} [opts]
 * @param {string[]|null} [opts.countries]
 * @param {string|null} [opts.country]
 * @param {Map|null} [opts.cache] inject fixture cache
 * @param {number} [opts.delayMs]
 * @param {boolean} [opts.force]
 */
export async function ensureMarriottCalaCountrySitemapCache(opts = {}) {
  if (opts.cache && !opts.force) return opts.cache;

  const countries = listMarriottDiscoveryCountries(opts);
  /** @type {Map<string, object>} */
  const byKey = new Map();
  const errors = [];
  const countryStats = [];

  for (let i = 0; i < countries.length; i++) {
    const country = countries[i];
    const slug = censusCountryToSitemapSlug(country);
    const url = countrySitemapUrl(slug);
    if (isDeprecatedMarriottSitemapHotelsXml(url)) {
      errors.push({
        family: "Marriott",
        country,
        url,
        error: "deprecated_sitemap_hotels_xml_blocked",
      });
      continue;
    }
    try {
      if ((opts.delayMs ?? 200) > 0 && i > 0) {
        await new Promise((r) => setTimeout(r, opts.delayMs ?? 200));
      }
      const page = await fetchMarriottCountrySitemapPage(url);
      let count = 0;
      for (const hotel of page.hotels) {
        const normalized = normalizeMarriottDirectoryHotel(hotel, {
          sourceUrl: url,
          countrySlug: slug,
          countryLabel: country,
        });
        if (!normalized.marshaCode) continue;
        const brand = mapMarriottMexicoBrand(normalized.name, normalized.website);
        const city =
          String(normalized.city || "").trim() ||
          inferCityFromMarriottTitle(normalized.name, brand) ||
          "";
        const row = {
          ...normalized,
          country,
          city,
          brand,
          affiliation: brand,
          parent: "Marriott",
          marshaCode: normalized.marshaCode,
          propertyId: normalized.marshaCode,
          propertyUrl: normalized.website,
          sourceUrl: url,
          source: "marriott_country_hotel_sitemap",
          discovery_adapter: "marriott_cala_country_sitemap",
          hqv_used: false,
        };
        byKey.set(`${country}|${normalized.marshaCode}`, row);
        // Also index by marsha alone (last country wins if collision — rare cross-border)
        byKey.set(normalized.marshaCode, row);
        count += 1;
      }
      countryStats.push({
        country,
        sitemap_url: url,
        hotel_count: count,
        ok: true,
      });
    } catch (err) {
      errors.push({
        family: "Marriott",
        country,
        url,
        error: err?.message || String(err),
      });
      countryStats.push({
        country,
        sitemap_url: url,
        hotel_count: 0,
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  byKey._meta = {
    version: MARRIOTT_DISCOVERY_ADAPTER_VERSION,
    source: MARRIOTT_DISCOVERY_SOURCE,
    countries,
    country_stats: countryStats,
    errors,
    hqv_required_for_discovery: false,
    loaded_at: new Date().toISOString(),
  };

  return byKey;
}

/**
 * Iterate unique Marriott directory rows (dedupe by marsha+country).
 * @param {Map<string, object>} cache
 */
export function iterateMarriottDirectoryRows(cache) {
  const seen = new Set();
  const rows = [];
  for (const [key, row] of cache.entries()) {
    if (key.startsWith("_")) continue;
    if (!row?.marshaCode) continue;
    const id = `${row.country || ""}|${row.marshaCode}`;
    if (seen.has(id)) continue;
    seen.add(id);
    rows.push(row);
  }
  return rows;
}

/**
 * @param {string} url
 * @param {string} [fallback]
 */
export function extractMarshaFromUrl(url, fallback = "") {
  return marshaFromMarriottWebsite(url) || String(fallback || "").trim().toUpperCase();
}
