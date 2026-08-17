/**
 * Hilton CALA locations discovery — country pages + Mexico brand pages.
 * Official: https://www.hilton.com/en/locations/{country-slug}/
 * Mexico keeps proven brand subpage crawl; non-Mexico uses country pages.
 */

import { censusCountryToSitemapSlug } from "../marriott-brand-directory-extract.js";
import {
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
} from "../hilton-brand-directory-extract.js";
import {
  loadHiltonBrandDirectoryConfigs,
  HILTON_BRAND_CENSUS_AFFILIATION_HINTS,
} from "../hilton-brand-registry.js";
import {
  CALA_DISCOVERY_PRIORITY_COUNTRIES,
  CALA_DISCOVERY_COUNTRY_ISO,
  resolveDiscoveryCountries,
} from "./census-autopilot-cala-discovery-shared.js";
import { ensureHiltonMexicoDirectoryCache } from "./census-autopilot-family-directory-adapters.js";

export const HILTON_CALA_DISCOVERY_VERSION = "census-autopilot-hilton-cala-discovery-v1";

export const HILTON_DISCOVERY_SOURCE = Object.freeze({
  type: "official_hilton_locations_directory",
  country_url_template: "https://www.hilton.com/en/locations/{country-slug}/",
  mexico_brand_url_template: "https://www.hilton.com/en/locations/mexico/{brand-slug}/",
  property_id_field: "ctyhocn",
});

/**
 * @param {string} countryLabel
 */
export function classifyHiltonCountryDiscoveryReadiness(countryLabel) {
  const label = String(countryLabel || "").trim();
  const slug = censusCountryToSitemapSlug(label);
  if (!slug) {
    return {
      country: label,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "No Hilton locations slug mapped for this country",
      locations_url: null,
    };
  }
  const url = `https://www.hilton.com/en/locations/${slug}/`;
  if (label === "Mexico") {
    return {
      country: label,
      readiness: "supported",
      ready: true,
      adapter: "ensureHiltonCalaDirectoryCache",
      note: "Mexico brand location pages (proven Autopilot path)",
      locations_url: url,
    };
  }
  if (CALA_DISCOVERY_PRIORITY_COUNTRIES.includes(label)) {
    return {
      country: label,
      readiness: "supported",
      ready: true,
      adapter: "ensureHiltonCalaDirectoryCache",
      note: `Official country locations page: ${url}`,
      locations_url: url,
    };
  }
  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensureHiltonCalaDirectoryCache",
    note: `Official locations URL pattern available (${url}); not priority-probed`,
    locations_url: url,
  };
}

function affiliationFromBrandCode(brandCode, configs = []) {
  const code = String(brandCode || "").trim().toUpperCase();
  const cfg = configs.find((c) => c.brandCode === code);
  if (cfg?.canonicalBrandName) return cfg.canonicalBrandName;
  const hints = HILTON_BRAND_CENSUS_AFFILIATION_HINTS[code];
  return hints?.[0] || code || null;
}

function countryMatches(row, countryLabel) {
  const iso = CALA_DISCOVERY_COUNTRY_ISO[countryLabel];
  const cc = String(row.countryCode || "").toUpperCase();
  if (iso && cc && cc === iso) return true;
  const name = String(row.country || "").toLowerCase();
  const want = String(countryLabel || "").toLowerCase();
  if (name && want && (name === want || name.includes(want) || want.includes(name))) return true;
  // Country page crawl — accept when countryPage meta matches
  if (String(row.countryPage || "").toLowerCase() === want) return true;
  return !cc && !name; // allow sparse rows from country page if no conflicting country
}

/**
 * @param {object} [opts]
 * @param {string[]|null} [opts.countries]
 * @param {string|null} [opts.country]
 * @param {Map|null} [opts.cache]
 * @param {number} [opts.delayMs]
 */
export async function ensureHiltonCalaDirectoryCache(opts = {}) {
  if (opts.cache) return opts.cache;

  const countries = resolveDiscoveryCountries(opts.country, opts.countries || CALA_DISCOVERY_PRIORITY_COUNTRIES)
    .filter((c) => classifyHiltonCountryDiscoveryReadiness(c).ready);

  /** @type {Map<string, object>} */
  const byKey = new Map();
  const errors = [];
  const countryStats = [];
  let configs = [];
  try {
    configs = await loadHiltonBrandDirectoryConfigs();
  } catch (err) {
    errors.push({ family: "Hilton", error: `brand_registry: ${err?.message || err}` });
  }

  const delayMs = opts.delayMs ?? 120;

  if (countries.includes("Mexico")) {
    try {
      const mx = await ensureHiltonMexicoDirectoryCache({ delayMs, force: opts.force });
      let n = 0;
      for (const row of mx.values()) {
        const cty = String(row.ctyhocn || "").toUpperCase();
        if (!cty) continue;
        byKey.set(`Mexico|${cty}`, { ...row, country: row.country || "Mexico" });
        byKey.set(cty, byKey.get(`Mexico|${cty}`));
        n += 1;
      }
      countryStats.push({ country: "Mexico", hotel_count: n, ok: true, mode: "brand_locations" });
    } catch (err) {
      errors.push({ family: "Hilton", country: "Mexico", error: err?.message || String(err) });
      countryStats.push({ country: "Mexico", hotel_count: 0, ok: false, error: err?.message || String(err) });
    }
  }

  for (const country of countries.filter((c) => c !== "Mexico")) {
    const slug = censusCountryToSitemapSlug(country);
    const url = `https://www.hilton.com/en/locations/${slug}/`;
    try {
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
      const page = await fetchHiltonLocationsPage(url);
      const hotels = extractHotelsFromPageData(page.pageData);
      let n = 0;
      for (const hotel of hotels) {
        const normalized = normalizeHiltonDirectoryHotel(hotel, {
          sourceUrl: url,
          countryPage: country,
        });
        if (!normalized.ctyhocn) continue;
        if (!countryMatches({ ...normalized, countryPage: country }, country)) {
          // Still keep if ISO matches expected for this page
          const iso = CALA_DISCOVERY_COUNTRY_ISO[country];
          if (iso && String(normalized.countryCode || "").toUpperCase() !== iso) continue;
        }
        const affiliation = affiliationFromBrandCode(normalized.brandCode, configs);
        const row = {
          ...normalized,
          country: country,
          affiliation,
          brand: affiliation,
          parent: "Hilton",
          propertyUrl: normalized.website || null,
          discovery_adapter: "hilton_cala_country_locations",
        };
        byKey.set(`${country}|${normalized.ctyhocn}`, row);
        byKey.set(normalized.ctyhocn, row);
        n += 1;
      }
      countryStats.push({ country, sitemap_url: url, hotel_count: n, ok: true, mode: "country_locations" });
    } catch (err) {
      errors.push({ family: "Hilton", country, url, error: err?.message || String(err) });
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
    version: HILTON_CALA_DISCOVERY_VERSION,
    source: HILTON_DISCOVERY_SOURCE,
    countries,
    country_stats: countryStats,
    errors,
    loaded_at: new Date().toISOString(),
  };
  return byKey;
}

/**
 * @param {Map<string, object>} cache
 */
export function iterateHiltonDirectoryRows(cache) {
  const seen = new Set();
  const rows = [];
  for (const [key, row] of cache.entries()) {
    if (!row?.ctyhocn) continue;
    const id = `${row.country || ""}|${row.ctyhocn}`;
    if (seen.has(id)) continue;
    seen.add(id);
    rows.push(row);
  }
  return rows;
}
