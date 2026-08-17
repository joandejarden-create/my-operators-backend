/**
 * Accor CALA discovery for Autopilot source_discovery.
 *
 * Sources (official Accor only):
 * 1. Continent browse JSON-LD ItemList (South / Central / North America)
 * 2. Accor Catalog API hydrate by property ID (country, city, brand, address)
 *
 * Never uses OTAs, old Census, or owner/operator/date fields.
 */

import {
  ACCOR_CONTINENT_PAGES,
  extractAccorContinentHotels,
} from "../accor-continent-directory-extract.js";
import {
  accorCountryNameToCode,
  fetchAccorCatalogByIds,
  fetchAccorCatalogHotels,
} from "../accor-catalog-api.js";
import { ACCOR_COUNTRY_CODE_TO_LABEL } from "../brand-sitemap/cala-url-segments.js";
import { accorCanonicalPropertyUrl } from "../hotel-census/accor-directory-name-normalize.js";
import {
  CALA_DISCOVERY_PRIORITY_COUNTRIES,
  CALA_DISCOVERY_COUNTRY_ISO,
  resolveDiscoveryCountries,
} from "./census-autopilot-cala-discovery-shared.js";

export const ACCOR_DISCOVERY_ADAPTER_VERSION =
  "census-autopilot-accor-cala-discovery-adapter-v1";

export const ACCOR_DISCOVERY_SOURCE = Object.freeze({
  type: "official_accor_continent_browse_plus_catalog",
  continent_url_template:
    "https://all.accor.com/a/en/destination/continent/{continent-slug}.html",
  catalog_api: "https://api.accor.com/catalog/v1/hotels",
  property_url_template: "https://all.accor.com/hotel/{PROPERTY_ID}/index.en.shtml",
  blocked_patterns: [
    "short continent path 403 patterns",
    "OTA listings",
  ],
});

/** Accor catalog brand codes → Census Brand display names (official Accor catalog). */
export const ACCOR_BRAND_CODE_TO_NAME = Object.freeze({
  IBH: "ibis",
  IBB: "ibis",
  IBS: "ibis",
  NOV: "Novotel",
  SOF: "Sofitel",
  MER: "Mercure",
  MGA: "MGallery Collection",
  MSH: "Mama Shelter",
  PUL: "Pullman",
  FAI: "Fairmont Hotels & Resorts",
  DES: "Design Hotels",
  SO: "SO/ Hotels and Resorts",
  TRI: "TRIBE",
  SWI: "Swissôtel",
  ADA: "Adagio",
  MOV: "Mövenpick",
  // Lifestyle / luxury partners — Accor Catalog brand field (CALA verified)
  BAN: "Banyan Tree",
  ANG: "Angsana",
  HYD: "Hyde",
  MOD: "Mondrian",
  SLS: "SLS",
  SOU: "Handwritten Collection",
});

export const ACCOR_CALA_PRIORITY_COUNTRIES = Object.freeze([
  ...CALA_DISCOVERY_PRIORITY_COUNTRIES,
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} code
 */
export function accorBrandNameFromCode(code) {
  const c = String(code || "").trim().toUpperCase();
  return ACCOR_BRAND_CODE_TO_NAME[c] || (c ? c : null);
}

/**
 * @param {string} countryLabel
 */
export function classifyAccorCountryDiscoveryReadiness(countryLabel) {
  const label = String(countryLabel || "").trim();
  const iso = CALA_DISCOVERY_COUNTRY_ISO[label] || accorCountryNameToCode(label);
  if (!iso && !ACCOR_COUNTRY_CODE_TO_LABEL[String(label).toUpperCase()]) {
    // Still allow if label is a known CALA radar country string
    if (!label) {
      return {
        country: label,
        readiness: "needs_adapter",
        ready: false,
        adapter: null,
        note: "Empty Accor country label",
        continent_url: null,
      };
    }
  }
  const priority = ACCOR_CALA_PRIORITY_COUNTRIES.includes(label);
  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensureAccorCalaDirectoryCache",
    note: priority
      ? "Official Accor continent browse + Catalog API hydrate (priority CALA)"
      : "Official Accor continent browse + Catalog API hydrate",
    continent_url:
      "https://all.accor.com/a/en/destination/continent/hotels-central-america-c10.html",
  };
}

/**
 * @param {string[]} ids
 * @param {object} [opts]
 */
async function hydrateAccorIds(ids, opts = {}) {
  const chunkSize = opts.chunkSize ?? 40;
  const delayMs = opts.delayMs ?? 120;
  /** @type {Map<string, object>} */
  const byId = new Map();
  const errors = [];

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    try {
      const res = await fetchAccorCatalogByIds(chunk, opts);
      if (!res.ok) {
        errors.push({ chunk: chunk.slice(0, 5), error: res.error || "catalog_fail" });
      } else {
        for (const h of res.hotels || []) {
          if (h?.propertyId) byId.set(String(h.propertyId).toUpperCase(), h);
        }
      }
    } catch (err) {
      errors.push({ chunk: chunk.slice(0, 5), error: err?.message || String(err) });
    }
    if (delayMs > 0 && i + chunkSize < ids.length) await sleep(delayMs);
  }
  return { byId, errors };
}

/**
 * @param {object} [opts]
 */
export async function ensureAccorCalaDirectoryCache(opts = {}) {
  if (opts.cache) return opts.cache;

  const countriesWanted = resolveDiscoveryCountries(
    opts.country,
    opts.countries || ACCOR_CALA_PRIORITY_COUNTRIES
  ).filter((c) => classifyAccorCountryDiscoveryReadiness(c).ready);

  const wantIso = new Set(
    countriesWanted
      .map((c) => CALA_DISCOVERY_COUNTRY_ISO[c] || accorCountryNameToCode(c))
      .filter(Boolean)
      .map((c) => String(c).toUpperCase())
  );

  /** @type {Map<string, object>} */
  const byKey = new Map();
  const errors = [];
  const countryStats = [];
  const sourceNotes = [];

  // 1) Continent browse — primary listing (avoid short continent path 403s)
  let continentRows = [];
  try {
    const continents = opts.continents || ["southAmerica", "centralAmerica", "northAmerica"];
    for (const key of continents) {
      if (!ACCOR_CONTINENT_PAGES[key]) continue;
      const result = await extractAccorContinentHotels({
        continent: key,
        maxPages: opts.maxContinentPages ?? undefined,
        delayMs: opts.delayMs ?? 120,
        fetchFn: opts.fetchFn,
      });
      continentRows.push(...(result.propertyRows || []));
      sourceNotes.push({
        continent: ACCOR_CONTINENT_PAGES[key].label,
        count: (result.propertyRows || []).length,
      });
    }
  } catch (err) {
    errors.push({ stage: "continent_browse", error: err?.message || String(err) });
  }

  const idSet = new Set(
    continentRows.map((r) => String(r.propertyId || "").toUpperCase()).filter(Boolean)
  );

  // 2) Country catalog queries for priority countries (fills DR/CR gaps if continent misses)
  for (const country of countriesWanted) {
    try {
      const cc = CALA_DISCOVERY_COUNTRY_ISO[country] || accorCountryNameToCode(country);
      const cat = await fetchAccorCatalogHotels(country, {
        countryCode: cc || undefined,
        enlargementAllowed: false,
        fetchFn: opts.fetchFn,
        apiKey: opts.apiKey,
      });
      if (cat.ok) {
        for (const h of cat.hotels || []) {
          if (h?.propertyId) idSet.add(String(h.propertyId).toUpperCase());
        }
        sourceNotes.push({ country_catalog: country, count: cat.count });
      }
    } catch (err) {
      errors.push({ stage: "country_catalog", country, error: err?.message || String(err) });
    }
    if ((opts.delayMs ?? 120) > 0) await sleep(opts.delayMs ?? 120);
  }

  const ids = [...idSet];
  const { byId, errors: hydrateErrors } = await hydrateAccorIds(ids, {
    delayMs: opts.delayMs ?? 120,
    chunkSize: opts.chunkSize ?? 40,
    apiKey: opts.apiKey,
  });
  errors.push(...hydrateErrors);

  const browseById = new Map(
    continentRows.map((r) => [String(r.propertyId || "").toUpperCase(), r])
  );

  const countsByCountry = Object.fromEntries(countriesWanted.map((c) => [c, 0]));

  for (const id of ids) {
    const cat = byId.get(id);
    const browse = browseById.get(id);
    const countryCode = String(cat?.countryCode || "").toUpperCase();
    if (wantIso.size && countryCode && !wantIso.has(countryCode)) continue;

    const countryLabel =
      (countryCode && ACCOR_COUNTRY_CODE_TO_LABEL[countryCode]) ||
      cat?.country ||
      null;
    if (!countryLabel) {
      // Without country, do not auto-insert — skip sparse rows
      continue;
    }
    if (
      countriesWanted.length &&
      !countriesWanted.some((c) => c.toLowerCase() === String(countryLabel).toLowerCase())
    ) {
      // Allow ISO match even if Accor label differs slightly
      if (!wantIso.has(countryCode)) continue;
    }

    const brandCode = String(cat?.brand || "").toUpperCase();
    const brandName = accorBrandNameFromCode(brandCode) || brandCode || null;
    const name = cat?.name || browse?.inferredHotelName || null;
    const propertyUrl =
      cat?.propertyUrl || browse?.propertyUrl || accorCanonicalPropertyUrl(id);

    const row = {
      propertyId: id,
      name,
      brand: brandName,
      brandCode,
      parent: "Accor",
      city: cat?.city || null,
      state: null,
      country: countryLabel,
      countryCode,
      addressLine1: cat?.address1 || null,
      propertyUrl,
      sourceUrl:
        browse?.continentSlug
          ? `https://all.accor.com/a/en/destination/continent/${browse.continentSlug}.html`
          : ACCOR_DISCOVERY_SOURCE.catalog_api,
      latitude: cat?.latitude ?? null,
      longitude: cat?.longitude ?? null,
      source: cat ? "accor_catalog_api" : "accor_continent_browse",
      discovery_adapter: "accor_cala_continent_catalog",
    };

    byKey.set(`${countryLabel}|${id}`, row);
    byKey.set(id, row);
    if (countsByCountry[countryLabel] != null) countsByCountry[countryLabel] += 1;
    else countsByCountry[countryLabel] = 1;
  }

  for (const country of countriesWanted) {
    countryStats.push({
      country,
      hotel_count: countsByCountry[country] || 0,
      ok: true,
      mode: "continent_browse_plus_catalog",
    });
  }

  byKey._meta = {
    version: ACCOR_DISCOVERY_ADAPTER_VERSION,
    source: ACCOR_DISCOVERY_SOURCE,
    countries: countriesWanted,
    country_stats: countryStats,
    source_notes: sourceNotes,
    continent_ids: ids.length,
    hydrated: byId.size,
    errors,
    loaded_at: new Date().toISOString(),
  };
  return byKey;
}

/**
 * @param {Map<string, object>} cache
 */
export function iterateAccorDirectoryRows(cache) {
  const seen = new Set();
  const rows = [];
  for (const [, row] of cache.entries()) {
    if (!row?.propertyId) continue;
    const key = `${row.country || ""}|${row.propertyId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push(row);
  }
  return rows;
}
