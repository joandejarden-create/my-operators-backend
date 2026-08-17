/**
 * Wyndham CALA discovery for Autopilot source_discovery.
 *
 * Source: official wyndhamhotels.com property sitemaps + property-page JSON-LD.
 * CALA filter uses page country / addressCountry metadata — never path keywords alone
 * (e.g. panama-city-florida must not count as Panama).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  extractWyndhamPropertyUrls,
  WYNDHAM_ORIGIN,
  WYNDHAM_SITEMAP_INDEX,
} from "../wyndham-brand-directory-extract.js";
import {
  CALA_DISCOVERY_PRIORITY_COUNTRIES,
  resolveDiscoveryCountries,
} from "./census-autopilot-cala-discovery-shared.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");
export const WYNDHAM_DEFAULT_SEED_REPORT = path.join(
  ROOT,
  "reports/wyndham-property-directory-extract.json"
);

export const WYNDHAM_DISCOVERY_ADAPTER_VERSION =
  "census-autopilot-wyndham-cala-discovery-adapter-v1";

export const WYNDHAM_DISCOVERY_SOURCE = Object.freeze({
  type: "official_wyndham_property_sitemap",
  origin: WYNDHAM_ORIGIN,
  sitemap_index: WYNDHAM_SITEMAP_INDEX,
  property_url_pattern: `${WYNDHAM_ORIGIN}/{brand}/{region}/{property}/overview`,
  cala_filter: "json_ld_addressCountry_metadata",
  blocked_patterns: [
    "path_keyword_cala_guesses",
    "OTA listings",
  ],
});

/** Wyndham URL brand slug → Brand Setup display name (Active/Live where mapped). */
export const WYNDHAM_BRAND_SLUG_TO_NAME = Object.freeze({
  "trademark-collection": "Trademark Collection by Wyndham",
  trademark: "Trademark Collection by Wyndham",
  "registry-collection": "Registry Collection",
  dazzler: "Dazzler by Wyndham",
  "dazzler-by-wyndham": "Dazzler by Wyndham",
  "woodspring-suites": "WoodSpring Suites",
  woodspring: "WoodSpring Suites",
  "everhome-suites": "Everhome Suites",
  everhome: "Everhome Suites",
  laquinta: "La Quinta by Wyndham",
  "la-quinta": "La Quinta by Wyndham",
  wyndham: "Wyndham Hotels & Resorts",
  "wyndham-garden": "Wyndham Garden",
  "wyndham-grand": "Wyndham Grand",
  "wyndham-alltra": "Wyndham Alltra",
  ramada: "Ramada",
  "days-inn": "Days Inn",
  daysinn: "Days Inn",
  microtel: "Microtel by Wyndham",
  tryp: "Tryp by Wyndham",
  "super-8": "Super 8",
  travelodge: "Travelodge",
  "howard-johnson": "Howard Johnson",
  "es-xl": "Esplendor by Wyndham",
  esplendor: "Esplendor by Wyndham",
});

/** Locale path segments that are not brand slugs (es-xl is a brand, not a locale). */
export const WYNDHAM_LOCALE_PATH_SEGMENTS = Object.freeze(
  new Set(["pt-br", "en-us", "es-mx", "es"])
);

/**
 * @param {string} brandSlug
 */
export function wyndhamBrandNameFromSlug(brandSlug) {
  const s = String(brandSlug || "").trim().toLowerCase();
  if (!s) return null;
  if (Object.prototype.hasOwnProperty.call(WYNDHAM_BRAND_SLUG_TO_NAME, s)) {
    return WYNDHAM_BRAND_SLUG_TO_NAME[s] || null;
  }
  // title-case fallback from slug
  return s
    .split("-")
    .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
    .join(" ");
}

/**
 * Extract brand slug from Wyndham property URL (skip locale segments).
 * @param {string} url
 */
export function wyndhamBrandSlugFromUrl(url) {
  const m = String(url || "")
    .toLowerCase()
    .match(/wyndhamhotels\.com\/([a-z0-9-]+)(?:\/([a-z0-9-]+))?/i);
  if (!m) return null;
  let slug = m[1];
  // pt-br / es are locales when followed by a real brand slug
  if ((slug === "pt-br" || slug === "en-us" || slug === "es-mx" || slug === "es") && m[2]) {
    slug = m[2];
  }
  return slug;
}

export const WYNDHAM_CALA_PRIORITY_COUNTRIES = Object.freeze([
  ...CALA_DISCOVERY_PRIORITY_COUNTRIES,
]);

/**
 * @param {string} countryLabel
 */
export function classifyWyndhamCountryDiscoveryReadiness(countryLabel) {
  const label = String(countryLabel || "").trim();
  if (!label) {
    return {
      country: label,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "Empty Wyndham country label",
      sitemap_url: null,
    };
  }
  const priority = WYNDHAM_CALA_PRIORITY_COUNTRIES.includes(label);
  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensureWyndhamCalaDirectoryCache",
    note: priority
      ? "Official Wyndham property sitemap + JSON-LD country filter (priority CALA)"
      : "Official Wyndham property sitemap + JSON-LD country filter",
    sitemap_url: WYNDHAM_SITEMAP_INDEX,
  };
}

/**
 * Stable official-ish property code from Wyndham row.
 * Prefer JSON-LD identifier when present; else property slug.
 * @param {object} row
 */
export function wyndhamPropertyCode(row) {
  const id = String(row.identifier || row.officialPropertyId || "").trim();
  if (id) return id.toUpperCase().replace(/[^A-Z0-9_-]/g, "").slice(0, 32);
  const slug = String(row.propertySlug || row.citySlug || "").trim().toLowerCase();
  if (slug) return slug.replace(/[^a-z0-9-]/g, "").slice(0, 48);
  return "";
}

/**
 * @param {object} [opts]
 */
export async function ensureWyndhamCalaDirectoryCache(opts = {}) {
  if (opts.cache) return opts.cache;

  const countriesWanted = resolveDiscoveryCountries(
    opts.country,
    opts.countries || WYNDHAM_CALA_PRIORITY_COUNTRIES
  ).filter((c) => classifyWyndhamCountryDiscoveryReadiness(c).ready);
  const wantNorm = new Set(countriesWanted.map((c) => c.toLowerCase()));

  /** @type {Map<string, object>} */
  const byKey = new Map();
  const errors = [];
  const countryStats = [];

  let extract;
  let seedUsed = false;
  const seedPath = opts.seedFromReportPath || WYNDHAM_DEFAULT_SEED_REPORT;

  try {
    if (!opts.forceLiveExtract && fs.existsSync(seedPath)) {
      const seeded = JSON.parse(fs.readFileSync(seedPath, "utf8"));
      if (seeded?.ok && Array.isArray(seeded.propertyRows) && seeded.propertyRows.length) {
        extract = seeded;
        seedUsed = true;
      }
    }
    if (!extract) {
      extract = await extractWyndhamPropertyUrls({
        calaOnly: true,
        fetchMetadata: opts.fetchMetadata !== false,
        delayMs: opts.delayMs ?? 80,
        maxProperties: opts.maxProperties ?? null,
        maxMetadataFetch: opts.maxMetadataFetch ?? null,
        fetchFn: opts.fetchFn,
      });
    }
  } catch (err) {
    errors.push({ stage: "wyndham_sitemap", error: err?.message || String(err) });
    extract = { ok: false, propertyRows: [], error: err?.message || String(err) };
  }

  if (!extract?.ok) {
    errors.push({ stage: "wyndham_sitemap", error: extract?.error || "extract_failed" });
  }

  const countsByCountry = Object.fromEntries(countriesWanted.map((c) => [c, 0]));

  for (const raw of extract.propertyRows || []) {
    const country = String(raw.country || "").trim();
    if (!country) continue;
    if (wantNorm.size && !wantNorm.has(country.toLowerCase())) continue;
    if (raw.calaFilterStatus === "excluded_non_cala") continue;

    const brandSlug = String(raw.brandSlug || "").toLowerCase();
    const brand = wyndhamBrandNameFromSlug(brandSlug) || raw.brandName || null;
    const code = wyndhamPropertyCode(raw);
    if (!code) continue;

    const row = {
      propertyId: code,
      identifier: raw.identifier || null,
      propertySlug: raw.propertySlug || raw.citySlug || null,
      name: raw.inferredHotelName || raw.name || null,
      brand,
      brandSlug,
      parent: "Wyndham",
      city: raw.city || null,
      state: null,
      country,
      countryNorm: raw.countryNorm || null,
      addressLine1: raw.address || null,
      propertyUrl: raw.propertyUrl || null,
      sourceUrl: WYNDHAM_SITEMAP_INDEX,
      latitude: raw.latitude ?? null,
      longitude: raw.longitude ?? null,
      source: "wyndham_sitemap",
      discovery_adapter: "wyndham_cala_property_sitemap",
      calaFilterStatus: raw.calaFilterStatus || "included",
    };

    byKey.set(`${country}|${code}`, row);
    byKey.set(code, row);
    if (countsByCountry[country] != null) countsByCountry[country] += 1;
    else countsByCountry[country] = 1;
  }

  for (const country of countriesWanted) {
    countryStats.push({
      country,
      hotel_count: countsByCountry[country] || 0,
      ok: true,
      mode: "sitemap_jsonld_country_filter",
    });
  }

  byKey._meta = {
    version: WYNDHAM_DISCOVERY_ADAPTER_VERSION,
    source: WYNDHAM_DISCOVERY_SOURCE,
    countries: countriesWanted,
    country_stats: countryStats,
    extract_ok: Boolean(extract?.ok),
    seed_used: seedUsed,
    seed_path: seedUsed ? seedPath : null,
    candidate_overview_urls: extract?.candidateOverviewUrls ?? null,
    metadata_fetched: extract?.metadataFetched ?? null,
    errors,
    loaded_at: new Date().toISOString(),
  };
  return byKey;
}

/**
 * @param {Map<string, object>} cache
 */
export function iterateWyndhamDirectoryRows(cache) {
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
