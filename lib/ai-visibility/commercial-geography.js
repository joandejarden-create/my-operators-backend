/**
 * AI Visibility commercial-region mapping (Phase 2D).
 *
 * Distinct from Radar subregions in COUNTRY_CONFIGS.
 * - CALA countries: reuse COUNTRY_CONFIGS keys (product commercial region = CALA)
 * - Europe / North America: minimal governed country lists for MVP country-level prompts
 * - Unknown countries remain unknown (no silent assignment)
 *
 * Headline regional metrics use region-scope prompts only (not country rollup).
 */

import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";

export const COMMERCIAL_GEOGRAPHY_VERSION = "ai_visibility_commercial_geography_v1";

export const COMMERCIAL_REGIONS = Object.freeze([
  "CALA",
  "Europe",
  "North America",
  "APAC",
  "Middle East & Africa",
]);

/**
 * Durable headline cohort rule (BUILD_DECISIONS):
 * Headline CALA/Europe/North America metrics use geographyScope=region only.
 * Country prompts stay in country cohorts. Do not auto-mix into headline Presence.
 */
export const HEADLINE_REGION_METRIC_COHORT_RULE =
  "region_scope_prompts_only_no_country_rollup";

/** Europe MVP country list (display names). Country-level support only — no invented subregions. */
export const EUROPE_COUNTRIES = Object.freeze([
  "Spain",
  "France",
  "United Kingdom",
  "Germany",
  "Italy",
  "Portugal",
  "Netherlands",
  "Ireland",
  "Switzerland",
  "Austria",
  "Belgium",
  "Greece",
]);

/** North America commercial region (US + Canada). Mexico remains CALA. */
export const NORTH_AMERICA_COUNTRIES = Object.freeze(["United States", "Canada"]);

/** Optional ISO alpha-2 for seed countries. */
export const COUNTRY_CODES = Object.freeze({
  Mexico: "MX",
  Spain: "ES",
  France: "FR",
  "United Kingdom": "GB",
  Germany: "DE",
  Italy: "IT",
  Portugal: "PT",
  Netherlands: "NL",
  Ireland: "IE",
  Switzerland: "CH",
  Austria: "AT",
  Belgium: "BE",
  Greece: "GR",
  "United States": "US",
  Canada: "CA",
});

function buildCalACountrySet() {
  return new Set(Object.keys(COUNTRY_CONFIGS));
}

/**
 * Resolve commercial region for a country display name.
 * @returns {{
 *   countryName: string|null,
 *   commercialRegion: string|null,
 *   radarSubregion: string|null,
 *   countryCode: string|null,
 *   known: boolean,
 *   source: string
 * }}
 */
export function resolveCommercialRegionForCountry(countryName) {
  const raw = String(countryName || "").trim();
  if (!raw) {
    return {
      countryName: null,
      commercialRegion: null,
      radarSubregion: null,
      countryCode: null,
      known: false,
      source: "empty",
    };
  }

  const cala = buildCalACountrySet();
  const calaHit = [...cala].find((k) => k.toLowerCase() === raw.toLowerCase());
  if (calaHit) {
    return {
      countryName: calaHit,
      commercialRegion: "CALA",
      radarSubregion: COUNTRY_CONFIGS[calaHit]?.region || null,
      countryCode: COUNTRY_CODES[calaHit] || null,
      known: true,
      source: "COUNTRY_CONFIGS→CALA",
    };
  }

  const euHit = EUROPE_COUNTRIES.find((k) => k.toLowerCase() === raw.toLowerCase());
  if (euHit) {
    return {
      countryName: euHit,
      commercialRegion: "Europe",
      radarSubregion: null,
      countryCode: COUNTRY_CODES[euHit] || null,
      known: true,
      source: "ai_visibility_europe_mvp",
    };
  }

  const naHit = NORTH_AMERICA_COUNTRIES.find((k) => k.toLowerCase() === raw.toLowerCase());
  if (naHit) {
    return {
      countryName: naHit,
      commercialRegion: "North America",
      radarSubregion: null,
      countryCode: COUNTRY_CODES[naHit] || null,
      known: true,
      source: "ai_visibility_north_america_mvp",
    };
  }

  return {
    countryName: raw,
    commercialRegion: null,
    radarSubregion: null,
    countryCode: null,
    known: false,
    source: "unknown",
  };
}

/**
 * Validate country ↔ commercial region consistency for a prompt row.
 */
export function validateCountryRegionPair(countryName, commercialRegion) {
  if (!countryName) return { ok: true };
  const resolved = resolveCommercialRegionForCountry(countryName);
  if (!resolved.known) {
    return {
      ok: false,
      error: `unknown_country:${countryName}`,
      resolved,
    };
  }
  if (
    commercialRegion &&
    String(commercialRegion).toLowerCase() !== String(resolved.commercialRegion).toLowerCase()
  ) {
    return {
      ok: false,
      error: `country_region_mismatch:${countryName}→${resolved.commercialRegion} vs ${commercialRegion}`,
      resolved,
    };
  }
  return { ok: true, resolved };
}

export function auditCommercialGeography() {
  return {
    COMMERCIAL_GEOGRAPHY_VERSION,
    COMMERCIAL_REGIONS_SUPPORTED: COMMERCIAL_REGIONS,
    GLOBAL_SCOPE: "explicit geographyScope=Global only (not average of regions)",
    CALA: {
      source: "COUNTRY_CONFIGS keys",
      countryCount: Object.keys(COUNTRY_CONFIGS).length,
    },
    EUROPE: {
      source: "ai_visibility_europe_mvp",
      countries: [...EUROPE_COUNTRIES],
    },
    NORTH_AMERICA: {
      source: "ai_visibility_north_america_mvp",
      countries: [...NORTH_AMERICA_COUNTRIES],
      note: "Mexico remains CALA",
    },
    COUNTRY_MAPPING_SOURCE:
      "CALA←COUNTRY_CONFIGS; Europe/NA←AI Visibility commercial geography v1",
    HEADLINE_REGION_METRIC_COHORT_RULE,
  };
}
