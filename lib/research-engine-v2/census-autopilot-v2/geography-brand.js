/**
 * Geography + brand-family coverage helpers for Autopilot V2.
 */

import { CVENT_LATAM_CARIBBEAN_COUNTRIES } from "../census-cvent-latam-country-registry.js";
import { BRAND_FAMILY_ADAPTER } from "./constants.js";

const REGION_TO_SUBCONTINENT = Object.freeze({
  Caribbean: "Caribbean",
  "Central America": "Central America",
  "South America": "South America",
  "North America": "North America (Mexico/LATAM ops)",
});

/**
 * @param {string} country
 */
export function assignDealalityGeoLite(country) {
  const hit = CVENT_LATAM_CARIBBEAN_COUNTRIES.find(
    (c) => c.country === country || (c.aliases || []).includes(country)
  );
  const region = hit?.region || "Unknown";
  return {
    Continent: "Americas",
    "Sub-Continent": REGION_TO_SUBCONTINENT[region] || "Unknown",
    Country: country || null,
    "State / Region": null,
    Market: null,
    Submarket: null,
    City: null,
    geo_rule: "dealality-lite-v2-from-country-registry",
    unmapped: !hit,
  };
}

/**
 * @param {object[]} classifiedRows
 */
export function summarizeCountryCoverage(classifiedRows) {
  /** @type {Map<string, number>} */
  const byCountry = new Map();
  for (const r of classifiedRows) {
    const c = r.origin_country || "Unknown";
    byCountry.set(c, (byCountry.get(c) || 0) + 1);
  }
  const ranked = [...byCountry.entries()]
    .map(([country, count]) => ({ country, count, ...assignDealalityGeoLite(country) }))
    .sort((a, b) => b.count - a.count);
  return {
    countries_represented: ranked.length,
    by_country: ranked,
    top_20: ranked.slice(0, 20),
    unmapped_countries: ranked.filter((x) => x.unmapped).map((x) => x.country),
  };
}

/**
 * @param {object[]} classifiedRows
 */
export function summarizeBrandFamilyCoverage(classifiedRows) {
  /** @type {Record<string, number>} */
  const counts = {};
  for (const r of classifiedRows) {
    const f = r.brand_family_inferred || "Unknown";
    counts[f] = (counts[f] || 0) + 1;
  }
  const families = Object.entries(counts)
    .map(([family, count]) => ({
      family,
      count,
      adapter_class: BRAND_FAMILY_ADAPTER[family] || "LONG_TAIL_INDEPENDENT",
    }))
    .sort((a, b) => b.count - a.count);

  const branded = classifiedRows.filter((r) => r.brand_family_inferred && r.brand_family_inferred !== "Independent").length;
  const independent = classifiedRows.length - branded;

  return {
    branded_count: branded,
    independent_count: independent,
    families,
    native_strong: families.filter((f) => f.adapter_class === "NATIVE_STRONG"),
    native_partial: families.filter((f) => f.adapter_class === "NATIVE_PARTIAL"),
    no_adapter: families.filter((f) => f.adapter_class === "NO_ADAPTER"),
    long_tail: families.filter((f) => f.adapter_class === "LONG_TAIL_INDEPENDENT"),
  };
}
