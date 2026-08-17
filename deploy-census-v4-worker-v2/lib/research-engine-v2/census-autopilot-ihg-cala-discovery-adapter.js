/**
 * IHG CALA destination directory discovery for Autopilot source_discovery.
 * Official: https://www.ihg.com/destinations/us/en/{country-slug}-hotels
 */

import {
  extractIhgCalaDestinationDirectory,
  ihgDestinationUrlForCountry,
  IHG_CALA_DESTINATION_SLUGS,
  IHG_CONTENT_SOURCE,
} from "../ihg-brand-directory-extract.js";
import {
  CALA_DISCOVERY_PRIORITY_COUNTRIES,
  resolveDiscoveryCountries,
} from "./census-autopilot-cala-discovery-shared.js";

export const IHG_CALA_DISCOVERY_VERSION = "census-autopilot-ihg-cala-discovery-v1";

export const IHG_DISCOVERY_SOURCE = Object.freeze({
  type: "official_ihg_destination_directory",
  url_template: "https://www.ihg.com/destinations/us/en/{country-slug}-hotels",
  secondary: "https://www.ihg.com/bin/sitemapindex.xml → hoteldetail XMLs",
  property_id_pattern: "/([A-Za-z0-9]{4,6})/hoteldetail",
  content_source: IHG_CONTENT_SOURCE,
});

/**
 * @param {string} countryLabel
 */
export function classifyIhgCountryDiscoveryReadiness(countryLabel) {
  const label = String(countryLabel || "").trim();
  const url = ihgDestinationUrlForCountry(label);
  if (!url || (!IHG_CALA_DESTINATION_SLUGS[label] && !label)) {
    // slugify still produces a URL — allow if destination URL builds
  }
  if (!url) {
    return {
      country: label,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "No IHG destination slug/URL for this country",
      destination_url: null,
    };
  }
  const priority = CALA_DISCOVERY_PRIORITY_COUNTRIES.includes(label);
  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensureIhgCalaDestinationCache",
    note: priority
      ? `Official destination directory (priority): ${url}`
      : `Official destination URL pattern available: ${url}`,
    destination_url: url,
  };
}

/**
 * @param {object} [opts]
 */
export async function ensureIhgCalaDestinationCache(opts = {}) {
  if (opts.cache) return opts.cache;

  const countries = resolveDiscoveryCountries(
    opts.country,
    opts.countries || CALA_DISCOVERY_PRIORITY_COUNTRIES
  ).filter((c) => classifyIhgCountryDiscoveryReadiness(c).ready);

  /** @type {Map<string, object>} */
  const byKey = new Map();
  const result = await extractIhgCalaDestinationDirectory({
    countries,
    delayMs: opts.delayMs ?? 200,
  });

  for (const row of result.propertyRows || []) {
    const id = String(row.propertyId || row.mnemonic || "").toUpperCase();
    if (!id) continue;
    const country = row.country || "";
    const normalized = {
      ...row,
      propertyId: id,
      mnemonic: id,
      name: row.name || row.inferredHotelName || null,
      brand: row.brand || null,
      parent: "IHG",
      country,
      city: row.city || null,
      addressLine1: row.addressText || null,
      propertyUrl: row.propertyUrl || row.website || null,
      sourceUrl: row.sourceUrl || "",
      source: IHG_CONTENT_SOURCE,
      discovery_adapter: "ihg_cala_destination_directory",
    };
    byKey.set(`${country}|${id}`, normalized);
    byKey.set(id, normalized);
  }

  byKey._meta = {
    version: IHG_CALA_DISCOVERY_VERSION,
    source: IHG_DISCOVERY_SOURCE,
    countries,
    page_results: result.pageResults || [],
    fetch_errors: result.fetchErrors || [],
    hotels_found: byKey.size / 2,
    loaded_at: new Date().toISOString(),
  };
  return byKey;
}

/**
 * @param {Map<string, object>} cache
 */
export function iterateIhgDirectoryRows(cache) {
  const seen = new Set();
  const rows = [];
  for (const [, row] of cache.entries()) {
    if (!row?.propertyId) continue;
    const id = `${row.country || ""}|${row.propertyId}`;
    if (seen.has(id)) continue;
    seen.add(id);
    rows.push(row);
  }
  return rows;
}
