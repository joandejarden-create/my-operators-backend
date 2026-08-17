/**
 * Resolve Hilton amenityIds from city-level locations pages by ctyhocn.
 */

import {
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
  hiltonLocationsUrl,
} from "../hilton-brand-directory-extract.js";
import { loadHiltonBrandDirectoryConfigs, affiliationHintsForBrand } from "../hilton-brand-registry.js";
import { normalizeKey, normalizeText } from "../independent-census/match-current-census.js";
import { formatAmenitiesText } from "../hilton-amenity-map.js";

const COUNTRY_SLUGS = {
  mexico: "mexico",
  "dominican republic": "dominican-republic",
  "costa rica": "costa-rica",
  panama: "panama",
  bahamas: "bahamas",
  jamaica: "jamaica",
  "trinidad and tobago": "trinidad-and-tobago",
  colombia: "colombia",
  argentina: "argentina",
  brazil: "brazil",
  chile: "chile",
  peru: "peru",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function citySlug(city) {
  return normalizeKey(city)
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function countrySlug(country) {
  const k = normalizeKey(country);
  return COUNTRY_SLUGS[k] || k.replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function resolveBrandConfig(affiliation, configs) {
  const aff = normalizeText(affiliation);
  for (const cfg of configs) {
    const hints = affiliationHintsForBrand(cfg);
    if (hints.some((h) => aff === h || aff.includes(h) || h.includes(aff))) return cfg;
  }
  return null;
}

/**
 * @param {string} ctyhocn
 * @param {{ affiliation: string, city: string, country: string }} censusRow
 * @param {Map<string, object[]>} cityCache
 * @param {object[]} brandConfigs
 */
export async function fetchAmenitiesTextByCtyhocnFromCityPage(
  ctyhocn,
  censusRow,
  cityCache,
  brandConfigs
) {
  const code = String(ctyhocn || "").trim().toUpperCase();
  if (!code) return "";

  const brandCfg = resolveBrandConfig(censusRow.affiliation, brandConfigs);
  if (!brandCfg) return "";

  const cSlug = countrySlug(censusRow.country);
  const ciSlug = citySlug(censusRow.city);
  if (!cSlug || !ciSlug) return "";

  const pageUrl = hiltonLocationsUrl(`locations/${cSlug}/${ciSlug}/${brandCfg.locationsSlug}/`);
  let directoryHotels = cityCache.get(pageUrl);
  if (!directoryHotels) {
    try {
      const page = await fetchHiltonLocationsPage(pageUrl);
      directoryHotels = extractHotelsFromPageData(page.pageData)
        .filter((h) => String(h?.brandCode || "").trim() === brandCfg.brandCode)
        .map((h) => normalizeHiltonDirectoryHotel(h, { sourceUrl: pageUrl }));
      cityCache.set(pageUrl, directoryHotels);
    } catch {
      cityCache.set(pageUrl, []);
      return "";
    }
  }

  const hit = directoryHotels.find((d) => String(d.ctyhocn || "").toUpperCase() === code);
  if (!hit?.amenityIds?.length) return "";
  return formatAmenitiesText(hit.amenityIds);
}

/**
 * @param {object[]} rows — { ctyhocn, affiliation, city, country }
 * @param {object} [opts]
 */
export async function buildCityPageAmenityFallbackIndex(rows, opts = {}) {
  const brandConfigs = await loadHiltonBrandDirectoryConfigs();
  /** @type {Map<string, string>} */
  const byCode = new Map();
  /** @type {Map<string, object[]>} */
  const cityCache = new Map();

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const code = String(row.ctyhocn || "").trim().toUpperCase();
    if (!code || byCode.has(code)) continue;
    if (opts.onProgress) opts.onProgress(`[city ${i + 1}/${rows.length}] ${code}`);
    const text = await fetchAmenitiesTextByCtyhocnFromCityPage(code, row, cityCache, brandConfigs);
    if (text) byCode.set(code, text);
    if (opts.pageDelayMs) await sleep(opts.pageDelayMs);
  }

  return byCode;
}
