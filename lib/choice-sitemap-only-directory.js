/**
 * Sitemap-only Choice directory rows for countries without regional Hotel JSON.
 */

import { readFileSync } from "node:fs";
import { deriveInferredHotelName } from "./independent-census/match-brand-directory-properties.js";
import {
  CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX,
  canonicalChoicePropertyUrl,
} from "./choice-regional-directory-extract.js";

export { CHOICE_SITEMAP_ONLY_COUNTRIES } from "./choice-regional-directory-extract.js";

const DEFAULT_SITEMAP_EXTRACT =
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";

/**
 * @param {string} countryLabel
 * @param {string} [extractPath]
 */
export function loadChoiceSitemapDirectoryForCountry(
  countryLabel,
  extractPath = DEFAULT_SITEMAP_EXTRACT
) {
  const prefix = CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX[countryLabel]?.toUpperCase();
  if (!prefix) return [];

  let extract;
  try {
    extract = JSON.parse(readFileSync(extractPath, "utf8"));
  } catch {
    return [];
  }

  const rows = Array.isArray(extract.propertyRows) ? extract.propertyRows : [];
  /** @type {object[]} */
  const hotels = [];

  for (const row of rows) {
    const propertyId = String(row.propertyId || "").toUpperCase();
    if (!propertyId.startsWith(prefix)) continue;

    const propertyUrl = canonicalChoicePropertyUrl(row.propertyUrl || "");
    if (!propertyUrl) continue;

    const name = deriveInferredHotelName(row);
    hotels.push({
      propertyId,
      name,
      propertyUrl,
      citySlug: row.citySlug || "",
      source: "choice_sitemap_only",
      regionalCountry: countryLabel,
      sitemapRow: row,
    });
  }

  return hotels;
}
