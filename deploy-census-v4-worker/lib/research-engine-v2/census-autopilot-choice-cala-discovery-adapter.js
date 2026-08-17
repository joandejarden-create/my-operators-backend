/**
 * Choice CALA regional directory discovery (multi-country).
 * Official: https://www.choicehotels.com/en-uk/{country}/regional-hotels
 * Address Source URL must remain property-level — regional URL is discovery only.
 */

import {
  buildChoiceRegionalPageForCountry,
  CHOICE_FETCH_HEADERS,
  CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX,
  CHOICE_CALA_COUNTRIES_WITH_REGIONAL,
  CHOICE_SITEMAP_ONLY_COUNTRIES,
  parseChoiceRegionalHotelsFromHtml,
  canonicalChoicePropertyUrl,
} from "../choice-regional-directory-extract.js";
import { extractChoiceRegionalHotelCards } from "./clean-census/choice-mexico-discovery.js";
import {
  CALA_DISCOVERY_PRIORITY_COUNTRIES,
  resolveDiscoveryCountries,
} from "./census-autopilot-cala-discovery-shared.js";
import {
  inferChoiceBrandFromOfficialPropertyUrl,
  isParentCompanyAsCurrentBrand,
} from "./census-autopilot-v3/current-affiliation.js";

export const CHOICE_CALA_DISCOVERY_VERSION = "census-autopilot-choice-cala-discovery-v1";

export const CHOICE_DISCOVERY_SOURCE = Object.freeze({
  type: "official_choice_regional_directory",
  url_template: "https://www.choicehotels.com/en-uk/{country-slug}/regional-hotels",
  property_id_pattern: "[A-Z]{2}\\d{3}",
  note: "Regional URL for discovery only; Census Address Source URL must be property-level",
});

/**
 * @param {string} countryLabel
 */
export function classifyChoiceCountryDiscoveryReadiness(countryLabel) {
  const label = String(countryLabel || "").trim();
  if (CHOICE_SITEMAP_ONLY_COUNTRIES.includes(label)) {
    return {
      country: label,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "Regional Hotel JSON empty — sitemap-only country",
      regional_url: null,
    };
  }
  if (!CHOICE_CALA_COUNTRIES_WITH_REGIONAL.includes(label) && !CALA_DISCOVERY_PRIORITY_COUNTRIES.includes(label)) {
    const page = buildChoiceRegionalPageForCountry(label);
    if (!page) {
      return {
        country: label,
        readiness: "needs_adapter",
        ready: false,
        adapter: null,
        note: "No Choice regional slug mapped",
        regional_url: null,
      };
    }
  }
  const page = buildChoiceRegionalPageForCountry(label);
  if (!page) {
    return {
      country: label,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "No Choice regional slug mapped",
      regional_url: null,
    };
  }
  const priority = CALA_DISCOVERY_PRIORITY_COUNTRIES.includes(label);
  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensureChoiceCalaRegionalCache",
    note: priority
      ? `Official regional page (priority): ${page.url}`
      : `Official regional page pattern available: ${page.url}`,
    regional_url: page.url,
  };
}

/**
 * @param {object} [opts]
 */
export async function ensureChoiceCalaRegionalCache(opts = {}) {
  if (opts.cache) return opts.cache;

  const countries = resolveDiscoveryCountries(
    opts.country,
    opts.countries || CALA_DISCOVERY_PRIORITY_COUNTRIES
  ).filter((c) => classifyChoiceCountryDiscoveryReadiness(c).ready);

  /** @type {Map<string, object>} */
  const byKey = new Map();
  const errors = [];
  const countryStats = [];
  const timeoutMs = opts.timeoutMs || 60000;

  for (const country of countries) {
    const page = buildChoiceRegionalPageForCountry(country);
    if (!page) continue;
    const prefix = String(CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX[country] || "").toUpperCase();
    try {
      const res = await fetch(page.url, {
        headers: CHOICE_FETCH_HEADERS,
        redirect: "follow",
        signal: AbortSignal.timeout(timeoutMs),
      });
      const html = await res.text();
      if (!res.ok || /access denied|robot check/i.test(html)) {
        errors.push({
          family: "Choice",
          country,
          url: page.url,
          error: !res.ok ? `http_${res.status}` : "blocked",
        });
        countryStats.push({ country, url: page.url, hotel_count: 0, ok: false });
        continue;
      }
      const ld = parseChoiceRegionalHotelsFromHtml(html);
      const cards = extractChoiceRegionalHotelCards(html);
      const cardById = new Map(cards.map((c) => [c.propertyId, c]));
      let n = 0;
      for (const h of ld) {
        const id = String(h.propertyId || "").toUpperCase();
        if (!id) continue;
        if (prefix && !id.startsWith(prefix)) continue;
        const card = cardById.get(id) || {};
        const propertyUrl = canonicalChoicePropertyUrl(h.propertyUrl);
        // Guard: never treat regional placeId URL as property URL
        if (/regional-hotels/i.test(propertyUrl) && /placeId=/i.test(propertyUrl)) continue;
        const brandFromCard = String(card.brandName || "").trim() || null;
        const brandFromUrl = inferChoiceBrandFromOfficialPropertyUrl(propertyUrl);
        const brand =
          (brandFromCard && !isParentCompanyAsCurrentBrand(brandFromCard)
            ? brandFromCard
            : null) || brandFromUrl;
        const row = {
          propertyId: id,
          name: h.name,
          propertyUrl,
          brand,
          brandCode: card.brandCode || null,
          brandName: brandFromCard,
          parent: "Choice Hotels International",
          city: card.city || null,
          state: card.state || null,
          country,
          addressLine1: card.addressLine1 || null,
          addressLine2: card.addressLine2 || null,
          postalCode: card.postalCode || null,
          latitude: card.latitude ?? null,
          longitude: card.longitude ?? null,
          amenityGroupLabels: card.amenityGroupLabels || [],
          source_url: page.url,
          source: "choice_regional_jsonld_card",
          discovery_adapter: "choice_cala_regional",
          narrative_description: null,
          narrative_description_supported: false,
        };
        byKey.set(`${country}|${id}`, row);
        byKey.set(id, row);
        n += 1;
      }
      for (const card of cards) {
        const id = String(card.propertyId || "").toUpperCase();
        if (!id || byKey.has(`${country}|${id}`)) continue;
        if (prefix && !id.startsWith(prefix)) continue;
        const brandFromCard = String(card.brandName || "").trim() || null;
        const propertyUrl = card.propertyUrl
          ? canonicalChoicePropertyUrl(card.propertyUrl)
          : null;
        const brandFromUrl = inferChoiceBrandFromOfficialPropertyUrl(propertyUrl);
        const brand =
          (brandFromCard && !isParentCompanyAsCurrentBrand(brandFromCard)
            ? brandFromCard
            : null) || brandFromUrl;
        const row = {
          propertyId: id,
          name: null,
          propertyUrl,
          brand,
          brandCode: card.brandCode || null,
          brandName: brandFromCard,
          parent: "Choice Hotels International",
          city: card.city || null,
          state: card.state || null,
          country,
          addressLine1: card.addressLine1 || null,
          addressLine2: card.addressLine2 || null,
          postalCode: card.postalCode || null,
          latitude: card.latitude ?? null,
          longitude: card.longitude ?? null,
          amenityGroupLabels: card.amenityGroupLabels || [],
          source_url: page.url,
          source: "choice_regional_hotel_card",
          discovery_adapter: "choice_cala_regional",
          narrative_description: null,
          narrative_description_supported: false,
        };
        byKey.set(`${country}|${id}`, row);
        byKey.set(id, row);
        n += 1;
      }
      countryStats.push({ country, url: page.url, hotel_count: n, ok: n > 0 });
    } catch (err) {
      errors.push({ family: "Choice", country, url: page.url, error: err?.message || String(err) });
      countryStats.push({ country, url: page.url, hotel_count: 0, ok: false, error: err?.message || String(err) });
    }
  }

  byKey._meta = {
    version: CHOICE_CALA_DISCOVERY_VERSION,
    source: CHOICE_DISCOVERY_SOURCE,
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
export function iterateChoiceDirectoryRows(cache) {
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
