/**
 * Small Luxury Hotels of the World (slh.com) → Hotel Census CALA match helpers.
 *
 * Source: public SLH hotel search API used by https://slh.com/explore-hotels
 * Affiliation / Parent Company: Brand Alias Mapping canonical
 *   "Small Luxury Hotels of the World"
 */

import { COUNTRY_CONFIG_LIST } from "./radar-buildout/country-configs.js";
import { CENSUS_FIELDS } from "./hotel-census/fields.js";
import { nameSimilarity, normalizeKey } from "./independent-census/match-current-census.js";

export const SLH_SEARCH_API_URL =
  "https://slh.com/api/slh/hotelsearchresults/gethotelsearchresults";
export const SLH_ORIGIN = "https://slh.com";

/** Exact Affiliation select value (Brand Alias Mapping canonical). */
export const SLH_AFFILIATION = "Small Luxury Hotels of the World";
export const SLH_PARENT_COMPANY = "Small Luxury Hotels of the World";

const CALA_COUNTRY_SET = new Set(COUNTRY_CONFIG_LIST.map((c) => c.toLowerCase()));

/** SLH location country labels → Dealality census country. */
export const SLH_COUNTRY_ALIASES = {
  mexico: "Mexico",
  "costa rica": "Costa Rica",
  panama: "Panama",
  belize: "Belize",
  guatemala: "Guatemala",
  honduras: "Honduras",
  "el salvador": "El Salvador",
  nicaragua: "Nicaragua",
  colombia: "Colombia",
  ecuador: "Ecuador",
  peru: "Peru",
  chile: "Chile",
  argentina: "Argentina",
  brazil: "Brazil",
  uruguay: "Uruguay",
  paraguay: "Paraguay",
  bolivia: "Bolivia",
  venezuela: "Venezuela",
  jamaica: "Jamaica",
  bahamas: "Bahamas",
  barbados: "Barbados",
  "dominican republic": "Dominican Republic",
  "puerto rico": "Puerto Rico",
  grenada: "Grenada",
  "cayman islands": "Cayman Islands",
  "turks & caicos": "Turks & Caicos",
  "turks and caicos": "Turks & Caicos",
  "st kitts & nevis": "Saint Kitts and Nevis",
  "st. kitts & nevis": "Saint Kitts and Nevis",
  "saint kitts & nevis": "Saint Kitts and Nevis",
  "saint kitts and nevis": "Saint Kitts and Nevis",
  "st vincent & the grenadines": "Saint Vincent and the Grenadines",
  "st. vincent & the grenadines": "Saint Vincent and the Grenadines",
  "saint vincent & the grenadines": "Saint Vincent and the Grenadines",
  "saint vincent and the grenadines": "Saint Vincent and the Grenadines",
  "antigua and barbuda": "Antigua and Barbuda",
  aruba: "Aruba",
  curacao: "Curaçao",
  "curaçao": "Curaçao",
  cuba: "Cuba",
  haiti: "Haiti",
  "trinidad and tobago": "Trinidad and Tobago",
  "british virgin islands": "British Virgin Islands",
  "us virgin islands": "U.S. Virgin Islands",
  "u.s. virgin islands": "U.S. Virgin Islands",
  martinique: "Martinique",
  guadeloupe: "Guadeloupe",
  suriname: "Suriname",
  guyana: "Guyana",
};

/** Do not overwrite these hard / competing affiliations with SLH. */
export const SLH_PROTECTED_AFFILIATIONS = new Set([
  "Design Hotels",
  "Autograph Collection",
  "Tribute Portfolio",
  "Luxury Collection",
  "W Hotels",
  "Westin",
  "Le Meridien",
  "St. Regis",
  "Ritz-Carlton",
  "Edition",
  "JW Marriott",
  "Marriott Hotels",
  "Four Seasons",
  "Rosewood",
  "Aman",
  "Mandarin Oriental",
  "Peninsula",
  "One&Only",
  "Six Senses",
  "Belmond",
  "Relais & Châteaux",
]);

export function isCalaCountry(country) {
  return CALA_COUNTRY_SET.has(String(country || "").trim().toLowerCase());
}

export function normalizeSlhName(value) {
  return normalizeKey(value)
    .replace(/\b(hotel|hotels|resort|spa|boutique|&|and|the|de|del|la|los|las)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} locationName e.g. "Tulum, Mexico"
 */
export function parseSlhLocation(locationName) {
  const raw = String(locationName || "").trim();
  const parts = raw
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length >= 2) {
    return { city: parts.slice(0, -1).join(", "), countryRaw: parts[parts.length - 1] };
  }
  return { city: "", countryRaw: raw };
}

/**
 * @param {string} countryRaw
 */
export function mapSlhCountryToCensus(countryRaw) {
  const key = String(countryRaw || "")
    .trim()
    .toLowerCase();
  if (!key) return "";
  if (SLH_COUNTRY_ALIASES[key]) return SLH_COUNTRY_ALIASES[key];
  const hit = COUNTRY_CONFIG_LIST.find((c) => c.toLowerCase() === key);
  return hit || "";
}

/**
 * @param {object} item SLH API hotel item
 */
export function normalizeSlhHotelItem(item) {
  const detailUrl = String(item?.detailUrl || "").trim();
  const slug = detailUrl.replace(/^\/hotels\//i, "").replace(/\/$/, "");
  const { city, countryRaw } = parseSlhLocation(item?.location?.name);
  const censusCountry = mapSlhCountryToCensus(countryRaw);
  const keyFeatures = Array.isArray(item?.keyFeatures)
    ? item.keyFeatures
        .map((f) => String(f?.label || "").trim())
        .filter(Boolean)
    : [];
  return {
    id: String(item?.id || "").trim(),
    title: String(item?.title || "").trim(),
    detailUrl,
    slug,
    propertyUrl: detailUrl ? `${SLH_ORIGIN}${detailUrl}` : "",
    city,
    countryRaw,
    censusCountry,
    lat: item?.location?.coordinates?.lat ?? null,
    lng: item?.location?.coordinates?.lng ?? null,
    description: String(item?.descriptionText || item?.shortDescriptionText || "").trim(),
    keyFeatures,
    isCala: Boolean(censusCountry) && isCalaCountry(censusCountry),
  };
}

/**
 * @param {string[]} labels
 */
export function formatSlhAmenitiesText(labels) {
  const seen = new Set();
  /** @type {string[]} */
  const out = [];
  for (const raw of labels || []) {
    const label = String(raw || "").replace(/\s+/g, " ").trim();
    if (!label) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out.join("; ");
}

/**
 * Fetch full SLH directory (API returns all items on pageIndex=0).
 * @param {{ country?: string, onProgress?: (msg: string) => void }} [opts]
 */
export async function fetchSlhHotels(opts = {}) {
  const params = new URLSearchParams({
    pageIndex: "0",
    sort: "descRelevance",
    viewType: "list",
  });
  if (opts.country) params.set("country", opts.country);

  opts.onProgress?.(`Fetching ${SLH_SEARCH_API_URL}`);
  const res = await fetch(`${SLH_SEARCH_API_URL}?${params}`, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
      Accept: "application/json",
      Referer: "https://slh.com/explore-hotels",
    },
  });
  if (!res.ok) throw new Error(`SLH search HTTP ${res.status}`);
  const json = await res.json();
  const items = Array.isArray(json.items) ? json.items : [];
  return {
    totalResults: json.totalResults ?? items.length,
    hotels: items.map(normalizeSlhHotelItem),
  };
}

/**
 * @param {{ onProgress?: (msg: string) => void }} [opts]
 */
export async function fetchSlhCalaProperties(opts = {}) {
  const { hotels, totalResults } = await fetchSlhHotels(opts);
  const cala = hotels.filter((h) => h.isCala && h.title && h.propertyUrl);
  opts.onProgress?.(`SLH global ${totalResults}; CALA ${cala.length}`);
  return cala;
}

/**
 * @param {ReturnType<typeof normalizeSlhHotelItem>} source
 * @param {import('airtable').Record} census
 */
export function scoreSlhCensusMatch(source, census) {
  const censusName = String(census.fields.name || "");
  const censusUrl = String(census.fields.Website || "").toLowerCase();
  const srcUrl = String(source.propertyUrl || "").toLowerCase();
  const slug = String(source.slug || "").toLowerCase();

  if (censusUrl && srcUrl && (censusUrl === srcUrl || censusUrl.includes(`/hotels/${slug}`))) {
    return { score: 100, reason: "website_exact" };
  }
  if (censusUrl && slug && censusUrl.includes(slug)) {
    return { score: 98, reason: "website_slug" };
  }

  const srcNorm = normalizeSlhName(source.title);
  const censusNorm = normalizeSlhName(censusName);
  if (srcNorm && censusNorm && srcNorm === censusNorm) {
    return { score: 95, reason: "name_exact" };
  }

  const sim = nameSimilarity(source.title, censusName);
  const city = String(census.fields[CENSUS_FIELDS.city] || "").trim();
  const cityHit =
    source.city && city
      ? normalizeKey(city).includes(normalizeKey(source.city)) ||
        normalizeKey(source.city).includes(normalizeKey(city))
      : null;

  if (sim >= 0.85) {
    return { score: Math.round(90 + sim * 5), reason: cityHit ? "name_high_city" : "name_high" };
  }
  if (sim >= 0.65 && cityHit) {
    return { score: Math.round(80 + sim * 10), reason: "name_medium_city" };
  }
  if (sim >= 0.72) {
    return { score: Math.round(75 + sim * 10), reason: "name_medium" };
  }
  if (sim >= 0.55 && cityHit) {
    return { score: Math.round(65 + sim * 10), reason: "name_low_city" };
  }
  return { score: 0, reason: "none" };
}

/**
 * @param {import('airtable').Record[]} censusRows
 * @param {ReturnType<typeof normalizeSlhHotelItem>[]} sourceProperties
 * @param {{ minScore?: number, stewardReviewMaxScore?: number }} [opts]
 */
export function planSlhAffiliationUpdates(censusRows, sourceProperties, opts = {}) {
  const minScore = opts.minScore ?? 80;
  const stewardMax = opts.stewardReviewMaxScore ?? 79;

  /** @type {Map<string, object>} */
  const byRecordId = new Map();
  /** @type {object[]} */
  const stewardReview = [];
  /** @type {object[]} */
  const unmatchedSources = [];

  for (const source of sourceProperties) {
    /** @type {{ rec: import('airtable').Record, score: number, reason: string } | null} */
    let best = null;
    /** @type {{ rec: import('airtable').Record, score: number, reason: string } | null} */
    let second = null;

    for (const rec of censusRows) {
      if (rec.fields[CENSUS_FIELDS.country] !== source.censusCountry) continue;
      const { score, reason } = scoreSlhCensusMatch(source, rec);
      if (score <= 0) continue;
      const hit = { rec, score, reason };
      if (!best || score > best.score) {
        second = best;
        best = hit;
      } else if (!second || score > second.score) {
        second = hit;
      }
    }

    if (!best) {
      unmatchedSources.push(source);
      continue;
    }

    const margin = second ? best.score - second.score : 100;
    if (best.score < minScore) {
      if (best.score >= 60 && best.score <= stewardMax) {
        stewardReview.push({
          censusRecordId: best.rec.id,
          censusName: best.rec.fields.name,
          censusCountry: best.rec.fields[CENSUS_FIELDS.country],
          currentAffiliation: best.rec.fields[CENSUS_FIELDS.affiliation] || "",
          propertyUrl: source.propertyUrl,
          slug: source.slug,
          sourceTitle: source.title,
          matchScore: best.score,
          matchReason: best.reason,
          margin,
          reviewReason: "below_auto_apply_threshold",
        });
      } else {
        unmatchedSources.push(source);
      }
      continue;
    }

    if (margin < 5 && second && second.score >= minScore - 5) {
      stewardReview.push({
        censusRecordId: best.rec.id,
        censusName: best.rec.fields.name,
        censusCountry: best.rec.fields[CENSUS_FIELDS.country],
        currentAffiliation: best.rec.fields[CENSUS_FIELDS.affiliation] || "",
        propertyUrl: source.propertyUrl,
        slug: source.slug,
        sourceTitle: source.title,
        matchScore: best.score,
        matchReason: best.reason,
        secondCensusName: second.rec.fields.name,
        secondScore: second.score,
        margin,
        reviewReason: "ambiguous_match_margin",
      });
      continue;
    }

    const existing = byRecordId.get(best.rec.id);
    if (existing && existing.matchScore >= best.score) continue;

    byRecordId.set(best.rec.id, {
      censusRecordId: best.rec.id,
      censusName: best.rec.fields.name,
      censusCountry: best.rec.fields[CENSUS_FIELDS.country],
      currentAffiliation: best.rec.fields[CENSUS_FIELDS.affiliation] || "",
      currentParentCompany: best.rec.fields[CENSUS_FIELDS.parentCompany] || "",
      currentWebsite: best.rec.fields.Website || "",
      propertyUrl: source.propertyUrl,
      slug: source.slug,
      sourceTitle: source.title,
      sourceId: source.id,
      matchScore: best.score,
      matchReason: best.reason,
      margin,
    });
  }

  return {
    matches: [...byRecordId.values()],
    stewardReview,
    unmatchedSources,
  };
}

/**
 * @param {object} row plan row
 * @param {{ fillWebsite?: boolean, fillParent?: boolean }} [opts]
 */
export function buildSlhCensusPatch(row, opts = {}) {
  const fillWebsite = opts.fillWebsite !== false;
  const fillParent = opts.fillParent !== false;
  /** @type {Record<string, string>} */
  const fields = {};

  const currentAff = String(row.currentAffiliation || "").trim();
  if (SLH_PROTECTED_AFFILIATIONS.has(currentAff)) {
    return { fields, blocked: true, blockReason: "protected_affiliation" };
  }
  if (currentAff !== SLH_AFFILIATION) {
    fields[CENSUS_FIELDS.affiliation] = SLH_AFFILIATION;
  }
  if (fillParent && !String(row.currentParentCompany || "").trim()) {
    fields[CENSUS_FIELDS.parentCompany] = SLH_PARENT_COMPANY;
  }
  if (fillWebsite && row.propertyUrl && !String(row.currentWebsite || "").trim()) {
    fields.Website = row.propertyUrl;
  }
  return { fields, blocked: false };
}
