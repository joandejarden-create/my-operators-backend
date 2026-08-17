/**
 * Handwritten Collection (Accor brand SOU) → Hotel Census CALA enrichment.
 *
 * Sources:
 * - Accor Catalog API brand=SOU (authoritative open directory)
 * - Official property pages e.g. https://all.accor.com/hotel/C280/index.en.shtml
 *
 * Affiliation / Parent Company: Brand Alias Mapping canonical
 *   "Handwritten Collection" / "AccorHotels"
 */

import { COUNTRY_CONFIG_LIST } from "./radar-buildout/country-configs.js";
import { fetchAccorCatalogHotels, fetchAccorCatalogByIds } from "./accor-catalog-api.js";
import { fetchAccorHotelAmenities } from "./accor-hotel-content-fetch.js";
import { CENSUS_FIELDS } from "./hotel-census/fields.js";
import { nameSimilarity, normalizeKey } from "./independent-census/match-current-census.js";
import { isBlankCensusValue } from "./hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "./hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hotel-census/hilton-property-id-contract.js";

export const HANDWRITTEN_AFFILIATION = "Handwritten Collection";
export const HANDWRITTEN_PARENT_COMPANY = "AccorHotels";
/** Accor catalog brand code for Handwritten Collection (confirmed via C280). */
export const HANDWRITTEN_BRAND_CODE = "SOU";

const CALA_SET = new Set(COUNTRY_CONFIG_LIST.map((c) => c.toLowerCase()));

export function isCalaCountry(country) {
  return CALA_SET.has(String(country || "").trim().toLowerCase());
}

/**
 * @param {object} [opts]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function fetchHandwrittenCalaCatalog(opts = {}) {
  /** @type {Map<string, object>} */
  const byId = new Map();

  for (const country of COUNTRY_CONFIG_LIST) {
    opts.onProgress?.(`Catalog ${country} brand=${HANDWRITTEN_BRAND_CODE}…`);
    const res = await fetchAccorCatalogHotels(country, {
      brand: HANDWRITTEN_BRAND_CODE,
      enlargementAllowed: false,
    });
    if (!res.ok) continue;
    for (const h of res.hotels || []) {
      if (String(h.brand || "").toUpperCase() !== HANDWRITTEN_BRAND_CODE) continue;
      byId.set(h.propertyId, {
        ...h,
        censusCountry:
          COUNTRY_CONFIG_LIST.find((c) => c.toLowerCase() === String(h.country || "").toLowerCase()) ||
          country,
      });
    }
  }

  return [...byId.values()];
}

/**
 * @param {object} source catalog hotel
 * @param {import('airtable').Record} census
 */
export function scoreHandwrittenCensusMatch(source, census) {
  const censusId = String(census.fields[CENSUS_PROPERTY_ID_FIELD] || "")
    .trim()
    .toUpperCase();
  if (censusId && censusId === String(source.propertyId || "").toUpperCase()) {
    return { score: 100, reason: "property_id" };
  }

  const website = String(census.fields.Website || "").toLowerCase();
  const pid = String(source.propertyId || "").toLowerCase();
  if (website && pid && website.includes(`/hotel/${pid}`)) {
    return { score: 98, reason: "website_property_id" };
  }

  const countryOk =
    normalizeKey(census.fields[CENSUS_FIELDS.country]) === normalizeKey(source.country) ||
    normalizeKey(census.fields[CENSUS_FIELDS.country]) === normalizeKey(source.censusCountry);
  if (!countryOk) return { score: 0, reason: "country_mismatch" };

  const censusName = String(census.fields.name || "");
  const sourceName = String(source.name || "");
  // Official Accor title often appends "Handwritten Collection" and product/suite wording;
  // census may keep a market nickname (e.g. Riviera Nayarit).
  if (/marival\s+distinct/i.test(sourceName) && /marival\s+distinct/i.test(censusName)) {
    const cityOk =
      !source.city ||
      !census.fields[CENSUS_FIELDS.city] ||
      normalizeKey(source.city).includes(normalizeKey(census.fields[CENSUS_FIELDS.city])) ||
      normalizeKey(census.fields[CENSUS_FIELDS.city]).includes(normalizeKey(source.city)) ||
      /nuevo\s*vallarta|riviera\s*nayarit|puerto\s*vallarta/i.test(
        `${source.city} ${census.fields[CENSUS_FIELDS.city]} ${censusName}`
      );
    if (cityOk) return { score: 94, reason: "marival_distinct_same_market" };
    return { score: 86, reason: "marival_distinct_country" };
  }

  const sourceCore = sourceName.replace(/\s+Handwritten Collection\s*$/i, "").trim();
  const sim = Math.max(nameSimilarity(sourceName, censusName), nameSimilarity(sourceCore, censusName));
  if (sim >= 0.85) return { score: Math.round(90 + sim * 8), reason: "name_high" };
  if (sim >= 0.65) return { score: Math.round(75 + sim * 10), reason: "name_medium" };
  return { score: 0, reason: "none" };
}

/**
 * @param {import('airtable').Record[]} censusRows
 * @param {object[]} catalogHotels
 * @param {{ minScore?: number }} [opts]
 */
export function planHandwrittenAffiliationUpdates(censusRows, catalogHotels, opts = {}) {
  const minScore = opts.minScore ?? 80;
  /** @type {Map<string, object>} */
  const byRecordId = new Map();
  /** @type {object[]} */
  const unmatchedSources = [];

  for (const source of catalogHotels) {
    let best = null;
    for (const rec of censusRows) {
      const { score, reason } = scoreHandwrittenCensusMatch(source, rec);
      if (score >= minScore && (!best || score > best.score)) best = { rec, score, reason };
    }
    if (!best) {
      unmatchedSources.push(source);
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
      currentPropertyId: best.rec.fields[CENSUS_PROPERTY_ID_FIELD] || "",
      source,
      matchScore: best.score,
      matchReason: best.reason,
    });
  }

  return { matches: [...byRecordId.values()], unmatchedSources };
}

/**
 * @param {object} row
 * @param {{ fillWebsite?: boolean, fillParent?: boolean, fillPropertyId?: boolean, overwriteIndependentAffiliation?: boolean }} [opts]
 */
export function buildHandwrittenCensusPatch(row, opts = {}) {
  const fillWebsite = opts.fillWebsite !== false;
  const fillParent = opts.fillParent !== false;
  const fillPropertyId = opts.fillPropertyId !== false;
  /** @type {Record<string, string>} */
  const fields = {};
  const source = row.source || {};

  const currentAff = String(row.currentAffiliation || "").trim();
  if (currentAff !== HANDWRITTEN_AFFILIATION) {
    fields[CENSUS_FIELDS.affiliation] = HANDWRITTEN_AFFILIATION;
  }

  const currentParent = String(row.currentParentCompany || "").trim();
  if (
    fillParent &&
    (!currentParent ||
      /^independent$/i.test(currentParent) ||
      currentParent !== HANDWRITTEN_PARENT_COMPANY)
  ) {
    // Only overwrite Parent when blank/Independent or correcting onto AccorHotels for this match
    if (!currentParent || /^independent$/i.test(currentParent)) {
      fields[CENSUS_FIELDS.parentCompany] = HANDWRITTEN_PARENT_COMPANY;
    } else if (currentParent !== HANDWRITTEN_PARENT_COMPANY) {
      // leave parent for steward if some other chain is set
      row.parentCompanySteward = currentParent;
    }
  }

  if (fillWebsite && source.propertyUrl && isBlankCensusValue(row.currentWebsite)) {
    fields.Website = source.propertyUrl;
  }
  if (fillPropertyId && source.propertyId && isBlankCensusValue(row.currentPropertyId)) {
    fields[CENSUS_PROPERTY_ID_FIELD] = source.propertyId;
  }

  return fields;
}

/**
 * Pipeline / announced Handwritten properties not yet on Accor open catalog.
 * Document only — do not invent Property IDs or create Open census rows.
 */
export const HANDWRITTEN_CALA_PIPELINE_STEWARD = [
  {
    name: "Nui Handwritten Collection – João Pessoa",
    city: "João Pessoa",
    country: "Brazil",
    status: "pipeline",
    expectedOpen: "2028–2029",
    note: "Announced Accor Handwritten debut in Brazil (Tambaú). Not in Accor catalog brand=SOU as of discovery run — do not invent Property ID.",
    sources: [
      "https://brasilturis.com.br/2025/09/05/handwritten-collection-estreia-no-brasil-com-hotel-em-joao-pessoa/",
      "https://hoteliernews.com.br/accor-anuncia-estreia-da-handwritten-collection-no-brasil/",
    ],
  },
];

/**
 * Fetch amenities for a Handwritten Accor property URL.
 * @param {string} propertyUrl
 */
export async function fetchHandwrittenAmenities(propertyUrl) {
  return fetchAccorHotelAmenities(propertyUrl);
}

/**
 * @param {string[]} propertyIds
 */
export async function fetchHandwrittenByIds(propertyIds) {
  return fetchAccorCatalogByIds(propertyIds);
}

export const MAP_HANDWRITTEN_CENSUS = {
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: CENSUS_FIELDS.parentCompany,
  website: "Website",
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
};
