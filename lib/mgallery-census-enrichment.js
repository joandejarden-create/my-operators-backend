/**
 * MGallery Collection (Accor brand MGA) → Hotel Census CALA enrichment.
 *
 * Sources:
 * - Accor Catalog API brand=MGA (authoritative open directory)
 * - mgallery.accor.com destinations page (uploaded / live)
 *
 * Affiliation / Parent Company: Brand Alias Mapping canonical
 *   "MGallery Collection" / "AccorHotels"
 */

import { COUNTRY_CONFIG_LIST } from "./radar-buildout/country-configs.js";
import { fetchAccorCatalogHotels, fetchAccorCatalogByIds } from "./accor-catalog-api.js";
import { fetchAccorHotelAmenities } from "./accor-hotel-content-fetch.js";
import { CENSUS_FIELDS } from "./hotel-census/fields.js";
import { nameSimilarity, normalizeKey } from "./independent-census/match-current-census.js";
import { isBlankCensusValue } from "./hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "./hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hotel-census/hilton-property-id-contract.js";

export const MGALLERY_AFFILIATION = "MGallery Collection";
export const MGALLERY_PARENT_COMPANY = "AccorHotels";
export const MGALLERY_BRAND_CODE = "MGA";

const CALA_SET = new Set(COUNTRY_CONFIG_LIST.map((c) => c.toLowerCase()));

export function isCalaCountry(country) {
  return CALA_SET.has(String(country || "").trim().toLowerCase());
}

/**
 * @param {object} [opts]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function fetchMgalleryCalaCatalog(opts = {}) {
  /** @type {Map<string, object>} */
  const byId = new Map();

  for (const country of COUNTRY_CONFIG_LIST) {
    opts.onProgress?.(`Catalog ${country} brand=MGA…`);
    const res = await fetchAccorCatalogHotels(country, {
      brand: MGALLERY_BRAND_CODE,
      enlargementAllowed: false,
    });
    if (!res.ok) continue;
    for (const h of res.hotels || []) {
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
export function scoreMgalleryCensusMatch(source, census) {
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

  if (census.fields[CENSUS_FIELDS.country] !== source.censusCountry) {
    // allow country label mismatch if property id already checked
    const countryOk =
      normalizeKey(census.fields[CENSUS_FIELDS.country]) === normalizeKey(source.country) ||
      normalizeKey(census.fields[CENSUS_FIELDS.country]) === normalizeKey(source.censusCountry);
    if (!countryOk) return { score: 0, reason: "country_mismatch" };
  }

  const sim = nameSimilarity(source.name, census.fields.name);
  if (sim >= 0.85) return { score: Math.round(90 + sim * 8), reason: "name_high" };
  if (sim >= 0.65) return { score: Math.round(75 + sim * 10), reason: "name_medium" };
  return { score: 0, reason: "none" };
}

/**
 * @param {import('airtable').Record[]} censusRows
 * @param {object[]} catalogHotels
 * @param {{ minScore?: number }} [opts]
 */
export function planMgalleryAffiliationUpdates(censusRows, catalogHotels, opts = {}) {
  const minScore = opts.minScore ?? 80;
  /** @type {Map<string, object>} */
  const byRecordId = new Map();
  /** @type {object[]} */
  const unmatchedSources = [];

  for (const source of catalogHotels) {
    let best = null;
    for (const rec of censusRows) {
      const { score, reason } = scoreMgalleryCensusMatch(source, rec);
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
 * @param {{ fillWebsite?: boolean, fillParent?: boolean, fillPropertyId?: boolean }} [opts]
 */
export function buildMgalleryCensusPatch(row, opts = {}) {
  const fillWebsite = opts.fillWebsite !== false;
  const fillParent = opts.fillParent !== false;
  const fillPropertyId = opts.fillPropertyId !== false;
  /** @type {Record<string, string>} */
  const fields = {};
  const source = row.source || {};

  if (String(row.currentAffiliation || "").trim() !== MGALLERY_AFFILIATION) {
    fields[CENSUS_FIELDS.affiliation] = MGALLERY_AFFILIATION;
  }
  if (fillParent && !String(row.currentParentCompany || "").trim()) {
    fields[CENSUS_FIELDS.parentCompany] = MGALLERY_PARENT_COMPANY;
  }

  if (fillWebsite && source.propertyUrl && !String(row.currentWebsite || "").trim()) {
    fields.Website = source.propertyUrl;
  }
  if (fillPropertyId && source.propertyId && !String(row.currentPropertyId || "").trim()) {
    fields[CENSUS_PROPERTY_ID_FIELD] = source.propertyId;
  }

  return fields;
}

/**
 * Plan amenities backfill for CALA MGallery census rows with Accor property URLs/IDs.
 * @param {object} [opts]
 */
export async function planMgalleryAmenitiesBackfill(opts = {}) {
  const { getPlatformBase } = await import("./hotel-census/platform-base.js");
  const { HOTEL_CENSUS_TABLE } = await import("./hotel-census/fields.js");
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    "name",
    "Website",
    CENSUS_PROPERTY_ID_FIELD,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.affiliation,
    CENSUS_AMENITIES_TEXT_FIELD,
  ];
  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();
  let targets = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === MGALLERY_AFFILIATION
  );
  if (opts.recordIds?.length) {
    const want = new Set(opts.recordIds);
    targets = targets.filter((r) => want.has(r.id));
  }

  const catalog = await fetchMgalleryCalaCatalog({ onProgress: opts.onProgress });
  const catalogById = new Map(catalog.map((h) => [h.propertyId.toUpperCase(), h]));

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const stewardExtras = [];

  for (const rec of targets) {
    const pid = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();
    let source = pid ? catalogById.get(pid) : null;
    if (!source) {
      const hit = planMgalleryAffiliationUpdates([rec], catalog, { minScore: 80 }).matches[0];
      source = hit?.source || null;
    }

    if (!source) {
      stewardExtras.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "not_on_accor_mgallery_catalog",
        propertyId: pid || null,
        website: rec.fields.Website || null,
      });
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "no_catalog_match",
      });
      continue;
    }

    const url = source.propertyUrl || String(rec.fields.Website || "").trim();
    opts.onProgress?.(`Amenities ${rec.fields.name} (${source.propertyId})…`);
    const fetched = await fetchAccorHotelAmenities(url);
    if (!fetched.ok || !fetched.amenitiesText) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "amenities_fetch_failed",
        errors: fetched.parseErrors,
      });
      continue;
    }

    /** @type {Record<string, unknown>} */
    const applyFields = {};
    if (opts.refreshAmenities || isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD])) {
      applyFields[CENSUS_AMENITIES_TEXT_FIELD] = fetched.amenitiesText;
    }
    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "amenities_already_populated",
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      propertyId: source.propertyId,
      propertyUrl: url,
      amenitiesText: fetched.amenitiesText,
      applyFields,
    });
  }

  return {
    generatedAt: new Date().toISOString(),
    catalogCount: catalog.length,
    censusRowsScanned: targets.length,
    readyToApply: planRows.length,
    skipped,
    stewardExtras,
    planRows,
  };
}

export { fetchAccorCatalogByIds };
