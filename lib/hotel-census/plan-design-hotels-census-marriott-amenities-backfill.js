/**
 * Plan Design Hotels census amenities from marriott.com HWS subpages (MARSHA-driven).
 */

import {
  crawlMarriottCountrySitemaps,
  censusCountryToSitemapSlug,
} from "../marriott-brand-directory-extract.js";
import { fetchMarriottSubpageContent } from "../marriott-subpage-content-fetch.js";
import { fetchMarriottBazaarvoiceProduct } from "../marriott-bazaarvoice-content-fetch.js";
import {
  DESIGN_HOTELS_AFFILIATION,
  isCalaCountry,
} from "../design-hotels-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import {
  buildDesignHotelsMarriottAmenitiesPatch,
  MAP_DESIGN_HOTELS_CENSUS_CONTENT,
} from "./design-hotels-census-content-contract.js";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {ReturnType<import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel>[]} hotels
 */
export function indexMarriottHotelsByMarsha(hotels) {
  /** @type {Map<string, { marshaCode: string, website: string, name: string }>} */
  const map = new Map();
  for (const hotel of hotels || []) {
    const marsha = String(hotel.marshaCode || "").trim().toUpperCase();
    if (!marsha) continue;
    map.set(marsha, {
      marshaCode: marsha,
      website: String(hotel.website || "").trim(),
      name: String(hotel.name || "").trim(),
    });
  }
  return map;
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.recordIds]
 * @param {number} [opts.limit]
 * @param {number} [opts.fetchDelayMs]
 * @param {boolean} [opts.refreshAmenities]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planDesignHotelsCensusMarriottAmenitiesBackfill(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of Object.values(MAP_DESIGN_HOTELS_CENSUS_CONTENT)) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }
  if (!selectFields.includes(CENSUS_PROPERTY_ID_FIELD)) selectFields.push(CENSUS_PROPERTY_ID_FIELD);

  const records = await base(HOTEL_CENSUS_TABLE).select({ fields: selectFields, pageSize: 100 }).all();

  let targets = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION &&
      !isBlankCensusValue(r.fields[CENSUS_PROPERTY_ID_FIELD])
  );

  if (opts.recordIds?.length) {
    const want = new Set(opts.recordIds);
    targets = targets.filter((r) => want.has(r.id));
  }
  if (opts.limit > 0) targets = targets.slice(0, opts.limit);

  const slugs = [
    ...new Set(
      targets
        .map((r) => censusCountryToSitemapSlug(r.fields[CENSUS_FIELDS.country]))
        .filter(Boolean)
    ),
  ];

  opts.onProgress?.(`Crawl Marriott sitemaps: ${slugs.join(", ") || "(none)"}`);
  const crawl = await crawlMarriottCountrySitemaps({
    countrySlugs: slugs,
    delayMs: 350,
    onProgress: opts.onProgress,
  });
  const marshaIndex = indexMarriottHotelsByMarsha(crawl.hotels);

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const fetchErrors = [];

  for (let i = 0; i < targets.length; i++) {
    const rec = targets[i];
    const marsha = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();
    const marriottListing = marshaIndex.get(marsha);
    const marriottUrl = marriottListing?.website || "";

    opts.onProgress?.(`[${i + 1}/${targets.length}] ${rec.fields.name} (${marsha})`);

    if (!marriottUrl) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        marshaCode: marsha,
        reason: "missing_marriott_url_for_marsha",
      });
      continue;
    }

    try {
      const content = await fetchMarriottSubpageContent(marriottUrl, { marshaCode: marsha });

      let description = content.description;
      if (!description) {
        const bv = await fetchMarriottBazaarvoiceProduct(marsha);
        if (bv?.description) description = bv.description;
      }

      if (!content.amenitiesText && !description) {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields.name,
          marshaCode: marsha,
          marriottUrl,
          reason: "no_marriott_content",
          parseErrors: content.parseErrors,
          subpages: content.subpages,
        });
        if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
        continue;
      }

      const applyFields = buildDesignHotelsMarriottAmenitiesPatch(
        rec.fields,
        {
          amenities: content.amenities,
          amenitiesText: content.amenitiesText,
          description,
        },
        selectFields,
        { refreshAmenities: Boolean(opts.refreshAmenities) }
      );

      if (!Object.keys(applyFields).length) {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields.name,
          marshaCode: marsha,
          marriottUrl,
          reason: "no_fill_blank_fields",
          marriottAmenities: content.amenities,
        });
        if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
        continue;
      }

      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        marshaCode: marsha,
        marriottUrl,
        marriottListingName: marriottListing?.name || "",
        marriottAmenities: content.amenities,
        amenitiesTextSuggested: content.amenitiesText,
        descriptionSuggested: description,
        subpages: content.subpages,
        parseErrors: content.parseErrors,
        applyFields,
      });
    } catch (err) {
      fetchErrors.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        marshaCode: marsha,
        error: err?.message || String(err),
      });
    }

    if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
  }

  return {
    generatedAt: new Date().toISOString(),
    censusRowsScanned: targets.length,
    marriottSitemapHotels: crawl.hotels.length,
    readyToApply: planRows.length,
    skipped,
    fetchErrors,
    planRows,
  };
}

/**
 * Audit amenities blanks for CALA Design Hotels with MARSHA.
 */
export async function auditDesignHotelsMarriottAmenitiesTargets() {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.affiliation,
    CENSUS_PROPERTY_ID_FIELD,
    MAP_DESIGN_HOTELS_CENSUS_CONTENT.amenities,
  ];

  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();
  const rows = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION
  );
  const withMarsha = rows.filter((r) => !isBlankCensusValue(r.fields[CENSUS_PROPERTY_ID_FIELD]));
  const blankAmenities = withMarsha.filter((r) =>
    isBlankCensusValue(r.fields[MAP_DESIGN_HOTELS_CENSUS_CONTENT.amenities])
  );

  return {
    total: rows.length,
    withMarsha: withMarsha.length,
    blankAmenitiesWithMarsha: blankAmenities.length,
    blankAmenityNames: blankAmenities.map((r) => r.fields.name),
  };
}
