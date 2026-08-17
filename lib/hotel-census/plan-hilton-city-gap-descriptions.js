/**
 * Gap-fill Hilton descriptions via city-level locations pages (not in country crawl).
 */

import { fetchHiltonHotelDescription, pickPrimaryHiltonDescription } from "../hilton-hotel-description-fetch.js";
import {
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
  hiltonLocationsUrl,
} from "../hilton-brand-directory-extract.js";
import { loadHiltonBrandDirectoryConfigs, affiliationHintsForBrand } from "../hilton-brand-registry.js";
import { nameSimilarity, normalizeKey, normalizeText, citiesMatch } from "../independent-census/match-current-census.js";
import {
  buildDescriptionEnrichmentFields,
  ENRICHMENT_SOURCE_HILTON_GRAPHQL,
  probeCensusDescriptionFields,
} from "./hilton-description-enrichment-contract.js";
import { buildDirectoryBackfillFields, buildFillBlankPatch } from "./hilton-census-field-backfill-contract.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import {
  directoryAmenityIdsToCensusFields,
  formatAmenitiesText,
} from "../hilton-amenity-map.js";
import { MAP_HILTON_CENSUS_FIELD_BACKFILL } from "./hilton-census-field-backfill-contract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";

const COUNTRY_SLUGS = {
  mexico: "mexico",
  "dominican republic": "dominican-republic",
  "costa rica": "costa-rica",
  panama: "panama",
  bahamas: "bahamas",
  jamaica: "jamaica",
  "trinidad and tobago": "trinidad-and-tobago",
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
 * @param {object[]} censusRows — { recordId, name, city, country, affiliation, fields }
 * @param {object} [opts]
 */
export async function planHiltonDescriptionsViaCityPages(censusRows, opts = {}) {
  const { fetchDelayMs = 300, minNameSim = 0.55, onProgress } = opts;
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const configs = await loadHiltonBrandDirectoryConfigs();
  const presentFields = await probeCensusDescriptionFields(base);
  const presentWritable = [...new Set([...presentFields, "Amenities", "Website", "Open Date", "Year Affiliated", MAP_DIRECTORY_ENRICHMENT.website])];
  const planRows = [];
  const fetchErrors = [];
  const skipped = [];

  /** @type {Map<string, object[]>} */
  const cityCache = new Map();

  for (let i = 0; i < censusRows.length; i++) {
    const row = censusRows[i];
    const brandCfg = resolveBrandConfig(row.affiliation, configs);
    if (!brandCfg) {
      skipped.push({ ...row, reason: "no_brand_config" });
      continue;
    }

    const cSlug = countrySlug(row.country);
    const ciSlug = citySlug(row.city);
    if (!cSlug || !ciSlug) {
      skipped.push({ ...row, reason: "no_geo_slug" });
      continue;
    }

    const pageUrl = hiltonLocationsUrl(`locations/${cSlug}/${ciSlug}/${brandCfg.locationsSlug}/`);
    const cacheKey = pageUrl;

    let directoryHotels = cityCache.get(cacheKey);
    if (!directoryHotels) {
      if (onProgress) onProgress(`Fetching city page ${pageUrl}`);
      try {
        const page = await fetchHiltonLocationsPage(pageUrl);
        directoryHotels = extractHotelsFromPageData(page.pageData)
          .filter((h) => String(h?.brandCode || "").trim() === brandCfg.brandCode)
          .map((h) => normalizeHiltonDirectoryHotel(h, { sourceUrl: pageUrl }));
        cityCache.set(cacheKey, directoryHotels);
      } catch (err) {
        skipped.push({ ...row, reason: "city_page_error", error: err?.message || String(err) });
        if (opts.pageDelayMs) await sleep(opts.pageDelayMs);
        continue;
      }
      if (opts.pageDelayMs) await sleep(opts.pageDelayMs);
    }

    let best = null;
    for (const d of directoryHotels) {
      const sim = nameSimilarity(row.name, d.name);
      const cityOk = citiesMatch(row.city, d.city);
      if (sim >= minNameSim && cityOk && (!best || sim > best.sim)) {
        best = { hotel: d, sim };
      }
    }

    if (!best) {
      skipped.push({ ...row, reason: "no_directory_match", hotelsOnPage: directoryHotels.length });
      continue;
    }

    const ctyhocn = best.hotel.ctyhocn;
    const website = best.hotel.website || `https://www.hilton.com/en/hotels/${ctyhocn.toLowerCase()}-hotel/`;
    if (onProgress) {
      onProgress(`[${i + 1}/${censusRows.length}] ${ctyhocn} — ${row.name} ↔ ${best.hotel.name} (${best.sim.toFixed(2)})`);
    }

    const dirFields = buildDirectoryBackfillFields(
      row.fields || {},
      {
        website,
        amenitiesText: formatAmenitiesText(best.hotel.amenityIds || []),
        openDate: best.hotel.status === "Open" ? best.hotel.openDate : null,
        amenityFlags: {},
      },
      presentWritable
    );
    const amenityPatch = directoryAmenityIdsToCensusFields(best.hotel.amenityIds || []);
    Object.assign(
      dirFields,
      buildFillBlankPatch(row.fields || {}, amenityPatch, presentWritable)
    );

    try {
      const descriptionRow = await fetchHiltonHotelDescription(ctyhocn, { refererUrl: website });
      const primary = pickPrimaryHiltonDescription(descriptionRow);
      const applyFields = buildDescriptionEnrichmentFields(row.fields || {}, descriptionRow, {
        fillBlankOnly: true,
        presentFields: presentWritable,
      });
      Object.assign(applyFields, dirFields);
      planRows.push({
        censusRecordId: row.recordId,
        censusName: row.name,
        directoryName: best.hotel.name,
        ctyhocn,
        matchNameSim: best.sim,
        cityPageUrl: pageUrl,
        primaryDescription: primary,
        applyFields,
        source: ENRICHMENT_SOURCE_HILTON_GRAPHQL,
        status: Object.keys(applyFields).length ? "ready" : "no_blank_fields",
      });
    } catch (err) {
      fetchErrors.push({ censusRecordId: row.recordId, ctyhocn, error: err?.message || String(err) });
    }

    if (fetchDelayMs > 0 && i < censusRows.length - 1) await sleep(fetchDelayMs);
  }

  return { planRows, fetchErrors, skipped };
}

/**
 * Load Hilton census rows needing city-gap backfill.
 * @param {{ openOnly?: boolean, anyTargetBlank?: boolean }} [opts]
 */
export async function loadBlankHiltonCensusRows(opts = {}) {
  const base = getPlatformBase();
  const targetCols = [
    MAP_HILTON_CENSUS_FIELD_BACKFILL.hotelDescription,
    MAP_HILTON_CENSUS_FIELD_BACKFILL.amenities,
    MAP_HILTON_CENSUS_FIELD_BACKFILL.website,
    MAP_HILTON_CENSUS_FIELD_BACKFILL.openDate,
  ];
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.city,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.status,
        CENSUS_FIELDS.parentCompany,
        ...targetCols,
        "Website",
      ],
      filterByFormula: `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`,
      pageSize: 100,
    })
    .all();

  let rows = records
    .filter((r) => {
      if (!opts.anyTargetBlank) {
        return isBlankCensusValue(r.fields[MAP_HILTON_CENSUS_FIELD_BACKFILL.hotelDescription]);
      }
      return targetCols.some((col) => isBlankCensusValue(r.fields?.[col]));
    })
    .map((r) => ({
      recordId: r.id,
      name: normalizeText(r.fields[CENSUS_FIELDS.name]),
      affiliation: normalizeText(r.fields[CENSUS_FIELDS.affiliation]),
      city: normalizeText(r.fields[CENSUS_FIELDS.city]),
      country: normalizeText(r.fields[CENSUS_FIELDS.country]),
      status: Array.isArray(r.fields[CENSUS_FIELDS.status])
        ? r.fields[CENSUS_FIELDS.status].join(",")
        : normalizeText(r.fields[CENSUS_FIELDS.status]),
      fields: r.fields,
    }));

  if (opts.openOnly) {
    rows = rows.filter((r) => /open/i.test(r.status));
  }
  return rows;
}
