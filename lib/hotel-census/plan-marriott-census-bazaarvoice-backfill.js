/**
 * Bulk Marriott census description backfill via Bazaarvoice (MARSHA product ids).
 */

import { fetchMarriottBazaarvoiceProducts } from "../marriott-bazaarvoice-content-fetch.js";
import {
  crawlMarriottCountrySitemaps,
  marshaFromMarriottWebsite,
} from "../marriott-brand-directory-extract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  matchDirectoryRowsToCensus,
} from "./match-brand-directory-to-census.js";
import {
  buildMarriottContentBackfillFields,
} from "./plan-marriott-census-content-backfill.js";
import {
  probeMarriottBackfillFields,
  MAP_MARRIOTT_CENSUS_FIELD_BACKFILL,
  buildMarriottFillBlankPatch,
} from "./marriott-census-field-backfill-contract.js";
import { isBlankCensusValue, MAP_DIRECTORY_ENRICHMENT } from "./brand-directory-enrichment-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "./hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import { deriveCountrySlugsFromCensusRows } from "./plan-marriott-census-enrichment.js";

const MARRIOTT_PARENT_FORMULA = `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`;

function resolveMarsha(censusRow, directoryByMarsha) {
  const fromField = String(censusRow.fields?.[CENSUS_PROPERTY_ID_FIELD] || "")
    .trim()
    .toUpperCase();
  if (fromField) return fromField;
  const fromWeb = marshaFromMarriottWebsite(censusRow.website);
  if (fromWeb) return fromWeb;
  for (const row of directoryByMarsha.values()) {
    if (row.name && censusRow.name && row.name.toLowerCase() === censusRow.name.toLowerCase()) {
      return row.marshaCode;
    }
  }
  return "";
}

/**
 * @param {object} [opts]
 * @param {number} [opts.batchSize]
 * @param {number} [opts.delayMs]
 * @param {number} [opts.crawlDelayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planMarriottCensusBazaarvoiceBackfill(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const { writable: presentWritable } = await probeMarriottBackfillFields(base);
  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of presentWritable) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }
  if (!selectFields.includes(MAP_DIRECTORY_ENRICHMENT.website)) {
    selectFields.push(MAP_DIRECTORY_ENRICHMENT.website);
  }
  if (!selectFields.includes(CENSUS_PROPERTY_ID_FIELD)) {
    selectFields.push(CENSUS_PROPERTY_ID_FIELD);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: MARRIOTT_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  const countrySlugs = deriveCountrySlugsFromCensusRows(censusRows);

  if (opts.onProgress) opts.onProgress(`Crawling ${countrySlugs.length} country sitemaps for MARSHA codes…`);
  const crawl = await crawlMarriottCountrySitemaps({
    countrySlugs,
    delayMs: opts.crawlDelayMs ?? 250,
    onProgress: opts.onProgress,
  });

  /** @type {Map<string, import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel>} */
  const directoryByMarsha = new Map(crawl.hotels.map((h) => [h.marshaCode, h]));

  const { matches } = matchDirectoryRowsToCensus(crawl.hotels, censusRows, { minConfidence: "low" });
  /** @type {Map<string, string>} */
  const marshaByRecordId = new Map();
  for (const row of censusRows) {
    const m = resolveMarsha(row, directoryByMarsha);
    if (m) marshaByRecordId.set(row.recordId, m);
  }
  for (const match of matches) {
    if (match.censusRow?.recordId && match.directoryRow?.marshaCode) {
      marshaByRecordId.set(match.censusRow.recordId, match.directoryRow.marshaCode);
    }
  }

  const targets = censusRows.filter((r) => isBlankCensusValue(r.fields?.[CENSUS_DESCRIPTION_FIELD]));
  const marshaCodes = [...new Set([...marshaByRecordId.values()])];

  if (opts.onProgress) {
    opts.onProgress(`Fetching Bazaarvoice descriptions for ${marshaCodes.length} MARSHA codes…`);
  }

  const bvByMarsha = await fetchMarriottBazaarvoiceProducts(marshaCodes, {
    batchSize: opts.batchSize ?? 10,
    delayMs: opts.delayMs ?? 200,
  });

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const censusRow of targets) {
    const marsha = marshaByRecordId.get(censusRow.recordId) || "";
    const bv = marsha ? bvByMarsha.get(marsha) : null;
    if (!marsha) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        reason: "no_marsha_resolved",
      });
      continue;
    }
    if (!bv?.description) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha,
        reason: "no_bazaarvoice_description",
      });
      continue;
    }

    const applyFields = buildMarriottContentBackfillFields(
      censusRow.fields || {},
      { description: bv.description, amenitiesText: "" },
      presentWritable
    );
    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha,
        reason: "no_fill_blank_fields",
      });
      continue;
    }

    planRows.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      marshaCode: marsha,
      descriptionSuggested: bv.description,
      bazaarvoiceName: bv.name,
      websiteSuggested: bv.website,
      contentSource: bv.source,
      applyFields,
    });
  }

  return {
    censusRowsScanned: targets.length,
    marshaCodesResolved: marshaCodes.length,
    bazaarvoiceHits: bvByMarsha.size,
    crawlSummary: {
      countryPagesFetched: crawl.countryPagesFetched,
      hotelsFound: crawl.hotelsFound,
      fetchErrors: crawl.fetchErrors?.length || 0,
    },
    readyToApply: planRows.length,
    skipped,
    presentWritable,
    targetFields: [MAP_MARRIOTT_CENSUS_FIELD_BACKFILL.hotelDescription],
    planRows,
  };
}

/**
 * Build website + property id patches from sitemap directory (fill-blank).
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 * @param {import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel} directoryRow
 * @param {string[]} presentWritable
 */
export function buildMarriottDirectoryIdentityPatch(censusRow, directoryRow, presentWritable) {
  const proposed = {};
  if (directoryRow.website) proposed[MAP_DIRECTORY_ENRICHMENT.website] = directoryRow.website;
  if (directoryRow.marshaCode) proposed[CENSUS_PROPERTY_ID_FIELD] = directoryRow.marshaCode;
  return buildMarriottFillBlankPatch(censusRow.fields || {}, proposed, presentWritable);
}

export { MARRIOTT_PARENT_FORMULA };
