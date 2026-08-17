/**
 * Plan Marriott census enrichment from country sitemap directory crawl.
 */

import { normalizeText } from "../independent-census/match-current-census.js";
import {
  marriottOpenMatchScore,
  marriottLocationTokenOverlap,
  marriottBrandsCompatible,
} from "../marriott-name-match.js";
import {
  crawlMarriottCountrySitemaps,
  censusCountryToSitemapSlug,
} from "../marriott-brand-directory-extract.js";
import {
  crawlRitzCarltonOverviewSupplement,
  mergeMarriottDirectoryRows,
} from "../marriott-brand-tld-sitemap-supplement.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  matchDirectoryRowsToCensus,
} from "./match-brand-directory-to-census.js";
import { marshaFromMarriottWebsite } from "../marriott-brand-directory-extract.js";
import {
  buildMarriottDirectoryBackfillFields,
  probeMarriottBackfillFields,
} from "./marriott-census-field-backfill-contract.js";
import { isBlankCensusValue, MAP_DIRECTORY_ENRICHMENT } from "./brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";

const MARRIOTT_PARENT_FORMULA = `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`;

/**
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>[]} censusRows
 */
export function deriveCountrySlugsFromCensusRows(censusRows) {
  /** @type {Set<string>} */
  const slugs = new Set();
  for (const row of censusRows) {
    const slug = censusCountryToSitemapSlug(row.country);
    if (slug) slugs.add(slug);
  }
  return [...slugs].sort();
}

function rowNeedsWebsiteOrPropertyIdBackfill(fields) {
  return (
    isBlankCensusValue(fields?.[MAP_DIRECTORY_ENRICHMENT.website]) ||
    isBlankCensusValue(fields?.[CENSUS_PROPERTY_ID_FIELD])
  );
}

function rowNeedsBackfill(fields, presentWritable) {
  for (const col of presentWritable) {
    if (isBlankCensusValue(fields?.[col])) return true;
  }
  return false;
}

function isOpenCensusStatus(statusRaw) {
  if (Array.isArray(statusRaw)) return statusRaw.some((x) => /open/i.test(String(x)));
  return /open/i.test(String(statusRaw || ""));
}

/**
 * Pick best directory row for a census row using Marriott-aware name matching.
 *
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 * @param {ReturnType<import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel>[]} directoryRows
 * @param {{ minScore?: number, openOnly?: boolean }} [opts]
 */
export function pickMarriottDirectoryNameMatch(censusRow, directoryRows, opts = {}) {
  const minScore = opts.minScore ?? 0.42;
  const censusFields = censusRow.fields || {};
  if (opts.openOnly && !isOpenCensusStatus(censusFields[CENSUS_FIELDS.status])) {
    return null;
  }

  const slug = censusCountryToSitemapSlug(censusRow.country);
  /** @type {{ directoryRow: object, sim: number } | null} */
  let best = null;
  /** @type {{ directoryRow: object, sim: number } | null} */
  let second = null;

  for (const directoryRow of directoryRows) {
    const countryPageOk =
      !slug || !directoryRow.countryPage || directoryRow.countryPage === slug;
    const sim = marriottOpenMatchScore(
      censusRow.name,
      directoryRow.name,
      censusRow.city,
      censusRow.country
    );

    if (!countryPageOk && sim < 0.68) continue;
    if (sim < minScore) continue;
    if (!marriottLocationTokenOverlap(censusRow.name, directoryRow.name, censusRow.country)) continue;
    if (!marriottBrandsCompatible(censusRow.name, directoryRow.name)) continue;

    const hit = { directoryRow, sim };
    if (!best || sim > best.sim) {
      second = best;
      best = hit;
    } else if (!second || sim > second.sim) {
      second = hit;
    }
  }

  if (!best) return null;
  const margin = second ? best.sim - second.sim : 1;
  if (best.sim < 0.55 && margin < 0.1) return null;
  if (best.sim < 0.48 && margin < 0.15) return null;
  return { ...best, secondSim: second?.sim ?? 0, margin };
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.countrySlugs]
 * @param {number} [opts.crawlDelayMs]
 * @param {string} [opts.minConfidence]
 * @param {ReturnType<import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel>[]} [opts.directoryRows]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planMarriottCensusEnrichment(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const { writable: presentWritable } = await probeMarriottBackfillFields(base);
  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of presentWritable) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }
  if (!selectFields.includes(CENSUS_FIELDS.status)) selectFields.push(CENSUS_FIELDS.status);

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: MARRIOTT_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  const countrySlugs =
    opts.countrySlugs?.length ? opts.countrySlugs : deriveCountrySlugsFromCensusRows(censusRows);

  let directoryRows = opts.directoryRows;
  let crawlSummary = null;
  if (!directoryRows) {
    const crawl = await crawlMarriottCountrySitemaps({
      countrySlugs,
      delayMs: opts.crawlDelayMs,
      onProgress: opts.onProgress,
    });
    if (opts.onProgress) opts.onProgress("Fetching Ritz-Carlton HWS sitemap supplement…");
    const ritz = await crawlRitzCarltonOverviewSupplement({ onProgress: opts.onProgress });
    directoryRows = mergeMarriottDirectoryRows(crawl.hotels, ritz.hotels);
    crawlSummary = {
      countryPagesFetched: crawl.countryPagesFetched,
      hotelsFound: directoryRows.length,
      marriottCountryHotels: crawl.hotelsFound,
      ritzCarltonSupplementHotels: ritz.hotelsFound,
      fetchErrors: [...(crawl.fetchErrors || []), ...(ritz.fetchErrors || [])],
    };
  }

  const { matches, unmatchedCensus, unmatchedDirectory } = matchDirectoryRowsToCensus(
    directoryRows,
    censusRows,
    { minConfidence: opts.minConfidence || "low" }
  );

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const match of matches) {
    const { directoryRow, censusRow, confidence, reason, score, nameSim, distanceMeters } = match;
    if (!censusRow?.recordId) continue;
    const censusFields = censusRow.fields || {};
    if (!rowNeedsBackfill(censusFields, presentWritable)) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha: directoryRow.marshaCode,
        reason: "already_complete",
      });
      continue;
    }

    const applyFields = buildMarriottDirectoryBackfillFields(
      censusFields,
      directoryRow,
      presentWritable
    );
    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha: directoryRow.marshaCode,
        reason: "no_fill_blank_fields",
      });
      continue;
    }

    planRows.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      affiliation: censusRow.affiliation,
      country: censusRow.country,
      city: censusRow.city,
      directoryName: directoryRow.name,
      marshaCode: directoryRow.marshaCode,
      websiteSuggested: directoryRow.website,
      matchConfidence: confidence,
      matchScore: score,
      matchReason: reason,
      nameSimilarity: nameSim,
      distanceMeters,
      applyFields,
      source: directoryRow.source,
    });
  }

  /** Direct website/marsha matches for rows not in directory match set */
  const matchedIds = new Set(planRows.map((r) => r.censusRecordId));
  for (const censusRow of censusRows) {
    if (matchedIds.has(censusRow.recordId)) continue;
    const marsha =
      normalizeText(censusRow.fields?.[CENSUS_PROPERTY_ID_FIELD]).toUpperCase() ||
      marshaFromMarriottWebsite(censusRow.website);
    if (!marsha) continue;
    const directoryRow = directoryRows.find((d) => d.marshaCode === marsha);
    if (!directoryRow) continue;
    const applyFields = buildMarriottDirectoryBackfillFields(
      censusRow.fields || {},
      directoryRow,
      presentWritable
    );
    if (!Object.keys(applyFields).length) continue;
    planRows.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      affiliation: censusRow.affiliation,
      country: censusRow.country,
      city: censusRow.city,
      directoryName: directoryRow.name,
      marshaCode: marsha,
      websiteSuggested: directoryRow.website,
      matchConfidence: "high",
      matchScore: 100,
      matchReason: "marsha/property_id direct",
      applyFields,
      source: "marsha_direct",
    });
  }

  /** Unmatched census rows: Marriott-aware name match (country page + brand-TLD). */
  const plannedIds = new Set(planRows.map((r) => r.censusRecordId));
  for (const censusRow of censusRows) {
    if (plannedIds.has(censusRow.recordId)) continue;
    if (!rowNeedsWebsiteOrPropertyIdBackfill(censusRow.fields || {})) continue;

    const best = pickMarriottDirectoryNameMatch(censusRow, directoryRows);
    if (!best) continue;

    const applyFields = buildMarriottDirectoryBackfillFields(
      censusRow.fields || {},
      best.directoryRow,
      presentWritable
    );
    if (!Object.keys(applyFields).length) continue;

    planRows.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      affiliation: censusRow.affiliation,
      country: censusRow.country,
      city: censusRow.city,
      directoryName: best.directoryRow.name,
      marshaCode: best.directoryRow.marshaCode,
      websiteSuggested: best.directoryRow.website,
      matchConfidence: best.sim >= 0.72 ? "medium" : "low",
      matchScore: Math.round(best.sim * 100),
      matchReason: "Marriott name match (country or brand-TLD sitemap)",
      nameSimilarity: best.sim,
      applyFields,
      source: best.directoryRow.source || "marriott_name_match",
    });
    plannedIds.add(censusRow.recordId);
  }

  /** Open-status rescue: global sitemap crawl for remaining open rows still blank. */
  let globalRescueCount = 0;
  const stillBlankOpen = censusRows.filter((row) => {
    if (plannedIds.has(row.recordId)) return false;
    if (!isOpenCensusStatus(row.fields?.[CENSUS_FIELDS.status])) return false;
    return rowNeedsWebsiteOrPropertyIdBackfill(row.fields || {});
  });

  if (stillBlankOpen.length && !opts.skipGlobalRescue) {
    if (opts.onProgress) {
      opts.onProgress(
        `Global sitemap rescue for ${stillBlankOpen.length} open census row(s) still blank…`
      );
    }
    const globalCrawl = await crawlMarriottCountrySitemaps({
      delayMs: opts.crawlDelayMs,
      onProgress: opts.onProgress,
    });
    const globalDirectory = mergeMarriottDirectoryRows(globalCrawl.hotels, directoryRows);

    for (const censusRow of stillBlankOpen) {
      if (plannedIds.has(censusRow.recordId)) continue;
      const best = pickMarriottDirectoryNameMatch(censusRow, globalDirectory, { minScore: 0.48 });
      if (!best) continue;

      const applyFields = buildMarriottDirectoryBackfillFields(
        censusRow.fields || {},
        best.directoryRow,
        presentWritable
      );
      if (!Object.keys(applyFields).length) continue;

      planRows.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        affiliation: censusRow.affiliation,
        country: censusRow.country,
        city: censusRow.city,
        directoryName: best.directoryRow.name,
        marshaCode: best.directoryRow.marshaCode,
        websiteSuggested: best.directoryRow.website,
        matchConfidence: "low",
        matchScore: Math.round(best.sim * 100),
        matchReason: "Open-status global sitemap rescue",
        nameSimilarity: best.sim,
        applyFields,
        source: "open_global_rescue",
      });
      plannedIds.add(censusRow.recordId);
      globalRescueCount++;
    }

    if (crawlSummary) {
      crawlSummary.globalRescueRows = globalRescueCount;
      crawlSummary.globalDirectoryHotels = globalDirectory.length;
    }
  }

  return {
    censusRowsScanned: censusRows.length,
    countrySlugs,
    crawlSummary,
    directoryHotels: directoryRows.length,
    readyToApply: planRows.length,
    skipped,
    unmatchedCensusCount: unmatchedCensus.length,
    unmatchedDirectoryCount: unmatchedDirectory.length,
    presentWritable,
    planRows,
  };
}

/**
 * @param {object} [opts]
 */
export async function auditMarriottCensusFieldBlanks(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  const { writable } = await probeMarriottBackfillFields(base);
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [...new Set([...writable, CENSUS_FIELDS.name, CENSUS_FIELDS.country])],
      filterByFormula: MARRIOTT_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  /** @type {Record<string, number>} */
  const blankCounts = {};
  for (const field of writable) blankCounts[field] = 0;
  for (const rec of records) {
    for (const field of writable) {
      if (isBlankCensusValue(rec.fields?.[field])) blankCounts[field]++;
    }
  }

  return {
    total: records.length,
    writable,
    blankCounts,
    countrySlugsAvailable: deriveCountrySlugsFromCensusRows(records.map(mapCensusRowForDirectoryMatch)),
  };
}
