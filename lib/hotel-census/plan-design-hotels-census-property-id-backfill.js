/**
 * Plan Property ID (MARSHA) backfill for CALA Design Hotels census rows.
 */

import {
  crawlMarriottCountrySitemaps,
  censusCountryToSitemapSlug,
} from "../marriott-brand-directory-extract.js";
import {
  fetchDesignHotelsHotelContent,
  designHotelsOverviewUrl,
} from "../design-hotels-hotel-content-fetch.js";
import {
  DESIGN_HOTELS_AFFILIATION,
  isCalaCountry,
} from "../design-hotels-census-enrichment.js";
import { pickMarriottDirectoryNameMatch } from "./plan-marriott-census-enrichment.js";
import { mapCensusRowForDirectoryMatch } from "./match-brand-directory-to-census.js";
import { buildMarriottDirectoryBackfillFields } from "./marriott-census-field-backfill-contract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import { getPlatformBase } from "./platform-base.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";

export const MIN_SCORE_GENERAL_DH_NAME = 0.55;
export const MIN_SCORE_DH_POOL = 0.45;
export const MIN_MARGIN_DH_POOL = 0.1;

/** @param {string} name */
export function isDesignHotelsMarriottListingName(name) {
  return /design hotels|member of design hotels/i.test(String(name || ""));
}

/**
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 * @param {ReturnType<import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel>[]} directoryRows
 */
export function pickDesignHotelsMarshaMatch(censusRow, directoryRows) {
  const dhPool = directoryRows.filter((r) => isDesignHotelsMarriottListingName(r.name));

  const general = pickMarriottDirectoryNameMatch(censusRow, directoryRows, { minScore: MIN_SCORE_GENERAL_DH_NAME });
  if (general && isDesignHotelsMarriottListingName(general.directoryRow.name)) {
    return { ...general, matchPath: "sitemap_general_dh_name" };
  }

  const dhOnly = pickMarriottDirectoryNameMatch(censusRow, dhPool, { minScore: MIN_SCORE_DH_POOL });
  if (dhOnly && dhOnly.sim >= MIN_SCORE_DH_POOL && dhOnly.margin >= MIN_MARGIN_DH_POOL) {
    return { ...dhOnly, matchPath: "sitemap_dh_pool" };
  }

  if (general && general.sim >= MIN_SCORE_GENERAL_DH_NAME) {
    return { ...general, matchPath: "sitemap_borderline", borderline: true };
  }

  return null;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.recordIds]
 * @param {number} [opts.fetchDelayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planDesignHotelsCensusPropertyIdBackfill(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    "name",
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.city,
    CENSUS_FIELDS.affiliation,
    CENSUS_PROPERTY_ID_FIELD,
    "Website",
  ];

  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();

  let targets = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION &&
      isBlankCensusValue(r.fields[CENSUS_PROPERTY_ID_FIELD])
  );

  if (opts.recordIds?.length) {
    const want = new Set(opts.recordIds);
    targets = targets.filter((r) => want.has(r.id));
  }

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
    delayMs: 400,
    onProgress: opts.onProgress,
  });

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const stewardReview = [];
  /** @type {object[]} */
  const unresolved = [];

  const presentFields = ["Website", CENSUS_PROPERTY_ID_FIELD];

  for (const rec of targets) {
    const censusRow = mapCensusRowForDirectoryMatch(rec);
    const website = String(rec.fields.Website || "").trim();
    const overviewUrl = designHotelsOverviewUrl(website);

    if (overviewUrl) {
      opts.onProgress?.(`Design Hotels page ${rec.fields.name}…`);
      const content = await fetchDesignHotelsHotelContent(overviewUrl, {
        fetchDelayMs: opts.fetchDelayMs ?? 600,
      });
      if (content.marshaCode) {
        const directoryRow = {
          marshaCode: content.marshaCode,
          website: content.marriottUrl || "",
          name: rec.fields.name,
        };
        const applyFields = buildMarriottDirectoryBackfillFields(
          rec.fields,
          directoryRow,
          presentFields
        );
        if (Object.keys(applyFields).length) {
          planRows.push({
            censusRecordId: rec.id,
            censusName: rec.fields.name,
            marshaCode: content.marshaCode,
            marriottUrl: content.marriottUrl,
            matchPath: "designhotels_marriott_link",
            matchScore: 1,
            marriottListingName: "",
            applyFields,
          });
          if ((opts.fetchDelayMs ?? 600) > 0) await sleep(opts.fetchDelayMs ?? 600);
          continue;
        }
      }
      if ((opts.fetchDelayMs ?? 600) > 0) await sleep(opts.fetchDelayMs ?? 600);
    }

    const countrySlug = censusCountryToSitemapSlug(rec.fields[CENSUS_FIELDS.country]);
    const countryHotels = crawl.hotels.filter(
      (h) => !countrySlug || h.countryPage === countrySlug
    );
    const match = pickDesignHotelsMarshaMatch(censusRow, countryHotels);

    if (match && !match.borderline) {
      const applyFields = buildMarriottDirectoryBackfillFields(
        rec.fields,
        match.directoryRow,
        presentFields
      );
      if (Object.keys(applyFields).length) {
        planRows.push({
          censusRecordId: rec.id,
          censusName: rec.fields.name,
          marshaCode: match.directoryRow.marshaCode,
          marriottUrl: match.directoryRow.website,
          matchPath: match.matchPath,
          matchScore: match.sim,
          marriottListingName: match.directoryRow.name,
          applyFields,
        });
        continue;
      }
    }

    if (match?.borderline) {
      stewardReview.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "borderline_sitemap_match",
        marshaCode: match.directoryRow.marshaCode,
        marriottListingName: match.directoryRow.name,
        matchScore: match.sim,
        website: website || null,
      });
      continue;
    }

    unresolved.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      reason: "no_marsha_on_marriott",
      website: website || null,
    });
  }

  return {
    generatedAt: new Date().toISOString(),
    censusRowsScanned: targets.length,
    marriottSitemapHotels: crawl.hotels.length,
    sitemapFetchErrors: crawl.fetchErrors,
    readyToApply: planRows.length,
    stewardReview,
    unresolved,
    planRows,
  };
}

/**
 * Audit Property ID coverage for CALA Design Hotels.
 */
export async function auditDesignHotelsPropertyIdCoverage() {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [CENSUS_FIELDS.name, CENSUS_FIELDS.country, CENSUS_FIELDS.affiliation, CENSUS_PROPERTY_ID_FIELD];
  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();
  const rows = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION
  );
  const withId = rows.filter((r) => !isBlankCensusValue(r.fields[CENSUS_PROPERTY_ID_FIELD]));
  return {
    total: rows.length,
    withPropertyId: withId.length,
    missingPropertyId: rows.length - withId.length,
  };
}
