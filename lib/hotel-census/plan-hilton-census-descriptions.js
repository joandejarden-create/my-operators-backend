/**
 * Plan Hilton GraphQL description fetch for matched Hotel Census rows.
 */

import { fetchHiltonHotelDescription, pickPrimaryHiltonDescription } from "../hilton-hotel-description-fetch.js";
import { crawlHiltonBrandDirectory } from "../hilton-brand-directory-extract.js";
import { resolveHiltonBrandAffiliationMatchers } from "./plan-hilton-brand-enrichment.js";
import {
  matchDirectoryRowsToCensus,
  loadCensusRowsForAffiliations,
  mapCensusRowForDirectoryMatch,
  ctyhocnFromWebsite,
} from "./match-brand-directory-to-census.js";
import {
  buildDescriptionEnrichmentFields,
  ENRICHMENT_SOURCE_HILTON_GRAPHQL,
  CENSUS_DESCRIPTION_FIELD,
  probeCensusDescriptionFields,
} from "./hilton-description-enrichment-contract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {object} opts
 * @param {import('../hilton-brand-directory-extract.js').HiltonBrandDirectoryConfig} opts.brandConfig
 * @param {number} [opts.crawlDelayMs]
 * @param {number} [opts.fetchDelayMs]
 * @param {string} [opts.minConfidence]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planHiltonBrandDescriptions(opts) {
  const {
    brandConfig,
    crawlDelayMs = 200,
    fetchDelayMs = 300,
    minConfidence = "medium",
    onProgress,
  } = opts;

  const brand = brandConfig.canonicalBrandName;
  if (onProgress) onProgress(`Crawling directory for ${brand}...`);
  const crawl = await crawlHiltonBrandDirectory({ brandConfig, delayMs: crawlDelayMs, onProgress });

  const alias = await resolveHiltonBrandAffiliationMatchers(brandConfig);
  const censusLoad = await loadCensusRowsForAffiliations(alias.affiliationMatchers);
  const { matches } = matchDirectoryRowsToCensus(crawl.hotels, censusLoad.rows, { minConfidence });

  const planRows = [];
  const fetchErrors = [];

  for (let i = 0; i < matches.length; i++) {
    const { directoryRow, censusRow, confidence, reason, score } = matches[i];
    if (!censusRow || !directoryRow?.brandPropertyCode) continue;

    const ctyhocn = directoryRow.brandPropertyCode;
    if (onProgress) onProgress(`[${i + 1}/${matches.length}] Fetch description ${ctyhocn} — ${directoryRow.name}`);

    try {
      const descriptionRow = await fetchHiltonHotelDescription(ctyhocn, {
        refererUrl: directoryRow.website,
      });
      const primary = pickPrimaryHiltonDescription(descriptionRow);
      const applyFields = buildDescriptionEnrichmentFields(censusRow.fields, descriptionRow, {
        fillBlankOnly: true,
      });

      planRows.push({
        brand,
        brandCode: brandConfig.brandCode,
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        directoryName: directoryRow.name,
        ctyhocn,
        website: descriptionRow.website || directoryRow.website,
        matchConfidence: confidence,
        matchScore: score,
        matchReason: reason,
        shortDesc: descriptionRow.shortDesc,
        headline: descriptionRow.headline,
        locationShortDesc: descriptionRow.locationShortDesc,
        hotelTeaserText: descriptionRow.hotelTeaserText,
        directionsTo: descriptionRow.directionsTo,
        primaryDescription: primary,
        applyFields,
        source: ENRICHMENT_SOURCE_HILTON_GRAPHQL,
        status: primary ? (Object.keys(applyFields).length ? "ready" : "no_blank_fields") : "empty_description",
      });
    } catch (err) {
      fetchErrors.push({
        ctyhocn,
        censusRecordId: censusRow.recordId,
        name: directoryRow.name,
        error: err?.message || String(err),
      });
      planRows.push({
        brand,
        brandCode: brandConfig.brandCode,
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        directoryName: directoryRow.name,
        ctyhocn,
        status: "fetch_error",
        error: err?.message || String(err),
      });
    }

    if (fetchDelayMs > 0 && i < matches.length - 1) await sleep(fetchDelayMs);
  }

  return {
    brand,
    brandConfig,
    crawlSummary: {
      hotelsFound: crawl.hotelsFound,
      countryPages: crawl.countryPageCount,
      fetchErrors: crawl.fetchErrors,
    },
    censusRowsLoaded: censusLoad.totalLoaded,
    matched: matches.filter((m) => m.censusRow).length,
    descriptionsFetched: planRows.filter((r) => r.primaryDescription).length,
    readyToApply: planRows.filter((r) => r.status === "ready").length,
    fetchErrors,
    planRows,
  };
}

/**
 * Quick plan using existing enrichment match list (directory rows with ctyhocn).
 * @param {Array<{ censusRecordId: string, censusName: string, directoryBrandPropertyCode: string, directoryName?: string, website?: string, fields?: object }>} matchedRows
 */
export async function planDescriptionsForMatchedRows(matchedRows, opts = {}) {
  const { fetchDelayMs = 300, onProgress } = opts;
  const planRows = [];
  const fetchErrors = [];

  for (let i = 0; i < matchedRows.length; i++) {
    const row = matchedRows[i];
    const ctyhocn = String(row.directoryBrandPropertyCode || row.ctyhocn || "").toUpperCase();
    if (!ctyhocn) continue;

    if (onProgress) onProgress(`[${i + 1}/${matchedRows.length}] ${ctyhocn}`);
    try {
      const descriptionRow = await fetchHiltonHotelDescription(ctyhocn, { refererUrl: row.website });
      const primary = pickPrimaryHiltonDescription(descriptionRow);
      const applyFields = buildDescriptionEnrichmentFields(row.fields || {}, descriptionRow, {
        fillBlankOnly: true,
        presentFields: opts.presentFields,
      });
      planRows.push({
        censusRecordId: row.censusRecordId,
        censusName: row.censusName,
        directoryName: row.directoryName,
        ctyhocn,
        primaryDescription: primary,
        shortDesc: descriptionRow.shortDesc,
        headline: descriptionRow.headline,
        applyFields,
        status: primary ? (Object.keys(applyFields).length ? "ready" : "no_blank_fields") : "empty_description",
      });
    } catch (err) {
      fetchErrors.push({ ctyhocn, error: err?.message || String(err) });
    }
    if (fetchDelayMs > 0 && i < matchedRows.length - 1) await sleep(fetchDelayMs);
  }

  return { planRows, fetchErrors };
}

const HILTON_PARENT_FORMULA = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;

/**
 * Resolve Hilton property code from census row (website URL or Brand Property Code).
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 */
export function resolveCensusCtyhocn(censusRow) {
  const fromWebsite = censusRow.websiteCtyhocn || ctyhocnFromWebsite(censusRow.website);
  if (fromWebsite) return fromWebsite;
  const code = String(censusRow.brandPropertyCode || "").trim().toUpperCase();
  if (/^[A-Z0-9]{5,12}$/.test(code)) return code;
  return "";
}

/**
 * Plan descriptions for all Hilton-parent census rows without directory crawl.
 * Uses Website ctyhocn, Brand Property Code, and optional enrichment-plan matches.
 *
 * @param {object} [opts]
 * @param {number} [opts.fetchDelayMs]
 * @param {Map<string, string>} [opts.enrichmentCtyhocnByCensusId] censusRecordId → ctyhocn from prior enrichment plan
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planHiltonCensusDescriptionsFromCensus(opts = {}) {
  const { fetchDelayMs = 300, onProgress, enrichmentCtyhocnByCensusId } = opts;
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const selectFields = await getCensusEnrichmentSelectFields(base);
  if (!selectFields.includes(CENSUS_DESCRIPTION_FIELD)) {
    selectFields.push(CENSUS_DESCRIPTION_FIELD);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: HILTON_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const presentFields = await probeCensusDescriptionFields(base);
  const censusRows = records.map(mapCensusRowForDirectoryMatch);

  const planRows = [];
  const fetchErrors = [];
  const skipped = [];

  for (let i = 0; i < censusRows.length; i++) {
    const censusRow = censusRows[i];
    let ctyhocn = resolveCensusCtyhocn(censusRow);
    let codeSource = ctyhocn ? (censusRow.websiteCtyhocn ? "website" : "brand_property_code") : "";

    if (!ctyhocn && enrichmentCtyhocnByCensusId?.has(censusRow.recordId)) {
      ctyhocn = enrichmentCtyhocnByCensusId.get(censusRow.recordId);
      codeSource = "enrichment_plan";
    }

    if (!ctyhocn) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        reason: "no_ctyhocn",
      });
      continue;
    }

    if (onProgress) {
      onProgress(`[${i + 1}/${censusRows.length}] ${ctyhocn} — ${censusRow.name} (${codeSource})`);
    }

    try {
      const descriptionRow = await fetchHiltonHotelDescription(ctyhocn, {
        refererUrl: censusRow.website || undefined,
      });
      const primary = pickPrimaryHiltonDescription(descriptionRow);
      const applyFields = buildDescriptionEnrichmentFields(censusRow.fields, descriptionRow, {
        fillBlankOnly: true,
        presentFields,
      });

      planRows.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        ctyhocn,
        codeSource,
        website: descriptionRow.website || censusRow.website,
        shortDesc: descriptionRow.shortDesc,
        headline: descriptionRow.headline,
        locationShortDesc: descriptionRow.locationShortDesc,
        hotelTeaserText: descriptionRow.hotelTeaserText,
        primaryDescription: primary,
        applyFields,
        source: ENRICHMENT_SOURCE_HILTON_GRAPHQL,
        status: primary ? (Object.keys(applyFields).length ? "ready" : "no_blank_fields") : "empty_description",
      });
    } catch (err) {
      fetchErrors.push({
        ctyhocn,
        censusRecordId: censusRow.recordId,
        name: censusRow.name,
        error: err?.message || String(err),
      });
      planRows.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        ctyhocn,
        status: "fetch_error",
        error: err?.message || String(err),
      });
    }

    if (fetchDelayMs > 0 && i < censusRows.length - 1) await sleep(fetchDelayMs);
  }

  return {
    scope: "hilton_parent_census",
    censusRowsLoaded: censusRows.length,
    withCtyhocn: planRows.length + fetchErrors.length,
    skippedNoCode: skipped.length,
    descriptionsFetched: planRows.filter((r) => r.primaryDescription).length,
    readyToApply: planRows.filter((r) => r.status === "ready").length,
    fetchErrors,
    skipped,
    planRows,
  };
}
