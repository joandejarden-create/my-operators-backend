/**
 * Plan Marriott census description + amenities backfill from overview pages.
 */

import {
  fetchMarriottHotelContent,
  marriottOverviewUrlFromWebsite,
} from "../marriott-hotel-content-fetch.js";
import { marshaFromMarriottWebsite } from "../marriott-brand-directory-extract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { mapCensusRowForDirectoryMatch } from "./match-brand-directory-to-census.js";
import { normalizeText } from "../independent-census/match-current-census.js";
import {
  buildMarriottFillBlankPatch,
  probeMarriottBackfillFields,
  MAP_MARRIOTT_CENSUS_FIELD_BACKFILL,
} from "./marriott-census-field-backfill-contract.js";
import { isBlankCensusValue, MAP_DIRECTORY_ENRICHMENT } from "./brand-directory-enrichment-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "./hilton-description-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";

const MARRIOTT_PARENT_FORMULA = `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function rowNeedsContent(fields) {
  return (
    isBlankCensusValue(fields?.[CENSUS_DESCRIPTION_FIELD]) ||
    isBlankCensusValue(fields?.[CENSUS_AMENITIES_TEXT_FIELD])
  );
}

/**
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 */
function resolveMarsha(censusRow) {
  const fromField = String(censusRow.fields?.[CENSUS_PROPERTY_ID_FIELD] || "")
    .trim()
    .toUpperCase();
  if (fromField) return fromField;
  return marshaFromMarriottWebsite(censusRow.website);
}

function nameMatchScore(censusName, content) {
  const nameKey = normalizeText(censusName).toLowerCase();
  const slug = String(content.website || "").match(/\/hotels\/([a-z0-9-]+)/i)?.[1] || "";
  if (!slug) return 0;
  const tokens = slug
    .split("-")
    .map((t) => t.trim())
    .filter((t) => t.length > 3 && !/^[a-z]{4,6}$/.test(t));
  if (!tokens.length) return 0;
  const hits = tokens.filter((t) => nameKey.includes(t));
  return hits.length / tokens.length;
}

/**
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>[]} censusRows
 * @param {{ marshaCode?: string, website?: string }} content
 */
function findCensusRowForContent(censusRows, content) {
  const marsha = String(content.marshaCode || "").trim().toUpperCase();
  if (!marsha) return null;

  let hit = censusRows.find((r) => resolveMarsha(r) === marsha);
  if (hit) return hit;

  hit = censusRows.find((r) => {
    const web = String(r.website || "").toLowerCase();
    return web.includes(`/hotels/${marsha.toLowerCase()}-`) || web.includes(`/hotels/${marsha.toLowerCase()}/`);
  });
  if (hit) return hit;

  let best = null;
  let bestScore = 0;
  for (const row of censusRows) {
    const score = nameMatchScore(row.name, content);
    if (score > bestScore) {
      bestScore = score;
      best = row;
    }
  }
  return bestScore >= 0.45 ? best : null;
}

/**
 * @param {object} contentRow
 * @param {string[]} presentWritable
 */
export function buildMarriottContentBackfillFields(censusFields, contentRow, presentWritable) {
  const proposed = {};
  if (contentRow.description) proposed[CENSUS_DESCRIPTION_FIELD] = contentRow.description;
  if (contentRow.amenitiesText) proposed[CENSUS_AMENITIES_TEXT_FIELD] = contentRow.amenitiesText;
  return buildMarriottFillBlankPatch(censusFields, proposed, presentWritable);
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.recordIds]
 * @param {string[]} [opts.marshaCodes]
 * @param {number} [opts.limit]
 * @param {number} [opts.fetchDelayMs]
 * @param {boolean} [opts.usePuppeteer]
 * @param {boolean} [opts.fallbackPuppeteer]
 * @param {ReturnType<import("../marriott-hotel-content-fetch.js").normalizeMarriottContentExport>[]} [opts.contentRows]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planMarriottCensusContentBackfill(opts = {}) {
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

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: MARRIOTT_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  let censusRows = records.map(mapCensusRowForDirectoryMatch);
  const allCensusRows = [...censusRows];
  if (opts.recordIds?.length) {
    const want = new Set(opts.recordIds);
    censusRows = censusRows.filter((r) => want.has(r.recordId));
  }
  if (opts.marshaCodes?.length) {
    const want = new Set(opts.marshaCodes.map((m) => m.toUpperCase()));
    censusRows = censusRows.filter((r) => want.has(resolveMarsha(r)));
  }

  censusRows = censusRows.filter((r) => rowNeedsContent(r.fields || {}));
  if (opts.limit > 0) censusRows = censusRows.slice(0, opts.limit);

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const fetchErrors = [];

  if (opts.contentRows?.length) {
    const exportRows = opts.contentRows.filter((r) => r?.marshaCode);
    for (let i = 0; i < exportRows.length; i++) {
      const content = exportRows[i];
      const marsha = content.marshaCode.toUpperCase();
      if (opts.onProgress) {
        opts.onProgress(`[${i + 1}/${exportRows.length}] export ${marsha}`);
      }

      const candidates = allCensusRows.filter((r) => rowNeedsContent(r.fields || {}));
      const censusRow = findCensusRowForContent(candidates, content);
      if (!censusRow) {
        skipped.push({
          marsha,
          reason: "census_row_not_found",
          sourceFile: content.sourceFile || "",
        });
        continue;
      }

      const website = String(
        censusRow.website ||
          censusRow.fields?.[MAP_DIRECTORY_ENRICHMENT.website] ||
          content.website ||
          ""
      ).trim();
      const applyFields = buildMarriottContentBackfillFields(
        censusRow.fields || {},
        content,
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
        affiliation: censusRow.affiliation,
        country: censusRow.country,
        city: censusRow.city,
        marshaCode: marsha,
        website,
        overviewUrl: content.website || marriottOverviewUrlFromWebsite(website),
        descriptionSuggested: content.description,
        amenitiesTextSuggested: content.amenitiesText,
        contentSource: content.sourceFile ? "overview_html_export" : "content_export",
        applyFields,
      });
    }

    return {
      censusRowsScanned: exportRows.length,
      readyToApply: planRows.length,
      skipped,
      fetchErrors,
      presentWritable,
      targetFields: [
        MAP_MARRIOTT_CENSUS_FIELD_BACKFILL.hotelDescription,
        MAP_MARRIOTT_CENSUS_FIELD_BACKFILL.amenities,
      ],
      planRows,
    };
  }

  for (let i = 0; i < censusRows.length; i++) {
    const censusRow = censusRows[i];
    const marsha = resolveMarsha(censusRow);
    const website = String(censusRow.website || censusRow.fields?.[MAP_DIRECTORY_ENRICHMENT.website] || "").trim();
    const overviewUrl = marriottOverviewUrlFromWebsite(website || marsha);

    if (opts.onProgress) {
      opts.onProgress(`[${i + 1}/${censusRows.length}] ${censusRow.name} (${marsha || "no-marsha"})`);
    }

    if (!marsha && !website) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        reason: "missing_website_and_marsha",
      });
      continue;
    }

    let content = null;
    try {
      const fetched = await fetchMarriottHotelContent(website || marsha, {
        marshaCode: marsha,
        usePuppeteer: opts.usePuppeteer,
        fallbackPuppeteer: opts.fallbackPuppeteer !== false,
      });
      content = {
        marshaCode: fetched.marshaCode || marsha,
        description: fetched.description,
        amenitiesText: fetched.amenitiesText,
        website,
        overviewUrl: fetched.overviewUrl,
        source: fetched.source,
        errors: fetched.errors,
        accessDenied: fetched.accessDenied,
      };
      if (fetched.accessDenied || (!fetched.description && !fetched.amenitiesText)) {
        fetchErrors.push({
          censusRecordId: censusRow.recordId,
          censusName: censusRow.name,
          marsha,
          overviewUrl: fetched.overviewUrl,
          errors: fetched.errors,
        });
        skipped.push({
          censusRecordId: censusRow.recordId,
          censusName: censusRow.name,
          marsha,
          reason: fetched.accessDenied ? "access_denied" : "no_content_parsed",
        });
        if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
        continue;
      }
    } catch (err) {
      fetchErrors.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha,
        error: err?.message || String(err),
      });
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha,
        reason: "fetch_error",
      });
      if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
      continue;
    }

    if (!content) continue;

    const applyFields = buildMarriottContentBackfillFields(
      censusRow.fields || {},
      content,
      presentWritable
    );
    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha,
        reason: "no_fill_blank_fields",
      });
      if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
      continue;
    }

    planRows.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      affiliation: censusRow.affiliation,
      country: censusRow.country,
      city: censusRow.city,
      marshaCode: marsha,
      website,
      overviewUrl: content.overviewUrl || overviewUrl,
      descriptionSuggested: content.description,
      amenitiesTextSuggested: content.amenitiesText,
      contentSource: content.source || "export",
      applyFields,
    });

    if (opts.fetchDelayMs > 0 && !opts.contentRows?.length) await sleep(opts.fetchDelayMs);
  }

  return {
    censusRowsScanned: censusRows.length,
    readyToApply: planRows.length,
    skipped,
    fetchErrors,
    presentWritable,
    targetFields: [
      MAP_MARRIOTT_CENSUS_FIELD_BACKFILL.hotelDescription,
      MAP_MARRIOTT_CENSUS_FIELD_BACKFILL.amenities,
    ],
    planRows,
  };
}

export { MARRIOTT_PARENT_FORMULA };
