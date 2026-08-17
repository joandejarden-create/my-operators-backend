/**
 * Bulk Marriott census description + amenities from marriott.com HWS subpages.
 * Fallback when /overview/ is Akamai-blocked; not a 1:1 match to overview chips.
 */

import { fetchMarriottSubpageContent, marriottHotelSlugFromOverviewUrl } from "../marriott-subpage-content-fetch.js";
import { marriottOverviewUrlFromWebsite } from "../marriott-hotel-content-fetch.js";
import {
  marshaFromMarriottWebsite,
} from "../marriott-brand-directory-extract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { mapCensusRowForDirectoryMatch } from "./match-brand-directory-to-census.js";
import {
  buildMarriottContentBackfillFields,
} from "./plan-marriott-census-content-backfill.js";
import {
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

function rowNeedsSubpageContent(fields) {
  return (
    isBlankCensusValue(fields?.[CENSUS_DESCRIPTION_FIELD]) ||
    isBlankCensusValue(fields?.[CENSUS_AMENITIES_TEXT_FIELD])
  );
}

function resolveMarsha(censusRow) {
  const fromField = String(censusRow.fields?.[CENSUS_PROPERTY_ID_FIELD] || "")
    .trim()
    .toUpperCase();
  if (fromField) return fromField;
  return marshaFromMarriottWebsite(censusRow.website);
}

function resolveSlug(censusRow) {
  const website = String(
    censusRow.website || censusRow.fields?.[MAP_DIRECTORY_ENRICHMENT.website] || ""
  ).trim();
  return marriottHotelSlugFromOverviewUrl(website) || marriottHotelSlugFromOverviewUrl(marriottOverviewUrlFromWebsite(website));
}

/**
 * @param {object} [opts]
 * @param {number} [opts.limit]
 * @param {number} [opts.fetchDelayMs]
 * @param {boolean} [opts.amenitiesOnly]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planMarriottCensusSubpageBackfill(opts = {}) {
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

  let censusRows = records
    .map(mapCensusRowForDirectoryMatch)
    .filter((r) => rowNeedsSubpageContent(r.fields || {}));

  if (opts.amenitiesOnly) {
    censusRows = censusRows.filter((r) => isBlankCensusValue(r.fields?.[CENSUS_AMENITIES_TEXT_FIELD]));
  }

  if (opts.limit > 0) censusRows = censusRows.slice(0, opts.limit);

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const fetchErrors = [];

  for (let i = 0; i < censusRows.length; i++) {
    const censusRow = censusRows[i];
    const marsha = resolveMarsha(censusRow);
    const slug = resolveSlug(censusRow);
    const website = String(censusRow.website || censusRow.fields?.[MAP_DIRECTORY_ENRICHMENT.website] || "").trim();

    if (opts.onProgress) {
      opts.onProgress(`[${i + 1}/${censusRows.length}] ${censusRow.name} (${marsha || slug || "?"})`);
    }

    if (!slug && !website) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        marsha,
        reason: "missing_website_and_slug",
      });
      continue;
    }

    try {
      const content = await fetchMarriottSubpageContent(website || slug, { marshaCode: marsha });
      if (!content.description && !content.amenitiesText) {
        skipped.push({
          censusRecordId: censusRow.recordId,
          censusName: censusRow.name,
          marsha,
          reason: "no_subpage_content",
          parseErrors: content.parseErrors,
        });
        if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
        continue;
      }

      const applyFields = buildMarriottContentBackfillFields(
        censusRow.fields || {},
        {
          description: content.description,
          amenitiesText: content.amenitiesText,
        },
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
        marshaCode: marsha,
        slug: content.slug || slug,
        website,
        descriptionSuggested: content.description,
        amenitiesTextSuggested: content.amenitiesText,
        contentSource: content.source,
        applyFields,
      });
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
    }

    if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
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
