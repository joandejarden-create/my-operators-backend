/**
 * Marriott census field backfill contract — fill-blank targets from marriott.com sitemaps.
 */

import { HOTEL_CENSUS_TABLE } from "./fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue, buildEnrichmentFieldActions } from "./brand-directory-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "./hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import {
  CENSUS_YEAR_AFFILIATED_FIELD,
  yearFromDate,
} from "./hilton-census-field-backfill-contract.js";

export const ENRICHMENT_SOURCE_MARRIOTT_SITEMAP = "marriott_country_sitemap";

export const MAP_MARRIOTT_CENSUS_FIELD_BACKFILL = {
  hotelDescription: CENSUS_DESCRIPTION_FIELD,
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
  website: MAP_DIRECTORY_ENRICHMENT.website,
  openDate: MAP_DIRECTORY_ENRICHMENT.openDate,
  yearAffiliated: CENSUS_YEAR_AFFILIATED_FIELD,
  propertyId: CENSUS_PROPERTY_ID_FIELD,
};

export const TARGET_MARRIOTT_BACKFILL_FIELDS = Object.values(MAP_MARRIOTT_CENSUS_FIELD_BACKFILL);

/** @param {import('airtable').Base} base */
async function fieldExists(base, fieldName) {
  try {
    await base(HOTEL_CENSUS_TABLE).select({ fields: [fieldName], maxRecords: 1 }).firstPage();
    return true;
  } catch (err) {
    const msg = err?.message || String(err);
    if (/unknown field|not found|invalid/i.test(msg)) return false;
    throw err;
  }
}

/** @param {import('airtable').Base} base */
export async function probeMarriottBackfillFields(base) {
  const writable = [];
  for (const field of TARGET_MARRIOTT_BACKFILL_FIELDS) {
    if (await fieldExists(base, field)) writable.push(field);
  }
  return { writable };
}

/**
 * @param {Record<string, unknown>} censusFields
 * @param {Record<string, unknown>} proposed
 * @param {string[]} presentFields
 */
export function buildMarriottFillBlankPatch(censusFields, proposed, presentFields) {
  const present = new Set(presentFields);
  const filtered = {};
  for (const [k, v] of Object.entries(proposed)) {
    if (!present.has(k)) continue;
    filtered[k] = v;
  }
  const { applyFields } = buildEnrichmentFieldActions(censusFields, filtered, {
    fillBlankOnly: true,
    includeBrandPropertyCode: false,
  });
  return applyFields;
}

/**
 * @param {object} censusFields
 * @param {ReturnType<import("../marriott-brand-directory-extract.js").normalizeMarriottDirectoryHotel>} directoryRow
 * @param {string[]} presentFields
 */
export function buildMarriottDirectoryBackfillFields(censusFields, directoryRow, presentFields) {
  const proposed = {};
  if (directoryRow.website) proposed[MAP_DIRECTORY_ENRICHMENT.website] = directoryRow.website;
  if (directoryRow.marshaCode) proposed[CENSUS_PROPERTY_ID_FIELD] = directoryRow.marshaCode;
  if (directoryRow.description) proposed[CENSUS_DESCRIPTION_FIELD] = directoryRow.description;
  if (directoryRow.amenitiesText) proposed[CENSUS_AMENITIES_TEXT_FIELD] = directoryRow.amenitiesText;
  if (directoryRow.openDate) proposed[MAP_DIRECTORY_ENRICHMENT.openDate] = directoryRow.openDate;

  const patch = buildMarriottFillBlankPatch(censusFields, proposed, presentFields);
  const openDate =
    patch[MAP_DIRECTORY_ENRICHMENT.openDate] || censusFields[MAP_DIRECTORY_ENRICHMENT.openDate];
  const year = yearFromDate(openDate);
  if (
    year &&
    presentFields.includes(CENSUS_YEAR_AFFILIATED_FIELD) &&
    isBlankCensusValue(censusFields[CENSUS_YEAR_AFFILIATED_FIELD])
  ) {
    patch[CENSUS_YEAR_AFFILIATED_FIELD] = year;
  }
  return patch;
}
