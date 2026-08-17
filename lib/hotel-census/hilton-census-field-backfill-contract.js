/**
 * Hilton census field backfill contract — fill-blank targets from hilton.com.
 */

import { HOTEL_CENSUS_TABLE } from "./fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue, buildEnrichmentFieldActions } from "./brand-directory-enrichment-contract.js";
import {
  CENSUS_AMENITIES_TEXT_FIELD,
  CENSUS_AMENITY_YN_COLUMNS,
  directoryAmenityIdsToCensusFields,
} from "../hilton-amenity-map.js";
import {
  CENSUS_DESCRIPTION_FIELD,
  MAP_HILTON_DESCRIPTION_ENRICHMENT,
} from "./hilton-description-enrichment-contract.js";

export const CENSUS_YEAR_AFFILIATED_FIELD =
  process.env.AIRTABLE_CENSUS_YEAR_AFFILIATED_FIELD || "Year Affiliated";

/** Formula fields — read-only in Airtable; derived from Year Affiliated / Open Date. */
export const CENSUS_FORMULA_AFFILIATION_FIELDS = [
  "Year & Month Affiliated",
  "Affiliated Month",
];

export const MAP_HILTON_CENSUS_FIELD_BACKFILL = {
  hotelDescription: CENSUS_DESCRIPTION_FIELD,
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
  website: MAP_DIRECTORY_ENRICHMENT.website,
  openDate: MAP_DIRECTORY_ENRICHMENT.openDate,
  yearAffiliated: CENSUS_YEAR_AFFILIATED_FIELD,
};

export const TARGET_BACKFILL_FIELDS = [
  ...Object.values(MAP_HILTON_CENSUS_FIELD_BACKFILL),
  ...CENSUS_AMENITY_YN_COLUMNS,
];

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
export async function probeHiltonBackfillFields(base) {
  const writable = [];
  const formula = [];
  for (const field of TARGET_BACKFILL_FIELDS) {
    if (await fieldExists(base, field)) writable.push(field);
  }
  for (const field of CENSUS_FORMULA_AFFILIATION_FIELDS) {
    if (await fieldExists(base, field)) formula.push(field);
  }
  if (await fieldExists(base, CENSUS_DESCRIPTION_FIELD)) {
    if (!writable.includes(CENSUS_DESCRIPTION_FIELD)) writable.push(CENSUS_DESCRIPTION_FIELD);
  }
  return { writable, formula };
}

/**
 * @param {string|null|undefined} dateStr ISO date
 */
export function yearFromDate(dateStr) {
  const s = String(dateStr || "").trim();
  if (!s) return null;
  const y = Number(s.slice(0, 4));
  return Number.isFinite(y) && y > 1800 && y < 2200 ? y : null;
}

/**
 * @param {number|null} year
 * @param {string|null|undefined} dateStr
 */
export function monthFromDate(dateStr) {
  const s = String(dateStr || "").trim();
  if (!s || s.length < 7) return null;
  const m = Number(s.slice(5, 7));
  return Number.isFinite(m) && m >= 1 && m <= 12 ? m : null;
}

/**
 * @param {Record<string, unknown>} censusFields
 * @param {Record<string, unknown>} proposed
 * @param {string[]} presentFields
 */
export function buildFillBlankPatch(censusFields, proposed, presentFields) {
  const present = new Set(presentFields);
  const filtered = {};
  for (const [k, v] of Object.entries(proposed)) {
    if (!present.has(k)) continue;
    filtered[k] = v;
  }
  const { applyFields } = buildEnrichmentFieldActions(censusFields, filtered, {
    fillBlankOnly: true,
    includeBrandPropertyCode: true,
  });
  return applyFields;
}

/**
 * @param {object} censusFields
 * @param {object} directoryHints — amenitiesText, amenityFlags, website, openDate
 * @param {string[]} presentFields
 */
export function buildDirectoryBackfillFields(censusFields, directoryHints, presentFields) {
  const proposed = {};
  if (directoryHints.website) proposed[MAP_DIRECTORY_ENRICHMENT.website] = directoryHints.website;
  if (directoryHints.amenitiesText) proposed[CENSUS_AMENITIES_TEXT_FIELD] = directoryHints.amenitiesText;
  if (directoryHints.openDate) proposed[MAP_DIRECTORY_ENRICHMENT.openDate] = directoryHints.openDate;

  if (directoryHints.amenityFlags) {
    for (const [col, val] of Object.entries(directoryHints.amenityFlags)) {
      if (val) proposed[col] = val;
    }
  }

  const patch = buildFillBlankPatch(censusFields, proposed, presentFields);

  const openDate =
    patch[MAP_DIRECTORY_ENRICHMENT.openDate] ||
    censusFields[MAP_DIRECTORY_ENRICHMENT.openDate];
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

/**
 * @param {object} censusFields
 * @param {string[]} amenityIds
 * @param {string[]} presentFields
 */
export function buildAmenityBackfillFromIds(censusFields, amenityIds, presentFields) {
  if (!amenityIds?.length) return {};
  const proposed = directoryAmenityIdsToCensusFields(amenityIds);
  return buildFillBlankPatch(censusFields, proposed, presentFields);
}
