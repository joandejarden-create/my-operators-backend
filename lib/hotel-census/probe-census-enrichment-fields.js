/**
 * Probe which Hotel Census enrichment columns exist in the live base.
 */

import { HOTEL_CENSUS_TABLE } from "./fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "./brand-directory-enrichment-contract.js";
import {
  CENSUS_AMENITIES_TEXT_FIELD,
  CENSUS_AMENITY_YN_COLUMNS,
} from "../hilton-amenity-map.js";

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
export async function probeCensusEnrichmentFields(base) {
  const core = Object.values(MAP_DIRECTORY_ENRICHMENT);
  const candidates = [...new Set([...core, CENSUS_AMENITIES_TEXT_FIELD, ...CENSUS_AMENITY_YN_COLUMNS])];
  const present = [];
  for (const field of candidates) {
    if (await fieldExists(base, field)) present.push(field);
  }
  return present;
}

/** @param {import('airtable').Base} base */
export async function getCensusEnrichmentSelectFields(base) {
  return probeCensusEnrichmentFields(base);
}
