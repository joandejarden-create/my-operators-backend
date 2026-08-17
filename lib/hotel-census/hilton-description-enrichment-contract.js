/**
 * Hilton hotel description → Hotel Census write contract.
 */

import { pickPrimaryHiltonDescription } from "../hilton-hotel-description-fetch.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { HOTEL_CENSUS_TABLE } from "./fields.js";

export const ENRICHMENT_SOURCE_HILTON_GRAPHQL = "hilton_graphql_facility_overview";

export const CENSUS_DESCRIPTION_FIELD =
  process.env.AIRTABLE_CENSUS_DESCRIPTION_FIELD || "Hotel Description";

export const CENSUS_HEADLINE_FIELD =
  process.env.AIRTABLE_CENSUS_HOTEL_HEADLINE_FIELD || "Hotel Headline";

export const MAP_HILTON_DESCRIPTION_ENRICHMENT = {
  description: CENSUS_DESCRIPTION_FIELD,
  headline: CENSUS_HEADLINE_FIELD,
};

/** @param {import('airtable').Base} base */
export async function probeCensusDescriptionFields(base) {
  const present = [];
  for (const field of Object.values(MAP_HILTON_DESCRIPTION_ENRICHMENT)) {
    try {
      await base(HOTEL_CENSUS_TABLE).select({ fields: [field], maxRecords: 1 }).firstPage();
      present.push(field);
    } catch (err) {
      const msg = err?.message || String(err);
      if (!/unknown field|not found|invalid/i.test(msg)) throw err;
    }
  }
  return present;
}

/**
 * @param {object} censusFields
 * @param {object} descriptionRow — from fetchHiltonHotelDescription
 * @param {{ fillBlankOnly?: boolean, presentFields?: string[] }} [opts]
 */
export function buildDescriptionEnrichmentFields(censusFields, descriptionRow, opts = {}) {
  const fillBlankOnly = opts.fillBlankOnly !== false;
  const present = new Set(opts.presentFields || Object.values(MAP_HILTON_DESCRIPTION_ENRICHMENT));
  const primary = pickPrimaryHiltonDescription(descriptionRow);
  /** @type {Record<string, string>} */
  const fields = {};

  if (present.has(CENSUS_DESCRIPTION_FIELD) && primary) {
    const current = censusFields?.[CENSUS_DESCRIPTION_FIELD];
    if (!fillBlankOnly || isBlankCensusValue(current)) {
      fields[CENSUS_DESCRIPTION_FIELD] = primary;
    }
  }

  if (present.has(CENSUS_HEADLINE_FIELD) && descriptionRow.headline) {
    const current = censusFields?.[CENSUS_HEADLINE_FIELD];
    if (!fillBlankOnly || isBlankCensusValue(current)) {
      fields[CENSUS_HEADLINE_FIELD] = descriptionRow.headline;
    }
  }

  return fields;
}
