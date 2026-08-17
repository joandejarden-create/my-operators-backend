/**
 * Hilton ctyhocn → Hotel Census Property ID field contract.
 */

import { HOTEL_CENSUS_TABLE } from "./fields.js";

export const CENSUS_PROPERTY_ID_FIELD =
  process.env.AIRTABLE_CENSUS_PROPERTY_ID_FIELD || "Property ID";

/** @param {import('airtable').Base} base */
export async function probeCensusPropertyIdField(base) {
  try {
    await base(HOTEL_CENSUS_TABLE)
      .select({ fields: [CENSUS_PROPERTY_ID_FIELD], maxRecords: 1 })
      .firstPage();
    return true;
  } catch (err) {
    const msg = err?.message || String(err);
    if (/unknown field|not found|invalid/i.test(msg)) return false;
    throw err;
  }
}

/**
 * Normalize census Property ID for comparison with Hilton ctyhocn.
 * @param {unknown} value
 */
export function normalizeCensusPropertyId(value) {
  const s = String(value ?? "").trim();
  if (!s) return "";
  return s.toUpperCase();
}
