/**
 * Resolve which property table owns an Airtable record id.
 *
 * Airtable REST GET /v0/{base}/{table}/{recordId} ignores table path and can
 * return the record under the wrong table name. Ownership must use
 * filterByFormula RECORD_ID()='…' (or Meta), not GET-by-id alone.
 *
 * HPC-only (founder correction 2026-11-20): never probe Hotel Census.
 */
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "./radar-internal-preview-hotels-v1.js";
import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
} from "./hpc-population/legacy-hotel-census-access-guard-v1.js";

/** @deprecated Constant retained for tests/docs — never query this table. */
export const HOTEL_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_CENSUS_TABLE || LEGACY_HOTEL_CENSUS_TABLE_NAME;

export const PROPERTY_IDENTITY_CONTRACT = "HE_PROPERTY_CENSUS_IDENTITY_V1";

/**
 * @param {import('airtable').Base} airtableBase
 * @param {string} recordId
 * @param {object} [opts]
 * @param {boolean} [opts.allowLegacyHotelCensusRead] — if true, throws (prohibited)
 */
export async function resolvePropertyCensusOwnership(airtableBase, recordId, opts = {}) {
  const id = String(recordId || "").trim();
  if (!id.startsWith("rec") || !airtableBase) {
    return {
      ok: false,
      recordId: id,
      owningTable: null,
      owningTableKind: null,
      record: null,
      censusFound: false,
      hpcFound: false,
      error: "invalid_args",
    };
  }

  if (opts.allowLegacyHotelCensusRead === true) {
    assertLegacyHotelCensusAccess(HOTEL_CENSUS_TABLE, {
      method: "GET",
      workflow: "property-census-identity-v1-prohibited-legacy",
    });
  }

  const formula = `RECORD_ID()='${id.replace(/'/g, "\\'")}'`;

  async function findIn(tableName) {
    try {
      const rows = await airtableBase(tableName)
        .select({ filterByFormula: formula, maxRecords: 1 })
        .firstPage();
      return rows?.[0] || null;
    } catch (err) {
      if (err?.statusCode === 404) return null;
      throw err;
    }
  }

  const hpcRec = await findIn(HOTEL_PROPERTY_CENSUS_TABLE);
  const hpcFound = Boolean(hpcRec);

  if (hpcFound) {
    return {
      ok: true,
      recordId: id,
      owningTable: HOTEL_PROPERTY_CENSUS_TABLE,
      owningTableKind: "HPC",
      record: hpcRec,
      censusFound: false,
      hpcFound: true,
      legacyCensusProbed: false,
    };
  }

  return {
    ok: false,
    recordId: id,
    owningTable: null,
    owningTableKind: "NOT_FOUND",
    record: null,
    censusFound: false,
    hpcFound: false,
    legacyCensusProbed: false,
    error: "not_found_hpc_only",
    note: "Legacy Hotel Census not probed — HPC-only policy",
  };
}
