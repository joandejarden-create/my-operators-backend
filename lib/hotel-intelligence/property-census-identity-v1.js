/**
 * Resolve which property table owns an Airtable record id.
 *
 * Airtable REST GET /v0/{base}/{table}/{recordId} ignores table path and can
 * return the record under the wrong table name. Ownership must use
 * filterByFormula RECORD_ID()='…' (or Meta), not GET-by-id alone.
 *
 * Default: HPC-only (legacy Hotel Census independence). Optional migration
 * read of Hotel Census when allowLegacyHotelCensusRead / env flag is set.
 */
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "./radar-internal-preview-hotels-v1.js";
import {
  assertLegacyHotelCensusAccess,
  migrationReadAllowed,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
} from "./hpc-population/legacy-hotel-census-access-guard-v1.js";

export const HOTEL_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_CENSUS_TABLE || LEGACY_HOTEL_CENSUS_TABLE_NAME;

export const PROPERTY_IDENTITY_CONTRACT = "HE_PROPERTY_CENSUS_IDENTITY_V1";

/**
 * @param {import('airtable').Base} airtableBase
 * @param {string} recordId
 * @param {object} [opts]
 * @param {boolean} [opts.allowLegacyHotelCensusRead]
 * @returns {Promise<{
 *   ok: boolean,
 *   recordId: string,
 *   owningTable: string|null,
 *   owningTableKind: 'HPC'|'HOTEL_CENSUS'|'AMBIGUOUS'|'NOT_FOUND'|null,
 *   record: object|null,
 *   censusFound: boolean,
 *   hpcFound: boolean,
 *   error?: string,
 * }>}
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

  const formula = `RECORD_ID()='${id.replace(/'/g, "\\'")}'`;
  const allowLegacy = migrationReadAllowed(opts);

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

  // HPC first — never touch legacy census unless explicitly allowed.
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

  if (!allowLegacy) {
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
      note: "Legacy Hotel Census not probed — HE independence mode",
    };
  }

  assertLegacyHotelCensusAccess(HOTEL_CENSUS_TABLE, {
    method: "GET",
    workflow: "property-census-identity-v1",
    allowLegacyHotelCensusRead: true,
  });

  const censusRec = await findIn(HOTEL_CENSUS_TABLE);
  const censusFound = Boolean(censusRec);

  if (censusFound) {
    return {
      ok: true,
      recordId: id,
      owningTable: HOTEL_CENSUS_TABLE,
      owningTableKind: "HOTEL_CENSUS",
      record: censusRec,
      censusFound: true,
      hpcFound: false,
      legacyCensusProbed: true,
      warning: "Legacy Hotel Census match — migrate to HPC; not HE runtime SoT",
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
    legacyCensusProbed: true,
    error: "not_found",
  };
}
