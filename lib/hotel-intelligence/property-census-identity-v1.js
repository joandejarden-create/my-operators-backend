/**
 * Resolve which property table owns an Airtable record id.
 *
 * Airtable REST GET /v0/{base}/{table}/{recordId} ignores table path and can
 * return the record under the wrong table name. Ownership must use
 * filterByFormula RECORD_ID()='…' (or Meta), not GET-by-id alone.
 */
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "./radar-internal-preview-hotels-v1.js";

export const HOTEL_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_CENSUS_TABLE || "Hotel Census";

export const PROPERTY_IDENTITY_CONTRACT = "HE_PROPERTY_CENSUS_IDENTITY_V1";

/**
 * @param {import('airtable').Base} airtableBase
 * @param {string} recordId
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
export async function resolvePropertyCensusOwnership(airtableBase, recordId) {
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

  const [censusRec, hpcRec] = await Promise.all([
    findIn(HOTEL_CENSUS_TABLE),
    findIn(HOTEL_PROPERTY_CENSUS_TABLE),
  ]);

  const censusFound = Boolean(censusRec);
  const hpcFound = Boolean(hpcRec);

  if (hpcFound && !censusFound) {
    return {
      ok: true,
      recordId: id,
      owningTable: HOTEL_PROPERTY_CENSUS_TABLE,
      owningTableKind: "HPC",
      record: hpcRec,
      censusFound,
      hpcFound,
    };
  }
  if (censusFound && !hpcFound) {
    return {
      ok: true,
      recordId: id,
      owningTable: HOTEL_CENSUS_TABLE,
      owningTableKind: "HOTEL_CENSUS",
      record: censusRec,
      censusFound,
      hpcFound,
    };
  }
  if (hpcFound && censusFound) {
    // Prefer HPC as canonical SoT when both unexpectedly match.
    return {
      ok: true,
      recordId: id,
      owningTable: HOTEL_PROPERTY_CENSUS_TABLE,
      owningTableKind: "AMBIGUOUS",
      record: hpcRec,
      censusFound,
      hpcFound,
      warning: "RECORD_ID matched both Hotel Census and Hotel Property Census; preferring HPC",
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
    error: "not_found",
  };
}
