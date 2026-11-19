/**
 * Legacy Hotel Census retirement guard (Dealality-wide).
 *
 * Canonical hotel SoT: Hotel Property Census `tbl9aY5ijiuIzzWam`.
 * Legacy table: Hotel Census `tblgj2qEwxjTcg6q0` — blocked for normal
 * production-intended product paths (HE, Radar/Scout, Brand Presence, BE metrics, etc.).
 *
 * Narrow migration reads require explicit opt-in:
 *   DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1
 *   (alias: HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1)
 * and/or call-site `allowLegacyHotelCensusRead: true`.
 *
 * Does not delete the Airtable table. Does not authorize production writes.
 */

export const LEGACY_HOTEL_CENSUS_TABLE_ID = "tblgj2qEwxjTcg6q0";
export const LEGACY_HOTEL_CENSUS_TABLE_NAME = "Hotel Census";
export const HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const LEGACY_CENSUS_GUARD_VERSION = "he-legacy-hotel-census-access-guard-v1";

/** In-memory request audit for instrumented tests. */
const accessLog = [];

export function resetLegacyCensusAccessLog() {
  accessLog.length = 0;
}

export function getLegacyCensusAccessLog() {
  return [...accessLog];
}

export function isLegacyHotelCensusTableRef(tableRef) {
  const raw = String(tableRef || "").trim();
  if (!raw) return false;
  if (raw === LEGACY_HOTEL_CENSUS_TABLE_ID) return true;
  if (raw.toLowerCase() === LEGACY_HOTEL_CENSUS_TABLE_NAME.toLowerCase()) return true;
  // URL path fragment
  if (raw.includes(LEGACY_HOTEL_CENSUS_TABLE_ID)) return true;
  if (/\/Hotel%20Census(\?|$)/i.test(raw) || /\/Hotel Census(\?|$)/i.test(raw)) return true;
  return false;
}

function envFlagTruthy(raw) {
  const env = String(raw || "").trim().toLowerCase();
  return env === "1" || env === "true" || env === "yes";
}

export function migrationReadAllowed(opts = {}) {
  if (opts.allowLegacyHotelCensusRead === true) return true;
  if (opts.forceBlock === true) return false;
  return (
    envFlagTruthy(process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ) ||
    envFlagTruthy(process.env.HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ)
  );
}

/**
 * Assert access to a table is permitted. Throws LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED.
 * @returns {{ allowed: boolean, mode: 'HPC_ONLY'|'MIGRATION_READ' }}
 */
export function assertLegacyHotelCensusAccess(tableRef, opts = {}) {
  const entry = {
    at: new Date().toISOString(),
    tableRef: String(tableRef || ""),
    workflow: opts.workflow || "unspecified",
    method: opts.method || "UNKNOWN",
  };

  if (!isLegacyHotelCensusTableRef(tableRef)) {
    entry.decision = "PASS_NON_LEGACY";
    accessLog.push(entry);
    return { allowed: true, mode: "HPC_ONLY" };
  }

  if (migrationReadAllowed(opts) && String(opts.method || "GET").toUpperCase() === "GET") {
    entry.decision = "ALLOW_MIGRATION_READ";
    accessLog.push(entry);
    return { allowed: true, mode: "MIGRATION_READ" };
  }

  entry.decision = "BLOCK";
  accessLog.push(entry);
  const err = new Error(
    `LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED: Dealality products must not access legacy Hotel Census (${LEGACY_HOTEL_CENSUS_TABLE_ID}). Use Hotel Property Census (${HPC_TABLE_ID}). Set DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1 only for explicit read-only migration tooling.`
  );
  err.code = "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED";
  err.tableId = LEGACY_HOTEL_CENSUS_TABLE_ID;
  throw err;
}

/**
 * Wrap fetch to refuse legacy Hotel Census URLs unless migration read allowed.
 */
export function guardFetch(fetchImpl, opts = {}) {
  const baseFetch = fetchImpl || globalThis.fetch;
  return async function guardedFetch(input, init) {
    const url = typeof input === "string" ? input : input?.url || String(input);
    const method = String(init?.method || "GET").toUpperCase();
    if (isLegacyHotelCensusTableRef(url)) {
      assertLegacyHotelCensusAccess(url, {
        ...opts,
        method,
        workflow: opts.workflow || "fetch",
      });
    }
    return baseFetch(input, init);
  };
}

export function summarizeLegacyCensusIndependence(log = getLegacyCensusAccessLog()) {
  const blocks = log.filter((e) => e.decision === "BLOCK");
  const migrationReads = log.filter((e) => e.decision === "ALLOW_MIGRATION_READ");
  const attempts = log.filter((e) => isLegacyHotelCensusTableRef(e.tableRef));
  return {
    version: LEGACY_CENSUS_GUARD_VERSION,
    totalLogEntries: log.length,
    legacyAttempts: attempts.length,
    blocked: blocks.length,
    migrationReads: migrationReads.length,
    zeroLegacyAccess: attempts.length === 0,
    heLegacyIndependentCandidate: blocks.length === 0 && migrationReads.length === 0,
  };
}
