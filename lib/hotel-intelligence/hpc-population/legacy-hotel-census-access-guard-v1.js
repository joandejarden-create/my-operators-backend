/**
 * Legacy Hotel Census access guard (Dealality-wide) — HPC-ONLY POLICY.
 *
 * Canonical hotel SoT: Hotel Property Census `tbl9aY5ijiuIzzWam`.
 * Legacy table: Hotel Census `tblgj2qEwxjTcg6q0`.
 *
 * FOUNDER CORRECTION (2026-11-20):
 * Hotel Census is NOT a trusted data source. Do not reconcile, import, match,
 * search, or fall back to it. Migration-read exceptions are REMOVED for all
 * ordinary research, enrichment, reconciliation, and product workflows.
 *
 * Record-content queries (GET/POST/PATCH/PUT/DELETE against the table) are
 * always blocked. Schema meta inspection is outside this table API guard.
 *
 * Does not delete the Airtable table. Does not authorize production writes.
 */

export const LEGACY_HOTEL_CENSUS_TABLE_ID = "tblgj2qEwxjTcg6q0";
export const LEGACY_HOTEL_CENSUS_TABLE_NAME = "Hotel Census";
export const HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const LEGACY_CENSUS_GUARD_VERSION = "he-legacy-hotel-census-access-guard-v2-hpc-only";

/** Permanent policy: legacy hotel record contents must not be queried. */
export const LEGACY_HOTEL_CENSUS_DATA_ACCESS_PROHIBITED = true;

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
  if (raw.includes(LEGACY_HOTEL_CENSUS_TABLE_ID)) return true;
  if (/\/Hotel%20Census(\?|$)/i.test(raw) || /\/Hotel Census(\?|$)/i.test(raw)) return true;
  return false;
}

/**
 * @deprecated Migration-read opt-in is permanently disabled for data access.
 * Always returns false. Env flags are ignored for record queries.
 */
export function migrationReadAllowed(_opts = {}) {
  return false;
}

/**
 * Assert access to a table is permitted. Throws LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED
 * for any method against legacy Hotel Census (including former migration GET).
 * @returns {{ allowed: boolean, mode: 'HPC_ONLY' }}
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

  entry.decision = "BLOCK";
  entry.policy = "LEGACY_HOTEL_CENSUS_DATA_ACCESS_PROHIBITED";
  accessLog.push(entry);
  const err = new Error(
    `LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED: Hotel Census (${LEGACY_HOTEL_CENSUS_TABLE_ID}) is not a trusted data source. ` +
      `Do not read, write, search, match, or reconcile it. Use Hotel Property Census (${HPC_TABLE_ID}) and independently verified evidence only. ` +
      `Migration-read exceptions are permanently disabled.`
  );
  err.code = "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED";
  err.tableId = LEGACY_HOTEL_CENSUS_TABLE_ID;
  throw err;
}

/**
 * Wrap fetch to refuse all legacy Hotel Census URLs.
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
    policy: "HPC_ONLY_ZERO_LEGACY_DATA_ACCESS",
    legacyDataAccessProhibited: LEGACY_HOTEL_CENSUS_DATA_ACCESS_PROHIBITED,
    totalLogEntries: log.length,
    legacyAttempts: attempts.length,
    blocked: blocks.length,
    migrationReads: migrationReads.length,
    zeroLegacyAccess: attempts.length === 0,
    heLegacyIndependentCandidate: blocks.length === 0 && migrationReads.length === 0,
  };
}
