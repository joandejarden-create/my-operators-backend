/**
 * Short-TTL in-memory cache for Scout read paths.
 *
 * Canonical source: Hotel Property Census (`tbl9aY5ijiuIzzWam`).
 * Records are adapted to Scout/legacy field shapes so aggregations stay stable.
 * Legacy Hotel Census is not queried on this path.
 */

import { getPlatformBase } from "../hotel-census/platform-base.js";
import {
  SCOUT_HPC_SELECT_FIELDS,
  HPC_RUNTIME_TABLE_NAME,
  buildHpcGeographyFormula,
  mapHpcRecordToScoutCensusShape,
} from "../hotel-census/hpc-scout-record-adapter-v1.js";
import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
} from "../hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";

/** @deprecated Scout now loads HPC; kept for import compatibility. */
export const SCOUT_CENSUS_CACHE_FIELDS = SCOUT_HPC_SELECT_FIELDS;

const DEFAULT_TTL_MS = 5 * 60 * 1000;

/** @type {Map<string, { records: object[], fieldsLoaded: string[], fetchedAt: number, warnings: string[], recordCount: number, sourceTable: string }>} */
const cacheByKey = new Map();
/** @type {Map<string, Promise<object>>} */
const inflightByKey = new Map();

function cacheKeyForOptions(options = {}) {
  const country = String(options.country || "").trim();
  const market = String(options.market || "").trim();
  if (country && market) return `hpc|country:${country.toLowerCase()}|market:${market.toLowerCase()}`;
  if (country) return `hpc|country:${country.toLowerCase()}`;
  return "hpc|__all__";
}

/**
 * @param {{ ttlMs?: number, forceRefresh?: boolean, country?: string, market?: string }} [options]
 * @returns {Promise<{ records: object[], fieldsLoaded: string[], fetchedAt: number, warnings: string[], recordCount: number, sourceTable: string, sourceTableId: string }>}
 */
export async function fetchScoutCensusRecords(options = {}) {
  // Fail closed if a caller still tries to force legacy table through this module.
  if (options.forceLegacyHotelCensus === true) {
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "GET",
      workflow: "scout-census-read-cache",
      forceBlock: true,
    });
  }

  const ttlMs = options.ttlMs ?? DEFAULT_TTL_MS;
  const now = Date.now();
  const key = cacheKeyForOptions(options);

  const cached = cacheByKey.get(key);
  if (!options.forceRefresh && cached && now - cached.fetchedAt < ttlMs) {
    return cached;
  }

  const inflight = inflightByKey.get(key);
  if (!options.forceRefresh && inflight) {
    return inflight;
  }

  const loadPromise = (async () => {
    const base = getPlatformBase();
    if (!base) {
      throw new Error("Platform base not configured");
    }

    const selectFields = [...SCOUT_HPC_SELECT_FIELDS];
    const warnings = [];
    const selectOptions = { fields: selectFields, pageSize: 100 };
    const geoFormula = buildHpcGeographyFormula(options);
    if (geoFormula) {
      selectOptions.filterByFormula = geoFormula;
    } else {
      warnings.push(
        "SCOUT_HPC_FULL_TABLE: no country filter — loading full Hotel Property Census (slow). Set Country filter to speed up Scout."
      );
    }

    const rawRecords = await base(HPC_RUNTIME_TABLE_NAME).select(selectOptions).all();
    const records = rawRecords.map(mapHpcRecordToScoutCensusShape);

    const entry = {
      records,
      fieldsLoaded: selectFields,
      fetchedAt: Date.now(),
      warnings,
      recordCount: records.length,
      sourceTable: HPC_RUNTIME_TABLE_NAME,
      sourceTableId: "tbl9aY5ijiuIzzWam",
      legacyHotelCensusRuntimeDependency: false,
    };
    cacheByKey.set(key, entry);
    inflightByKey.delete(key);
    return entry;
  })();

  inflightByKey.set(key, loadPromise);
  try {
    return await loadPromise;
  } catch (err) {
    inflightByKey.delete(key);
    throw err;
  }
}

/** Dev/tests only — clear cached HPC/scout rows. */
export function clearScoutCensusCache() {
  cacheByKey.clear();
  inflightByKey.clear();
}
