/**
 * Short-TTL in-memory cache for Scout read paths (Hotel Census Legacy or HPC).
 * Avoids duplicate full-table .all() calls within a single page load / API request chain.
 *
 * P8.8: options.useHpc → Hotel Property Census via scout-hpc-adapter (0 Legacy reads).
 */

import { CENSUS_FIELDS } from "../hotel-census/fields.js";
import { HOTEL_CENSUS_TABLE, getPlatformBase } from "../hotel-census/platform-base.js";
import {
  fetchScoutHpcRecords,
  scoutCensusReadCounters,
} from "./scout-hpc-adapter.js";

/** Union of fields used across Scout market-map, market-coverage, and opportunity-signals. */
export const SCOUT_CENSUS_CACHE_FIELDS = [
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.affiliation,
  CENSUS_FIELDS.parentCompany,
  CENSUS_FIELDS.status,
  CENSUS_FIELDS.rooms,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.market,
  CENSUS_FIELDS.submarket,
  CENSUS_FIELDS.chainScale,
  CENSUS_FIELDS.location,
  CENSUS_FIELDS.operationType,
  CENSUS_FIELDS.managementCompany,
  CENSUS_FIELDS.projectPhase,
  "Latitude",
  "Longitude",
  "Ex-Affiliation",
  "Ex-Affiliation 2",
  "Open Date",
  "projected_open_date",
];

const OPTIONAL_CACHE_FIELDS = [
  "Ex-Affiliation",
  "Ex-Affiliation 2",
  "Open Date",
  "projected_open_date",
];

const DEFAULT_TTL_MS = 5 * 60 * 1000;

/** @type {Map<string, { records: object[], fieldsLoaded: string[], fetchedAt: number, warnings: string[], recordCount: number }>} */
const cacheByKey = new Map();
/** @type {Map<string, Promise<object>>} */
const inflightByKey = new Map();

function cacheKeyForOptions(options = {}) {
  const source = options.useHpc ? "hpc" : "legacy";
  const country = String(options.country || "").trim();
  const market = String(options.market || "").trim();
  const city = String(options.city || "").trim();
  if (country && market) {
    return `${source}|country:${country.toLowerCase()}|market:${market.toLowerCase()}`;
  }
  if (country && city) {
    return `${source}|country:${country.toLowerCase()}|city:${city.toLowerCase()}`;
  }
  if (country) return `${source}|country:${country.toLowerCase()}`;
  return `${source}|__all__`;
}

function escapeFormulaString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function buildGeographyFormula(options = {}) {
  const parts = [];
  const country = String(options.country || "").trim();
  const market = String(options.market || "").trim();
  if (country) {
    parts.push(`{${CENSUS_FIELDS.country}}='${escapeFormulaString(country)}'`);
  }
  if (market) {
    parts.push(`{${CENSUS_FIELDS.market}}='${escapeFormulaString(market)}'`);
  }
  if (!parts.length) return null;
  return parts.length === 1 ? parts[0] : `AND(${parts.join(",")})`;
}

function isUnknownFieldError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return msg.includes("unknown field") || msg.includes("invalid field");
}

/**
 * @param {{ ttlMs?: number, forceRefresh?: boolean, country?: string, market?: string, city?: string, useHpc?: boolean }} [options]
 * @returns {Promise<{ records: object[], fieldsLoaded: string[], fetchedAt: number, warnings: string[], recordCount: number, source?: object }>}
 */
export async function fetchScoutCensusRecords(options = {}) {
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
    if (options.useHpc) {
      const hpcEntry = await fetchScoutHpcRecords({
        country: options.country,
        city: options.city,
        market: options.market,
      });
      const entry = {
        ...hpcEntry,
        useHpc: true,
      };
      cacheByKey.set(key, entry);
      inflightByKey.delete(key);
      return entry;
    }

    const base = getPlatformBase();
    if (!base) {
      throw new Error("Platform base not configured");
    }

    let selectFields = [...SCOUT_CENSUS_CACHE_FIELDS];
    let records;
    const warnings = [];
    const selectOptions = { fields: selectFields, pageSize: 100 };
    const geoFormula = buildGeographyFormula(options);
    if (geoFormula) {
      selectOptions.filterByFormula = geoFormula;
    } else {
      warnings.push(
        "SCOUT_CENSUS_FULL_TABLE: no country filter — loading full Hotel Census (slow). Set Country filter to speed up Scout."
      );
    }

    try {
      records = await base(HOTEL_CENSUS_TABLE).select(selectOptions).all();
      scoutCensusReadCounters.legacy += 1;
    } catch (err) {
      if (!isUnknownFieldError(err)) throw err;
      selectFields = SCOUT_CENSUS_CACHE_FIELDS.filter((f) => !OPTIONAL_CACHE_FIELDS.includes(f));
      warnings.push(
        "SCOUT_CENSUS_CACHE: optional Ex-Affiliation / date fields omitted from cache load."
      );
      records = await base(HOTEL_CENSUS_TABLE)
        .select({ ...selectOptions, fields: selectFields })
        .all();
      scoutCensusReadCounters.legacy += 1;
    }

    const entry = {
      records,
      fieldsLoaded: selectFields,
      fetchedAt: Date.now(),
      warnings,
      recordCount: records.length,
      useHpc: false,
      source: {
        base: "AIRTABLE_BASE_ID_ALT",
        table: HOTEL_CENSUS_TABLE,
        hotelSource: "Hotel Census",
        censusSource: "legacy",
        readOnly: true,
        writes: false,
      },
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

/** Dev/tests only — clear cached census rows. */
export function clearScoutCensusCache() {
  cacheByKey.clear();
  inflightByKey.clear();
}
