import {
  ALIAS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
} from "./fields.js";
import { BRAND_ALIAS_TABLE, getPlatformBase } from "./platform-base.js";

/** Trim only — Phase 1 exact match (case-sensitive as stored in Airtable). */
export function exactMatchKey(value) {
  if (value == null) return "";
  return String(value).trim();
}

function isAliasRowActive(fields) {
  const v = fields[ALIAS_FIELDS.active];
  if (v === true) return true;
  if (v === false || v == null) return false;
  const s = String(v).trim().toLowerCase();
  return s === "yes" || s === "true" || s === "1" || s === "active";
}

function readAliasRow(record) {
  const f = record.fields || {};
  return {
    recordId: record.id,
    canonicalBrandName: exactMatchKey(f[ALIAS_FIELDS.canonicalBrandName]),
    aliasSourceBrandName: exactMatchKey(f[ALIAS_FIELDS.aliasSourceBrandName]),
    parentCompany: exactMatchKey(f[ALIAS_FIELDS.parentCompany]),
    active: isAliasRowActive(f),
    matchConfidence: f[ALIAS_FIELDS.matchConfidence] ?? null,
    notes: f[ALIAS_FIELDS.notes] ?? null,
  };
}

/**
 * Load all active alias rows (read-only). Returns [] if table missing or empty.
 */
export async function loadActiveBrandAliasRows() {
  const base = getPlatformBase();
  if (!base) return [];

  try {
    const records = await base(BRAND_ALIAS_TABLE)
      .select({
        fields: Object.values(ALIAS_FIELDS),
        pageSize: 100,
      })
      .all();

    return records.map(readAliasRow).filter((r) => r.active);
  } catch (err) {
    const msg = err?.message || String(err);
    if (/could not find|not found|unknown field/i.test(msg)) {
      return [];
    }
    throw err;
  }
}

/** Normalize parent company for alias row matching (trim, strip common suffixes, lowercase). */
export function normalizeParentCompanyKey(value) {
  let s = exactMatchKey(value);
  if (!s) return "";
  s = s.replace(/,\s*(Inc\.?|LLC\.?|L\.L\.C\.?|Corp\.?|Corporation)$/i, "");
  s = s.replace(/\s+(Inc\.?|LLC\.?|L\.L\.C\.?)$/i, "");
  s = s.toLowerCase();
  if (s === "choice hotels international" || s === "choice hotels intl") {
    return "choice hotels";
  }
  return s;
}

function parentCompanyMatches(rowParent, requestedParent) {
  const req = normalizeParentCompanyKey(requestedParent);
  if (!req) return true;
  const row = normalizeParentCompanyKey(rowParent);
  if (!row) return true;
  return row === req;
}

/**
 * Resolve canonical brand + affiliation matchers for census queries.
 *
 * @param {string} requestedBrand Brand Explorer / API request name
 * @param {string} [parentCompany] Optional parent company filter
 * @param {object} [opts]
 * @param {BrandAliasRow[]} [opts.preloadedAliases]
 */
export async function resolveBrandAffiliationMatchers(requestedBrand, parentCompany, opts = {}) {
  const requested = exactMatchKey(requestedBrand);
  if (!requested) {
    return {
      ok: false,
      error: "brand query parameter is required",
    };
  }

  const warnings = [];
  const matchPath = [];
  let aliasTableAvailable = true;
  let aliasRows = opts.preloadedAliases;

  if (aliasRows == null) {
    try {
      aliasRows = await loadActiveBrandAliasRows();
    } catch (e) {
      aliasTableAvailable = false;
      aliasRows = [];
      warnings.push("ALIAS_TABLE_UNAVAILABLE: " + (e?.message || String(e)));
    }
  }

  if (aliasRows.length === 0 && aliasTableAvailable) {
    warnings.push("NO_ACTIVE_ALIAS_ROWS: using fallback exact Affiliation match");
  }

  let canonicalBrandName = null;

  for (const row of aliasRows) {
    if (row.canonicalBrandName === requested) {
      canonicalBrandName = row.canonicalBrandName;
      matchPath.push("canonical_name");
      break;
    }
  }

  if (!canonicalBrandName) {
    for (const row of aliasRows) {
      if (row.aliasSourceBrandName === requested) {
        canonicalBrandName = row.canonicalBrandName;
        matchPath.push("alias_name");
        break;
      }
    }
  }

  const scopedRows = canonicalBrandName
    ? aliasRows.filter(
        (r) =>
          r.canonicalBrandName === canonicalBrandName &&
          parentCompanyMatches(r.parentCompany, parentCompany)
      )
    : [];

  let affiliationMatchers = [];
  let aliasRecordsUsed = [];

  if (canonicalBrandName && scopedRows.length > 0) {
    const seen = new Set();
    for (const row of scopedRows) {
      const alias = row.aliasSourceBrandName;
      if (!alias || seen.has(alias)) continue;
      if (alias === CENSUS_INDEPENDENT_AFFILIATION) continue;
      seen.add(alias);
      affiliationMatchers.push(alias);
      aliasRecordsUsed.push({
        recordId: row.recordId,
        aliasSourceBrandName: alias,
        matchConfidence: row.matchConfidence,
        parentCompany: row.parentCompany || null,
      });
    }
    matchPath.push("alias_table");
  } else {
    if (!canonicalBrandName) {
      canonicalBrandName = requested;
      matchPath.push("fallback_requested_name");
      if (aliasRows.length > 0) {
        warnings.push(
          "NO_ALIAS_FOR_REQUESTED_BRAND: no Canonical or Alias row matched requested name; using exact Affiliation = requested brand"
        );
      }
    } else {
      warnings.push(
        "NO_ALIAS_ROWS_FOR_CANONICAL: canonical resolved but no active alias rows after Parent Company filter"
      );
      matchPath.push("fallback_canonical_only");
    }
    affiliationMatchers = [requested];
  }

  return {
    ok: true,
    requestedBrand: requested,
    canonicalBrandName,
    parentCompany: exactMatchKey(parentCompany) || null,
    affiliationMatchers,
    aliasRecordsUsed,
    matchPath,
    warnings,
    aliasTableAvailable,
    usedAliasTable: matchPath.includes("alias_table"),
  };
}

/**
 * @typedef {object} BrandAliasRow
 * @property {string} recordId
 * @property {string} canonicalBrandName
 * @property {string} aliasSourceBrandName
 * @property {string} parentCompany
 * @property {boolean} active
 */
