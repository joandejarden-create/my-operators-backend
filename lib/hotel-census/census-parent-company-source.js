/**
 * Source-of-truth Parent Company resolution for Hotel Census rows.
 * Brand Setup - Brand Basics (AIRTABLE_BASE_ID) + Brand Alias Mapping + manual overrides.
 */

import Airtable from "airtable";
import {
  exactMatchKey,
  loadActiveBrandAliasRows,
  normalizeParentCompanyKey,
} from "./brand-alias-resolve.js";
import { CENSUS_INDEPENDENT_AFFILIATION } from "./fields.js";

const BRAND_SETUP_TABLE = "Brand Setup - Brand Basics";

/** Affiliation → Parent Company when brand is absent from Brand Setup. */
export const MANUAL_AFFILIATION_PARENT_OVERRIDES = {
  "Fiesta Inn": "Grupo Posadas, S.A.B. DE C.V.",
  "Gamma": "Grupo Posadas, S.A.B. DE C.V.",
  "Gamma by Fiesta Inn": "Grupo Posadas, S.A.B. DE C.V.",
  "Fiesta Americana": "Grupo Posadas, S.A.B. DE C.V.",
  "Grand Fiesta Americana": "Grupo Posadas, S.A.B. DE C.V.",
  "Live Aqua": "Grupo Posadas, S.A.B. DE C.V.",
  "The Explorean": "Grupo Posadas, S.A.B. DE C.V.",
  "One Hotels": "Grupo Posadas, S.A.B. DE C.V.",
};

const HILTON_PARENT_NORM = normalizeParentCompanyKey("Hilton Worldwide");

/**
 * @returns {Promise<{ brandSetupParent: Map<string, string>, hiltonBrandKeys: Set<string> }>}
 */
export async function loadBrandSetupParentIndex() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID for Brand Setup");
  }

  const base = new Airtable({ apiKey }).base(baseId);
  /** @type {Map<string, string>} */
  const brandSetupParent = new Map();
  /** @type {Set<string>} */
  const hiltonBrandKeys = new Set();

  const rows = await base(BRAND_SETUP_TABLE)
    .select({ fields: ["Brand Name", "Parent Company"], pageSize: 100 })
    .all();

  for (const r of rows) {
    const brand = exactMatchKey(r.get("Brand Name"));
    const parent = exactMatchKey(r.get("Parent Company"));
    if (!brand || !parent) continue;
    brandSetupParent.set(brand, parent);
    if (normalizeParentCompanyKey(parent) === HILTON_PARENT_NORM) {
      hiltonBrandKeys.add(brand);
    }
  }

  return { brandSetupParent, hiltonBrandKeys };
}

/**
 * @param {Awaited<ReturnType<typeof loadActiveBrandAliasRows>>} aliasRows
 * @returns {Map<string, string>}
 */
export function buildAliasParentIndex(aliasRows) {
  /** @type {Map<string, string>} */
  const aliasParent = new Map();
  for (const row of aliasRows) {
    if (!row.parentCompany) continue;
    for (const name of [row.canonicalBrandName, row.aliasSourceBrandName]) {
      const k = exactMatchKey(name);
      if (k) aliasParent.set(k, row.parentCompany);
    }
  }
  return aliasParent;
}

/**
 * @param {string} affiliation
 * @param {{ brandSetupParent: Map<string, string>, aliasParent: Map<string, string> }} indexes
 * @returns {{ expectedParent: string, source: string } | null}
 */
export function resolveExpectedParentCompany(affiliation, indexes) {
  const aff = exactMatchKey(affiliation);
  if (!aff) return null;
  if (aff === CENSUS_INDEPENDENT_AFFILIATION) {
    return { expectedParent: "", source: "independent_blank" };
  }

  if (MANUAL_AFFILIATION_PARENT_OVERRIDES[aff]) {
    return {
      expectedParent: MANUAL_AFFILIATION_PARENT_OVERRIDES[aff],
      source: "manual_override",
    };
  }

  if (indexes.aliasParent.has(aff)) {
    return {
      expectedParent: indexes.aliasParent.get(aff),
      source: "brand_alias",
    };
  }

  if (indexes.brandSetupParent.has(aff)) {
    return {
      expectedParent: indexes.brandSetupParent.get(aff),
      source: "brand_setup",
    };
  }

  return null;
}

/**
 * @param {string} affiliation
 * @param {Set<string>} hiltonBrandKeys
 */
export function isHiltonBrandAffiliation(affiliation, hiltonBrandKeys) {
  const aff = exactMatchKey(affiliation);
  if (!aff) return false;
  if (hiltonBrandKeys.has(aff)) return true;
  for (const bk of hiltonBrandKeys) {
    if (aff.includes(bk) || bk.includes(aff)) return true;
  }
  return false;
}

/**
 * @param {string} current
 * @param {string} expected
 */
export function parentCompanyNeedsUpdate(current, expected) {
  const cur = exactMatchKey(current);
  const exp = exactMatchKey(expected);
  if (!exp) return Boolean(cur);
  return normalizeParentCompanyKey(cur) !== normalizeParentCompanyKey(exp);
}

export { HILTON_PARENT_NORM };
