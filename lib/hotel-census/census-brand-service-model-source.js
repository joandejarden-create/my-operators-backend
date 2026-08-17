/**
 * Brand Setup → Hotel Census Hotel Service Model inheritance (branded rows only).
 * Source: Brand Setup - Brand Basics on AIRTABLE_BASE_ID.
 */

import Airtable from "airtable";
import {
  exactMatchKey,
  loadActiveBrandAliasRows,
} from "./brand-alias-resolve.js";
import { CENSUS_INDEPENDENT_AFFILIATION } from "./fields.js";
import { BRAND_BASICS_FIELDS, BRAND_BASICS_TABLE } from "../independent-census/brand-setup-cala-inventory.js";

/** Allowed Hotel Census single-select options (must match Airtable schema). */
export const CENSUS_HOTEL_SERVICE_MODEL_OPTIONS = [
  "Full-Service",
  "Select-Service",
  "Extended Stay",
  "All-Inclusive",
  "Lifestyle / Boutique",
  "All-Inclusive Resort",
  "Lifestyle / Full-Service",
  "Lifestyle / Wellness Resort",
  "Lifestyle / Select-Service",
];

const CENSUS_OPTION_KEYS = new Set(
  CENSUS_HOTEL_SERVICE_MODEL_OPTIONS.map((v) => normalizeServiceModelKey(v))
);

/** Brand Setup / legacy label variants → census option */
const SERVICE_MODEL_ALIASES = {
  "full service": "Full-Service",
  "full-service": "Full-Service",
  "select service": "Select-Service",
  "select-service": "Select-Service",
  "extended stay": "Extended Stay",
  "all inclusive": "All-Inclusive",
  "all-inclusive": "All-Inclusive",
  "all inclusive resort": "All-Inclusive Resort",
  "lifestyle boutique": "Lifestyle / Boutique",
  "lifestyle / boutique": "Lifestyle / Boutique",
  "lifestyle full service": "Lifestyle / Full-Service",
  "lifestyle / full service": "Lifestyle / Full-Service",
  "lifestyle / full-service": "Lifestyle / Full-Service",
  "lifestyle wellness resort": "Lifestyle / Wellness Resort",
  "lifestyle / wellness resort": "Lifestyle / Wellness Resort",
  "lifestyle select service": "Lifestyle / Select-Service",
  "lifestyle / select service": "Lifestyle / Select-Service",
  "lifestyle / select-service": "Lifestyle / Select-Service",
};

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) {
    return v
      .map((item) => (typeof item === "string" ? item.trim() : item?.name || ""))
      .filter(Boolean)
      .join("; ");
  }
  if (typeof v === "object" && v.name) return String(v.name).trim();
  return String(v).trim();
}

export function normalizeServiceModelKey(value) {
  return valueToStr(value)
    .toLowerCase()
    .replace(/[^a-z0-9/]+/g, " ")
    .replace(/\s*\/\s*/g, " / ")
    .trim();
}

/**
 * @param {string} raw — Brand Setup or census value
 * @returns {string|null} Canonical census select option
 */
export function normalizeServiceModelForCensus(raw) {
  const text = valueToStr(raw);
  if (!text) return null;

  if (CENSUS_HOTEL_SERVICE_MODEL_OPTIONS.includes(text)) return text;

  const key = normalizeServiceModelKey(text);
  if (CENSUS_OPTION_KEYS.has(key)) {
    return CENSUS_HOTEL_SERVICE_MODEL_OPTIONS.find(
      (opt) => normalizeServiceModelKey(opt) === key
    ) || null;
  }

  if (SERVICE_MODEL_ALIASES[key]) return SERVICE_MODEL_ALIASES[key];

  return null;
}

/**
 * @returns {Promise<Map<string, string>>} Brand Name → census Hotel Service Model
 */
export async function loadBrandSetupServiceModelIndex() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID for Brand Setup");
  }

  const base = new Airtable({ apiKey }).base(baseId);
  /** @type {Map<string, string>} */
  const index = new Map();
  /** @type {Map<string, string>} */
  const invalidBrandValues = new Map();

  const rows = await base(BRAND_BASICS_TABLE)
    .select({
      fields: [BRAND_BASICS_FIELDS.brandName, BRAND_BASICS_FIELDS.serviceModel],
      pageSize: 100,
    })
    .all();

  for (const r of rows) {
    const brand = exactMatchKey(r.get(BRAND_BASICS_FIELDS.brandName));
    const rawServiceModel = valueToStr(r.get(BRAND_BASICS_FIELDS.serviceModel));
    if (!brand || !rawServiceModel) continue;

    const normalized = normalizeServiceModelForCensus(rawServiceModel);
    if (normalized) {
      index.set(brand, normalized);
    } else {
      invalidBrandValues.set(brand, rawServiceModel);
    }
  }

  return { index, invalidBrandValues, brandSetupRows: rows.length };
}

/**
 * @param {Awaited<ReturnType<typeof loadActiveBrandAliasRows>>} aliasRows
 * @returns {Map<string, string>} affiliation or alias → canonical brand name
 */
export function buildAffiliationToCanonicalIndex(aliasRows) {
  /** @type {Map<string, string>} */
  const map = new Map();
  for (const row of aliasRows) {
    const canonical = exactMatchKey(row.canonicalBrandName);
    if (!canonical) continue;
    map.set(canonical, canonical);
    const alias = exactMatchKey(row.aliasSourceBrandName);
    if (alias) map.set(alias, canonical);
  }
  return map;
}

/**
 * Resolve census Affiliation → Brand Setup service model (branded + mapped only).
 *
 * @param {string} affiliation
 * @param {Map<string, string>} affiliationToCanonical
 * @param {Map<string, string>} brandSetupServiceModel
 * @returns {{ canonicalBrand: string, serviceModel: string, matchSource: string } | null}
 */
export function resolveBrandSetupServiceModel(
  affiliation,
  affiliationToCanonical,
  brandSetupServiceModel
) {
  const aff = exactMatchKey(affiliation);
  if (!aff || aff === CENSUS_INDEPENDENT_AFFILIATION) return null;

  let canonical = affiliationToCanonical.get(aff) || null;
  let matchSource = canonical ? "brand_alias" : null;

  if (!canonical && brandSetupServiceModel.has(aff)) {
    canonical = aff;
    matchSource = "brand_setup_direct";
  }

  if (!canonical) return null;

  const serviceModel = brandSetupServiceModel.get(canonical);
  if (!serviceModel) return null;

  if (!matchSource) {
    matchSource = aff === canonical ? "brand_setup_direct" : "brand_alias";
  }

  return { canonicalBrand: canonical, serviceModel, matchSource };
}

export function serviceModelNeedsUpdate(current, expected) {
  const cur = normalizeServiceModelForCensus(current);
  const exp = normalizeServiceModelForCensus(expected);
  if (!exp) return false;
  if (!cur) return true;
  return cur !== exp;
}
