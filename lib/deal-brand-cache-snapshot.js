/**
 * Single read model for deal brand match scores: Deal Brand Cache only.
 * Writers: refreshDealBrandCacheForRecordId (api/my-deals.js) — the only compute path.
 */

import { lookupMatchScoreNewByBrand, normalizeBrandLookupKey } from "./match-score-brand-lookup.js";

/** @param {number|null|undefined} n */
export function roundMatchScoreNew(n) {
  if (n == null || n === "" || Number.isNaN(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

/**
 * @typedef {Object} DealBrandCacheRow
 * @property {string} [preferredBrandsChosen]
 * @property {number} [preferredScore]
 * @property {Record<string, number|null|undefined>} [matchScoresNewByBrand]
 * @property {Record<string, object>} [breakdownNewDetailsByBrand]
 * @property {Array<{ brand: string, score?: number|null, breakdownNewDetails?: object }>} [topAlternatives]
 */

/**
 * Unified score index: preferred scores map + top alternatives (alternatives fill gaps only).
 * @param {DealBrandCacheRow|null|undefined} cacheRow
 */
export function buildScoresIndexFromCacheRow(cacheRow) {
  /** @type {Map<string, { score: number|null, breakdown: object, displayKey: string }>} */
  const index = new Map();

  const add = (brandName, score, breakdown = {}) => {
    const display = String(brandName || "").trim();
    if (!display) return;
    const key = normalizeBrandLookupKey(display);
    if (!key || index.has(key)) return;
    const rounded = score != null && score !== "" ? roundMatchScoreNew(score) : null;
    index.set(key, {
      score: rounded,
      breakdown: breakdown && typeof breakdown === "object" ? breakdown : {},
      displayKey: display,
    });
  };

  const alts = cacheRow?.topAlternatives;
  if (Array.isArray(alts)) {
    for (const a of alts) {
      const brand = typeof a === "object" && a && a.brand != null ? String(a.brand).trim() : "";
      if (!brand) continue;
      add(brand, a.score, a.breakdownNewDetails);
    }
  }

  const byBrand = cacheRow?.matchScoresNewByBrand;
  if (byBrand && typeof byBrand === "object") {
    for (const [brandName, score] of Object.entries(byBrand)) {
      let breakdown = {};
      const bd = cacheRow?.breakdownNewDetailsByBrand;
      if (bd && typeof bd === "object") {
        breakdown = bd[brandName] || {};
        if (!Object.keys(breakdown).length) {
          const want = normalizeBrandLookupKey(brandName);
          for (const k of Object.keys(bd)) {
            if (normalizeBrandLookupKey(k) === want) {
              breakdown = bd[k] || {};
              break;
            }
          }
        }
      }
      add(brandName, score, breakdown);
    }
  }

  return index;
}

/**
 * Flat score map for list/API clients — same keys as buildScoresIndexFromCacheRow (preferred map + top alternatives).
 * @param {DealBrandCacheRow|null|undefined} cacheRow
 * @returns {Record<string, number|null|undefined>}
 */
export function matchScoresMapFromCacheRow(cacheRow) {
  const index = buildScoresIndexFromCacheRow(cacheRow);
  /** @type {Record<string, number|null|undefined>} */
  const out = {};
  for (const entry of index.values()) {
    if (!entry.displayKey) continue;
    out[entry.displayKey] = entry.score;
  }
  return out;
}

/**
 * @param {ReturnType<typeof buildScoresIndexFromCacheRow>} index
 * @param {string|null|undefined} brandName
 */
export function getScoreFromCacheIndex(index, brandName) {
  if (!index || !brandName) return null;
  const key = normalizeBrandLookupKey(brandName);
  if (!key) return null;
  const hit = index.get(key);
  return hit ? hit.score : null;
}

/**
 * @param {ReturnType<typeof buildScoresIndexFromCacheRow>} index
 * @param {string|null|undefined} brandName
 */
export function getBreakdownFromCacheIndex(index, brandName) {
  if (!index || !brandName) return {};
  const key = normalizeBrandLookupKey(brandName);
  if (!key) return {};
  const hit = index.get(key);
  return hit ? hit.breakdown : {};
}

/**
 * True when cache is missing or any required brand has no score in the index.
 * @param {DealBrandCacheRow|null|undefined} cacheRow
 * @param {string[]} requiredBrandNames
 */
export function cacheNeedsRefresh(cacheRow, requiredBrandNames = []) {
  if (!cacheRow) return true;

  const required = [...new Set(requiredBrandNames.map((b) => String(b || "").trim()).filter(Boolean))];
  const index = buildScoresIndexFromCacheRow(cacheRow);

  if (required.length) {
    if (!index.size) return true;
    if (required.some((b) => getScoreFromCacheIndex(index, b) == null)) return true;
  }

  const alts = cacheRow?.topAlternatives;
  if (!Array.isArray(alts) || alts.length === 0) return true;

  return false;
}

/**
 * @param {DealBrandCacheRow|null|undefined} cacheRow
 * @param {string|null|undefined} preferredBrand
 */
export function preferredScoreFromCache(cacheRow, preferredBrand) {
  if (!cacheRow) return null;
  if (preferredBrand) {
    const fromMap = lookupMatchScoreNewByBrand(cacheRow.matchScoresNewByBrand, preferredBrand);
    if (fromMap != null) return roundMatchScoreNew(fromMap);
    const index = buildScoresIndexFromCacheRow(cacheRow);
    const fromIndex = getScoreFromCacheIndex(index, preferredBrand);
    if (fromIndex != null) return fromIndex;
  }
  if (cacheRow.preferredScore != null && !Number.isNaN(Number(cacheRow.preferredScore))) {
    return roundMatchScoreNew(cacheRow.preferredScore);
  }
  return null;
}
