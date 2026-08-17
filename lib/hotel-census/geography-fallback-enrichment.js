/**
 * Fill-blank Market / Submarket fallbacks when STR strict match is insufficient.
 *
 * Priority (fill blank only):
 *   Market: STR excel by STR Number → city+country mode → single-country market
 *   Submarket: STR excel by STR Number → city (when Market present)
 */

import { CENSUS_FIELDS } from "./fields.js";
import { normalizeStrId, normalizeKey } from "../str-census-import/normalize.mjs";

export const MAP_MARKET_SUBMARKET_FALLBACK = {
  market: CENSUS_FIELDS.market,
  submarket: CENSUS_FIELDS.submarket,
  strId: "STR Number",
};

export function isBlankValue(value) {
  if (value == null) return true;
  return String(value).trim() === "";
}

/**
 * @param {Array<{ fields: object }>} records
 */
export function buildMarketFallbackIndexes(records) {
  /** @type {Map<string, Map<string, number>>} */
  const countryMarkets = new Map();
  /** @type {Map<string, Map<string, number>>} */
  const cityCountryMarkets = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const market = String(f[CENSUS_FIELDS.market] ?? "").trim();
    if (!market) continue;
    const country = String(f[CENSUS_FIELDS.country] ?? "").trim();
    const city = String(f[CENSUS_FIELDS.city] ?? "").trim();

    if (country) {
      if (!countryMarkets.has(country)) countryMarkets.set(country, new Map());
      const cm = countryMarkets.get(country);
      cm.set(market, (cm.get(market) || 0) + 1);
    }

    const ccKey = `${normalizeKey(city)}||${normalizeKey(country)}`;
    if (city && country) {
      if (!cityCountryMarkets.has(ccKey)) cityCountryMarkets.set(ccKey, new Map());
      const m = cityCountryMarkets.get(ccKey);
      m.set(market, (m.get(market) || 0) + 1);
    }
  }

  /** @type {Map<string, string>} */
  const singleCountryMarket = new Map();
  for (const [country, mmap] of countryMarkets) {
    if (mmap.size === 1) singleCountryMarket.set(country, [...mmap.keys()][0]);
  }

  /** @type {Map<string, string>} */
  const countryModeMarket = new Map();
  for (const [country, mmap] of countryMarkets) {
    const top = topCountedMarket(mmap);
    if (top) countryModeMarket.set(country, top);
  }

  return { countryMarkets, cityCountryMarkets, singleCountryMarket, countryModeMarket };
}

function topCountedMarket(countMap) {
  let best = "";
  let bestN = 0;
  for (const [market, n] of countMap) {
    if (n > bestN) {
      best = market;
      bestN = n;
    }
  }
  return best;
}

/**
 * @param {object} row census fields
 * @param {Map<string, object>} excelByStrId normalized STR ID → excel row
 * @param {{ cityCountryMarkets: Map<string, Map<string, number>>, singleCountryMarket: Map<string, string>, countryModeMarket: Map<string, string> }} indexes
 */
export function proposeMarketSubmarketFallback(row, excelByStrId, indexes) {
  /** @type {Record<string, string>} */
  const fields = {};
  /** @type {Record<string, string>} */
  const sources = {};

  const strId = normalizeStrId(row[MAP_MARKET_SUBMARKET_FALLBACK.strId]);
  const excelRow = strId ? excelByStrId.get(strId) : null;

  if (isBlankValue(row[CENSUS_FIELDS.market])) {
    const fromStr = String(excelRow?.strMarket ?? "").trim();
    if (fromStr) {
      fields[CENSUS_FIELDS.market] = fromStr;
      sources[CENSUS_FIELDS.market] = "str_excel_str_id";
    } else {
      const city = String(row[CENSUS_FIELDS.city] ?? "").trim();
      const country = String(row[CENSUS_FIELDS.country] ?? "").trim();
      const ccKey = `${normalizeKey(city)}||${normalizeKey(country)}`;
      const cityMode = indexes.cityCountryMarkets.has(ccKey)
        ? topCountedMarket(indexes.cityCountryMarkets.get(ccKey))
        : "";
      if (cityMode) {
        fields[CENSUS_FIELDS.market] = cityMode;
        sources[CENSUS_FIELDS.market] = "census_city_country_mode_market";
      } else {
        const countryMode = indexes.countryModeMarket.get(country);
        if (countryMode) {
          fields[CENSUS_FIELDS.market] = countryMode;
          sources[CENSUS_FIELDS.market] = "census_country_mode_market";
        } else {
          const countryOnly = indexes.singleCountryMarket.get(country);
          if (countryOnly) {
            fields[CENSUS_FIELDS.market] = countryOnly;
            sources[CENSUS_FIELDS.market] = "census_single_country_market";
          }
        }
      }
    }
  }

  const marketNow = fields[CENSUS_FIELDS.market] || String(row[CENSUS_FIELDS.market] ?? "").trim();

  if (isBlankValue(row[CENSUS_FIELDS.submarket])) {
    const fromStr = String(excelRow?.strSubmarket ?? "").trim();
    if (fromStr) {
      fields[CENSUS_FIELDS.submarket] = fromStr;
      sources[CENSUS_FIELDS.submarket] = "str_excel_str_id";
    } else if (marketNow) {
      const city = String(row[CENSUS_FIELDS.city] ?? "").trim();
      if (city) {
        fields[CENSUS_FIELDS.submarket] = city;
        sources[CENSUS_FIELDS.submarket] = "city_when_market_present";
      }
    }
  }

  return { fields, sources };
}

/**
 * @param {Array<{ strId?: string }>} excelRows
 */
export function indexStrExcelById(excelRows) {
  /** @type {Map<string, object>} */
  const map = new Map();
  for (const ex of excelRows) {
    const id = normalizeStrId(ex.strId);
    if (id) map.set(id, ex);
  }
  return map;
}
