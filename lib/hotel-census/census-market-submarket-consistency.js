/**
 * Hotel Census Market / Submarket consistency audit and repair proposals.
 */

import { CENSUS_FIELDS } from "./fields.js";
import {
  normStrLabel,
  resolveCensusCountryKey,
  resolveStrSubmarketToCorridor,
} from "./census-str-submarket-corridors.js";
import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";
import {
  getSubmarketOptionsForCountry,
  isValidSubmarketOption,
  normalizeSubmarketLabel,
} from "../radar-submarket.js";
import { normalizeKey, normalizeStrId } from "../str-census-import/normalize.mjs";

const STR_ID_FIELD = "STR Number";

/**
 * @param {object} ms marketSubmarkets map
 * @param {string} market
 */
export function findMarketSubmarketKey(ms, market) {
  const mk = normStrLabel(market);
  if (!mk) return null;
  for (const key of Object.keys(ms)) {
    const kk = normStrLabel(key);
    if (kk === mk || mk.includes(kk) || kk.includes(mk)) return key;
  }
  return null;
}

/**
 * @param {string} countryKey
 * @param {string} market
 */
export function getAllowedSubmarketsForMarket(countryKey, market) {
  const config = COUNTRY_CONFIGS[countryKey];
  if (!config) return null;
  const ms = config.marketSubmarkets || {};
  if (!Object.keys(ms).length) return null;
  const key = findMarketSubmarketKey(ms, market);
  return key ? ms[key] : null;
}

/**
 * @param {Array<{ id: string, fields: object }>} records
 */
export function buildMarketSubmarketConsistencyIndexes(records) {
  /** @type {Map<string, Map<string, number>>} */
  const submarketDominantMarket = new Map();
  /** @type {Map<string, { market: string, submarket: string, count: number }>} */
  const cityGeoMode = new Map();
  /** @type {Map<string, Map<string, number>>} */
  const citySubmarketMode = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const country = String(f[CENSUS_FIELDS.country] ?? "").trim();
    const market = String(f[CENSUS_FIELDS.market] ?? "").trim();
    const submarket = String(f[CENSUS_FIELDS.submarket] ?? "").trim();
    const city = String(f[CENSUS_FIELDS.city] ?? "").trim();
    if (!country) continue;

    if (market && submarket) {
      const smKey = `${country}|${normStrLabel(submarket)}`;
      if (!submarketDominantMarket.has(smKey)) submarketDominantMarket.set(smKey, new Map());
      const mmap = submarketDominantMarket.get(smKey);
      mmap.set(market, (mmap.get(market) || 0) + 1);
    }

    if (city && country) {
      const ccKey = `${normalizeKey(city)}||${normalizeKey(country)}`;
      if (market && submarket) {
        const bucket = `${market}||${submarket}`;
        if (!cityGeoMode.has(ccKey)) cityGeoMode.set(ccKey, new Map());
        const cm = cityGeoMode.get(ccKey);
        cm.set(bucket, (cm.get(bucket) || 0) + 1);
      }
      if (submarket) {
        if (!citySubmarketMode.has(ccKey)) citySubmarketMode.set(ccKey, new Map());
        const sm = citySubmarketMode.get(ccKey);
        sm.set(submarket, (sm.get(submarket) || 0) + 1);
      }
    }
  }

  /** @type {Map<string, { market: string, share: number, total: number }>} */
  const submarketToDominantMarket = new Map();
  for (const [smKey, mmap] of submarketDominantMarket) {
    const total = [...mmap.values()].reduce((a, b) => a + b, 0);
    const sorted = [...mmap.entries()].sort((a, b) => b[1] - a[1]);
    if (!sorted.length) continue;
    submarketToDominantMarket.set(smKey, {
      market: sorted[0][0],
      share: sorted[0][1] / total,
      total,
    });
  }

  /** @type {Map<string, { market: string, submarket: string, count: number }>} */
  const cityGeoModeBest = new Map();
  for (const [ccKey, cm] of cityGeoMode) {
    const sorted = [...cm.entries()].sort((a, b) => b[1] - a[1]);
    if (!sorted.length) continue;
    const [pair, count] = sorted[0];
    const [market, submarket] = pair.split("||");
    cityGeoModeBest.set(ccKey, { market, submarket, count });
  }

  /** @type {Map<string, { submarket: string, count: number, total: number }>} */
  const citySubmarketModeBest = new Map();
  for (const [ccKey, sm] of citySubmarketMode) {
    const total = [...sm.values()].reduce((a, b) => a + b, 0);
    const sorted = [...sm.entries()].sort((a, b) => b[1] - a[1]);
    if (!sorted.length) continue;
    citySubmarketModeBest.set(ccKey, {
      submarket: sorted[0][0],
      count: sorted[0][1],
      total,
    });
  }

  return {
    submarketToDominantMarket,
    cityGeoModeBest,
    citySubmarketModeBest,
  };
}

function labelsEquivalent(a, b) {
  return normStrLabel(a) === normStrLabel(b);
}

function cityAlignsWithSubmarket(city, submarket) {
  const c = normStrLabel(city);
  const s = normStrLabel(submarket);
  if (!c || !s) return false;
  if (c === s || c.includes(s) || s.includes(c)) return true;
  const cityCore = c.replace(/^port of /, "");
  const subCore = s.replace(/^port of /, "");
  return cityCore === subCore || cityCore.includes(subCore) || subCore.includes(cityCore);
}

function isBroadCountryMarket(market, country) {
  const m = normStrLabel(market);
  const c = normStrLabel(country);
  if (!m || !c) return false;
  return m === c || /provincial|regional/.test(market);
}

/**
 * @param {object} row
 * @param {object} context
 * @param {ReturnType<typeof buildMarketSubmarketConsistencyIndexes>} context.indexes
 * @param {Map<string, object>} [context.excelByStrId]
 * @param {object} [options]
 */
export function proposeMarketSubmarketConsistencyFix(row, context, options = {}) {
  const minDominantShare = options.minDominantShare ?? 0.85;
  const minDominantTotal = options.minDominantTotal ?? 5;
  const minCityModeShare = options.minCityModeShare ?? 0.7;
  const minCityModeCount = options.minCityModeCount ?? 3;
  const trustStr = options.trustStr !== false;

  const country = String(row[CENSUS_FIELDS.country] ?? row.country ?? "").trim();
  const city = String(row[CENSUS_FIELDS.city] ?? row.city ?? "").trim();
  const name = String(row[CENSUS_FIELDS.name] ?? row.name ?? "").trim();
  const market = String(row[CENSUS_FIELDS.market] ?? row.market ?? "").trim();
  const submarket = String(row[CENSUS_FIELDS.submarket] ?? row.submarket ?? "").trim();
  const strId = normalizeStrId(row[STR_ID_FIELD]);

  const issues = [];
  const proposed = {};
  let reason = null;
  let confidence = "Low";
  let source = null;

  if (!country) {
    return { issues: ["missing_country"], proposed: {}, reason: null, confidence: "No Match", skipped: true };
  }

  const countryKey = resolveCensusCountryKey(country);
  const indexes = context.indexes;
  const ccKey = `${normalizeKey(city)}||${normalizeKey(country)}`;

  let strExcelRow = null;
  let strEndorsedMarket = "";
  let strEndorsedSubmarket = "";
  if (trustStr && strId && context.excelByStrId?.has(strId)) {
    strExcelRow = context.excelByStrId.get(strId);
    strEndorsedMarket = String(strExcelRow?.strMarket ?? "").trim();
    strEndorsedSubmarket = String(strExcelRow?.strSubmarket ?? "").trim();
  }
  const strAnchored = Boolean(strExcelRow && (strEndorsedMarket || strEndorsedSubmarket));

  if (market && submarket) {
    const allowed = getAllowedSubmarketsForMarket(countryKey, market);
    if (allowed && !allowed.includes(submarket) && submarket !== "Other") {
      issues.push("submarket_not_allowed_for_market");
    }
    if (/provincial|regional/i.test(market) && !/other|regional|provincial/i.test(submarket)) {
      const domKey = `${country}|${normStrLabel(submarket)}`;
      const dom = indexes.submarketToDominantMarket.get(domKey);
      const domIsMetro =
        dom &&
        !/provincial|regional/i.test(dom.market) &&
        dom.total >= minDominantTotal &&
        dom.share >= minDominantShare;
      const strEndorsesPair =
        strAnchored &&
        strEndorsedMarket &&
        labelsEquivalent(strEndorsedMarket, market) &&
        (!strEndorsedSubmarket || labelsEquivalent(strEndorsedSubmarket, submarket));
      if (domIsMetro && !labelsEquivalent(dom.market, market) && !strEndorsesPair) {
        issues.push("provincial_market_with_specific_submarket");
      }
    }
  }

  const cityCorridor = resolveStrSubmarketToCorridor({ country, city, market: "", strSubmarket: city });
  if (cityCorridor?.submarket && submarket && !labelsEquivalent(cityCorridor.submarket, submarket)) {
    issues.push("city_corridor_submarket_mismatch");
  }

  const domKey = `${country}|${normStrLabel(submarket)}`;
  const dominant = indexes.submarketToDominantMarket.get(domKey);
  if (dominant && market && !labelsEquivalent(dominant.market, market)) {
    if (dominant.total >= minDominantTotal && dominant.share >= minDominantShare) {
      issues.push("market_not_dominant_for_submarket");
    }
  }

  // 1) STR Excel (highest confidence)
  if (strAnchored) {
    const cityMode = indexes.cityGeoModeBest.get(ccKey);
    const cityModeEndorsesCurrentMarket =
      cityMode &&
      cityMode.count >= minCityModeCount &&
      market &&
      labelsEquivalent(cityMode.market, market);

    const cityModeIsMetro =
      cityMode &&
      !/provincial|regional/i.test(cityMode.market) &&
      !isBroadCountryMarket(cityMode.market, country);
    const blockStrProvincialOverMetro =
      strEndorsedMarket &&
      /provincial|regional/i.test(strEndorsedMarket) &&
      cityModeEndorsesCurrentMarket &&
      cityModeIsMetro &&
      labelsEquivalent(cityMode.market, market);
    const metroGeoLocked =
      cityModeEndorsesCurrentMarket &&
      cityModeIsMetro &&
      market &&
      labelsEquivalent(cityMode.market, market) &&
      submarket &&
      (cityAlignsWithSubmarket(city, submarket) ||
        labelsEquivalent(cityMode.submarket, submarket)) &&
      (!strEndorsedMarket || labelsEquivalent(strEndorsedMarket, cityMode.market));

    if (blockStrProvincialOverMetro) {
      // Keep metro market when STR would incorrectly widen to provincial.
    } else if (
      strEndorsedMarket &&
      !labelsEquivalent(strEndorsedMarket, market) &&
      !metroGeoLocked
    ) {
      proposed[CENSUS_FIELDS.market] = strEndorsedMarket;
      reason = "str_excel_market";
      confidence = "High";
      source = "str_excel";
    }
    if (strEndorsedSubmarket && !labelsEquivalent(strEndorsedSubmarket, submarket)) {
      const citySm = indexes.citySubmarketModeBest.get(ccKey);
      const cityModeEndorsesCurrentSubmarket =
        citySm &&
        citySm.total >= minCityModeCount &&
        submarket &&
        labelsEquivalent(citySm.submarket, submarket) &&
        citySm.count / citySm.total >= minCityModeShare;
      const cityCorridorForCity = resolveStrSubmarketToCorridor({
        country,
        city,
        market: "",
        strSubmarket: city,
      });
      const strDisagreesWithCityCorridor =
        cityCorridorForCity?.submarket &&
        !labelsEquivalent(cityCorridorForCity.submarket, strEndorsedSubmarket);
      const cityEndorsesCurrentSubmarket =
        cityAlignsWithSubmarket(city, submarket) ||
        (cityCorridorForCity?.submarket &&
          labelsEquivalent(cityCorridorForCity.submarket, submarket));

      if (
        !(
          (cityModeEndorsesCurrentSubmarket || cityEndorsesCurrentSubmarket) &&
          (strDisagreesWithCityCorridor || !labelsEquivalent(strEndorsedSubmarket, submarket))
        )
      ) {
        proposed[CENSUS_FIELDS.submarket] = strEndorsedSubmarket;
        reason = reason ? "str_excel_market_submarket" : "str_excel_submarket";
        confidence = "High";
        source = source || "str_excel";
      }
    }
  }

  const strMarketLocked =
    strAnchored &&
    strEndorsedMarket &&
    (labelsEquivalent(strEndorsedMarket, market) ||
      labelsEquivalent(strEndorsedMarket, proposed[CENSUS_FIELDS.market]));
  const strSubmarketLocked =
    strAnchored &&
    strEndorsedSubmarket &&
    (labelsEquivalent(strEndorsedSubmarket, submarket) ||
      labelsEquivalent(strEndorsedSubmarket, proposed[CENSUS_FIELDS.submarket]));

  let marketNow = proposed[CENSUS_FIELDS.market] || market;
  let submarketNow = proposed[CENSUS_FIELDS.submarket] || submarket;

  // 2) City → corridor when current submarket disagrees with city (before dominant-market)
  if (city && cityCorridor?.submarket && submarketNow) {
    if (!labelsEquivalent(cityCorridor.submarket, submarketNow)) {
      const normalizedCityCorridor = normalizeSubmarketLabel(cityCorridor.submarket, countryKey);
      const nextSubmarket = normalizedCityCorridor || cityCorridor.submarket;
      if (isValidSubmarketOption(nextSubmarket, countryKey)) {
        const citySm = indexes.citySubmarketModeBest.get(
          `${normalizeKey(city)}||${normalizeKey(country)}`
        );
        const cityAgrees =
          citySm &&
          citySm.total >= minCityModeCount &&
          labelsEquivalent(citySm.submarket, nextSubmarket) &&
          citySm.count / citySm.total >= minCityModeShare;
        if (cityAgrees || issues.includes("city_corridor_submarket_mismatch")) {
          proposed[CENSUS_FIELDS.submarket] = nextSubmarket;
          submarketNow = nextSubmarket;
          reason = reason || "city_corridor_submarket";
          confidence = confidence === "High" ? "High" : "Medium";
          source = source || "city_corridor";
        }
      }
    }
  }

  marketNow = proposed[CENSUS_FIELDS.market] || marketNow;
  submarketNow = proposed[CENSUS_FIELDS.submarket] || submarketNow;

  const corridorMatchesSubmarket =
    cityCorridor?.submarket &&
    submarketNow &&
    labelsEquivalent(cityCorridor.submarket, submarketNow);
  const keepProvincialWithCorridor =
    /provincial|regional/i.test(marketNow) && corridorMatchesSubmarket;

  // 3) Dominant market for submarket (fixes provincial / regional mis-tags)
  const domKeyNow = `${country}|${normStrLabel(submarketNow)}`;
  const dominantNow = indexes.submarketToDominantMarket.get(domKeyNow);
  if (
    !strMarketLocked &&
    !keepProvincialWithCorridor &&
    !proposed[CENSUS_FIELDS.market] &&
    submarketNow &&
    dominantNow
  ) {
    if (
      marketNow &&
      !labelsEquivalent(dominantNow.market, marketNow) &&
      dominantNow.total >= minDominantTotal &&
      dominantNow.share >= minDominantShare
    ) {
      proposed[CENSUS_FIELDS.market] = dominantNow.market;
      reason = reason || "submarket_dominant_market";
      confidence = confidence === "High" ? "High" : "Medium";
      source = source || "census_submarket_dominant_market";
    }
  }

  // Provincial / regional market with a specific corridor submarket
  if (
    !strMarketLocked &&
    !strSubmarketLocked &&
    !keepProvincialWithCorridor &&
    !proposed[CENSUS_FIELDS.market] &&
    /provincial|regional/i.test(marketNow) &&
    submarketNow &&
    !/other|regional|provincial/i.test(submarketNow) &&
    dominantNow &&
    !labelsEquivalent(dominantNow.market, marketNow)
  ) {
    proposed[CENSUS_FIELDS.market] = dominantNow.market;
    reason = reason || "provincial_market_realign";
    confidence = confidence === "High" ? "High" : "Medium";
    source = source || "census_submarket_dominant_market";
  }

  const marketAfterDom = proposed[CENSUS_FIELDS.market] || marketNow;
  const submarketAfterDom = proposed[CENSUS_FIELDS.submarket] || submarketNow;

  // 4) Submarket must belong to market (nested registry countries)
  const allowed = getAllowedSubmarketsForMarket(countryKey, marketAfterDom);
  if (
    allowed &&
    submarketAfterDom &&
    !allowed.includes(submarketAfterDom) &&
    submarketAfterDom !== "Other"
  ) {
    const cityHit = resolveStrSubmarketToCorridor({
      country,
      city,
      market: marketAfterDom,
      strSubmarket: "",
    });
    if (cityHit?.submarket && allowed.includes(cityHit.submarket)) {
      if (!labelsEquivalent(cityHit.submarket, submarketAfterDom)) {
        proposed[CENSUS_FIELDS.submarket] = cityHit.submarket;
        reason = reason || "submarket_fit_under_market_from_city";
        confidence = confidence === "High" ? "High" : "Medium";
        source = source || "city_corridor_under_market";
      }
    } else if (dominant && !labelsEquivalent(dominant.market, marketAfterDom)) {
      proposed[CENSUS_FIELDS.market] = dominant.market;
      reason = reason || "market_match_submarket_registry";
      confidence = confidence === "High" ? "High" : "Medium";
      source = source || "census_submarket_dominant_market";
    }
  }

  const marketFinal = proposed[CENSUS_FIELDS.market] || marketAfterDom;
  let submarketFinal = proposed[CENSUS_FIELDS.submarket] || submarketAfterDom;

  // 5) City mode agreement
  const cityMode = indexes.cityGeoModeBest.get(ccKey);
  if (
    city &&
    cityMode &&
    cityMode.count >= minCityModeCount &&
    cityMode.count / (indexes.citySubmarketModeBest.get(ccKey)?.total || cityMode.count) >=
      minCityModeShare
  ) {
    if (!labelsEquivalent(cityMode.market, marketFinal)) {
      if (!proposed[CENSUS_FIELDS.market] && !strMarketLocked) {
        const currentIsBroad =
          !marketFinal ||
          isBroadCountryMarket(marketFinal, country) ||
          issues.includes("market_not_dominant_for_submarket");
        if (currentIsBroad) {
          proposed[CENSUS_FIELDS.market] = cityMode.market;
          reason = reason || "city_country_mode_market";
          confidence = confidence === "High" ? "High" : "Medium";
          source = source || "census_city_mode";
        }
      }
    }
    if (!labelsEquivalent(cityMode.submarket, submarketFinal)) {
      if (!proposed[CENSUS_FIELDS.submarket]) {
        proposed[CENSUS_FIELDS.submarket] = cityMode.submarket;
        reason = reason || "city_country_mode_submarket";
        confidence = confidence === "High" ? "High" : "Medium";
        source = source || "census_city_mode";
      }
    }
  }

  submarketFinal = proposed[CENSUS_FIELDS.submarket] || submarketFinal;

  // 6) Normalize submarket to canonical corridor label
  const normalized = normalizeSubmarketLabel(submarketFinal, countryKey);
  if (
    normalized &&
    normalized !== "Other" &&
    submarketFinal &&
    !labelsEquivalent(normalized, submarketFinal) &&
    isValidSubmarketOption(normalized, countryKey)
  ) {
    proposed[CENSUS_FIELDS.submarket] = normalized;
    reason = reason || "normalize_submarket_label";
    confidence = confidence === "High" ? "High" : "Medium";
    source = source || "dealality_submarket_normalize";
  }

  const finalFields = {};
  for (const [key, value] of Object.entries(proposed)) {
    const next = String(value ?? "").trim();
    const current = String(row[key] ?? "").trim();
    if (next && !labelsEquivalent(next, current)) {
      finalFields[key] = next;
    }
  }

  if (!Object.keys(finalFields).length) {
    return {
      issues,
      proposed: {},
      reason: null,
      confidence: "No Match",
      source: null,
      skipped: true,
      name,
      country,
      city,
      market,
      submarket,
    };
  }

  const subOut = finalFields[CENSUS_FIELDS.submarket];
  if (subOut && !isValidSubmarketOption(subOut, countryKey)) {
    const options = getSubmarketOptionsForCountry(countryKey);
    if (!options.includes(subOut)) {
      return {
        issues: [...issues, "invalid_submarket_option"],
        proposed: {},
        reason: "validation_failed",
        confidence: "No Match",
        source,
        skipped: true,
        name,
        country,
        city,
        market,
        submarket,
      };
    }
  }

  return {
    issues,
    proposed: finalFields,
    reason,
    confidence,
    source,
    skipped: false,
    name,
    country,
    city,
    market,
    submarket,
  };
}

/**
 * @param {Array<{ id: string, fields: object }>} records
 * @param {object} context
 * @param {object} [options]
 */
export function planMarketSubmarketConsistencyFixes(records, context, options = {}) {
  const minConfidence = options.minConfidence || "Medium";
  const rank = { High: 3, Medium: 2, Low: 1, "No Match": 0 };
  const minRank = rank[minConfidence] ?? 2;

  const plans = [];
  const issueCounts = {};
  const sourceCounts = {};

  for (const rec of records) {
    const plan = proposeMarketSubmarketConsistencyFix(rec.fields, context, options);
    for (const issue of plan.issues || []) {
      issueCounts[issue] = (issueCounts[issue] || 0) + 1;
    }
    if (plan.skipped) continue;
    if ((rank[plan.confidence] ?? 0) < minRank) continue;

    plans.push({
      recordId: rec.id,
      name: plan.name,
      country: plan.country,
      city: plan.city,
      currentMarket: plan.market,
      currentSubmarket: plan.submarket,
      proposed: plan.proposed,
      reason: plan.reason,
      confidence: plan.confidence,
      source: plan.source,
      issues: plan.issues,
    });
    sourceCounts[plan.source] = (sourceCounts[plan.source] || 0) + 1;
  }

  return { plans, issueCounts, sourceCounts };
}
