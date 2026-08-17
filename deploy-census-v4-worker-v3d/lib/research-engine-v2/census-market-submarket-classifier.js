/**
 * Market geography classifier — Continent / Sub-Continent / Market / Submarket
 * High-confidence proposals for Hotel Property Census Autopilot.
 */

import {
  CENSUS_GEO_FIELDS,
  resolveContinentSubContinentFromCountry,
  resolveMarketFromCity,
  resolveSubmarketHighOnly,
  isMateriallyDifferentGeo,
} from "./census-region-market-map.js";
import {
  CITY_CLASS,
  classifyAndNormalizeCityState,
  isDescriptorCity,
} from "./census-city-state-normalizer.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";

export const MARKET_GEOGRAPHY_QUEUE_ID = "market_geography_completion";

export const MARKET_GEOGRAPHY_STATUS = Object.freeze({
  APPLIED_CLEAN: "production_census_market_geography_completion_applied_clean",
  PARTIAL_STEWARD:
    "production_census_market_geography_completion_partial_steward_remaining",
  READY_NEEDS_MISSION:
    "production_census_market_geography_completion_ready_needs_mission",
  BLOCKED: "production_census_market_geography_completion_blocked",
});

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

/**
 * Classify + propose High geography patches for one census record.
 * @param {object} record
 * @param {{
 *   fieldExists?: { continent?: boolean, subContinent?: boolean, market?: boolean, submarket?: boolean },
 *   fillSubmarket?: boolean,
 * }} [opts]
 */
export function classifyMarketGeography(record, opts = {}) {
  const fields = record?.fields || {};
  const fieldExists = {
    continent: opts.fieldExists?.continent !== false,
    subContinent: opts.fieldExists?.subContinent !== false,
    market: opts.fieldExists?.market !== false,
    submarket: opts.fieldExists?.submarket !== false,
  };
  const fillSubmarket = opts.fillSubmarket !== false;

  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const cityRaw = String(fields[MAP_FIRST_PASS.city] || "").trim();
  const address = String(fields[MAP_FIRST_PASS.address] || "").trim();
  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();

  const existing = {
    continent: String(fields[CENSUS_GEO_FIELDS.continent] || "").trim(),
    subContinent: String(fields[CENSUS_GEO_FIELDS.subContinent] || "").trim(),
    market: String(fields[CENSUS_GEO_FIELDS.market] || "").trim(),
    submarket: String(fields[CENSUS_GEO_FIELDS.submarket] || "").trim(),
  };

  const patch = {};
  const steward = [];
  const notes = [];
  const sources = [];

  // --- Continent / Sub-Continent from Country map ---
  const regionMap = resolveContinentSubContinentFromCountry(country);
  if (!country) {
    notes.push("country_missing");
    steward.push({
      reason: "country_missing_for_continent",
      field: CENSUS_GEO_FIELDS.continent,
    });
  } else if (!regionMap) {
    notes.push("country_unmapped");
    steward.push({
      reason: "country_not_in_continent_map",
      field: CENSUS_GEO_FIELDS.continent,
      country,
    });
  } else {
    if (fieldExists.continent) {
      if (isBlank(existing.continent)) {
        patch[CENSUS_GEO_FIELDS.continent] = regionMap.continent;
        sources.push({
          field: CENSUS_GEO_FIELDS.continent,
          method: "country_continent_map",
          confidence: "High",
        });
      } else if (isMateriallyDifferentGeo(existing.continent, regionMap.continent)) {
        steward.push({
          reason: "continent_conflict_with_country_map",
          field: CENSUS_GEO_FIELDS.continent,
          existing: existing.continent,
          proposed: regionMap.continent,
        });
      }
    }
    if (fieldExists.subContinent) {
      if (isBlank(existing.subContinent)) {
        patch[CENSUS_GEO_FIELDS.subContinent] = regionMap.subContinent;
        sources.push({
          field: CENSUS_GEO_FIELDS.subContinent,
          method: "country_subcontinent_map",
          confidence: "High",
        });
      } else if (
        isMateriallyDifferentGeo(existing.subContinent, regionMap.subContinent)
      ) {
        steward.push({
          reason: "subcontinent_conflict_with_country_map",
          field: CENSUS_GEO_FIELDS.subContinent,
          existing: existing.subContinent,
          proposed: regionMap.subContinent,
        });
      }
    }
  }

  // --- Market from clean City ---
  const cityNorm = classifyAndNormalizeCityState(fields);
  const cityClean =
    cityNorm.city_clean &&
    cityNorm.class !== CITY_CLASS.UNKNOWN &&
    cityNorm.class !== CITY_CLASS.DESCRIPTOR &&
    cityNorm.class !== CITY_CLASS.BLANK &&
    cityNorm.class !== CITY_CLASS.MIXED_UNRESOLVED &&
    !isDescriptorCity(cityRaw);

  let resolvedMarket = existing.market || null;

  if (fieldExists.market) {
    if (!cityClean) {
      notes.push("market_deferred_dirty_city");
    } else {
      const cityForMarket = cityNorm.normalized_city || cityRaw;
      const mkt = resolveMarketFromCity({ city: cityForMarket, country });
      if (!mkt.ok) {
        notes.push(mkt.reason || "market_source_needed");
      } else if (isBlank(existing.market)) {
        patch[CENSUS_GEO_FIELDS.market] = mkt.market;
        resolvedMarket = mkt.market;
        sources.push({
          field: CENSUS_GEO_FIELDS.market,
          method: mkt.method,
          confidence: "High",
        });
      } else if (isMateriallyDifferentGeo(existing.market, mkt.market)) {
        steward.push({
          reason: "market_conflict_with_city_map",
          field: CENSUS_GEO_FIELDS.market,
          existing: existing.market,
          proposed: mkt.market,
        });
        resolvedMarket = existing.market;
      } else {
        resolvedMarket = existing.market;
      }
    }
  }

  // --- Submarket High only ---
  if (fillSubmarket && fieldExists.submarket) {
    if (!isBlank(existing.submarket)) {
      // Never overwrite populated Submarket without steward
      const probe = resolveSubmarketHighOnly({
        market: resolvedMarket,
        city: cityNorm.normalized_city || cityRaw,
        address,
        propertyName,
      });
      if (
        probe.ok &&
        isMateriallyDifferentGeo(existing.submarket, probe.submarket)
      ) {
        steward.push({
          reason: "submarket_conflict_existing_populated",
          field: CENSUS_GEO_FIELDS.submarket,
          existing: existing.submarket,
          proposed: probe.submarket,
        });
      }
    } else if (resolvedMarket) {
      const sub = resolveSubmarketHighOnly({
        market: resolvedMarket,
        city: cityNorm.normalized_city || cityRaw,
        address,
        propertyName,
      });
      if (sub.ok && sub.confidence === "High") {
        patch[CENSUS_GEO_FIELDS.submarket] = sub.submarket;
        sources.push({
          field: CENSUS_GEO_FIELDS.submarket,
          method: sub.method,
          confidence: "High",
          evidence_token: sub.evidence_token || null,
        });
      } else {
        notes.push(sub.reason || "submarket_optional_blank");
      }
    } else {
      notes.push("submarket_deferred_no_market");
    }
  }

  // Do not attach Enrichment Status / Priority / Last Reviewed Date here.
  // Those frequently conflict with existing Discovery-era values and route the
  // whole proposal to steward even when Continent/Market already match or are writable.

  return {
    record_id: record.id,
    identity_key: fields[MAP_FIRST_PASS.identityKey] || null,
    property_name: propertyName,
    country,
    city: cityRaw,
    city_class: cityNorm.class,
    existing,
    patch,
    sources,
    steward,
    notes,
    high_writable: Object.keys(patch).length > 0 && steward.length === 0,
  };
}

/**
 * Build Autopilot High proposals from census records.
 * @param {object[]} censusRecords
 * @param {object} [opts]
 */
export function buildMarketGeographyProposals(censusRecords = [], opts = {}) {
  const proposals = [];
  const stewardCases = [];
  const blocked = [];
  let scanned = 0;
  const blanks = {
    continent: 0,
    subContinent: 0,
    market: 0,
    submarket: 0,
  };
  const highCounts = {
    continent: 0,
    subContinent: 0,
    market: 0,
    submarket: 0,
  };

  for (const rec of censusRecords) {
    scanned += 1;
    const fields = rec.fields || {};
    if (isBlank(fields[CENSUS_GEO_FIELDS.continent])) blanks.continent += 1;
    if (isBlank(fields[CENSUS_GEO_FIELDS.subContinent])) blanks.subContinent += 1;
    if (isBlank(fields[CENSUS_GEO_FIELDS.market])) blanks.market += 1;
    if (isBlank(fields[CENSUS_GEO_FIELDS.submarket])) blanks.submarket += 1;

    if (fields[MAP_FIRST_PASS.humanReview] === true) {
      blocked.push({
        record_id: rec.id,
        reason: "human_review_required",
      });
      continue;
    }

    const row = classifyMarketGeography(rec, opts);
    if (row.steward.length) {
      stewardCases.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        steward: row.steward,
      });
    }
    if (!Object.keys(row.patch).length) continue;

    // Geo fields only — never Enrichment Status/Priority/Date (idempotent conflicts).
    const geoPatch = {};
    for (const [k, v] of Object.entries(row.patch)) {
      if (
        [
          CENSUS_GEO_FIELDS.continent,
          CENSUS_GEO_FIELDS.subContinent,
          CENSUS_GEO_FIELDS.market,
          CENSUS_GEO_FIELDS.submarket,
        ].includes(k)
      ) {
        geoPatch[k] = v;
      }
    }
    if (
      !geoPatch[CENSUS_GEO_FIELDS.continent] &&
      !geoPatch[CENSUS_GEO_FIELDS.subContinent] &&
      !geoPatch[CENSUS_GEO_FIELDS.market] &&
      !geoPatch[CENSUS_GEO_FIELDS.submarket]
    ) {
      continue;
    }

    if (geoPatch[CENSUS_GEO_FIELDS.continent]) highCounts.continent += 1;
    if (geoPatch[CENSUS_GEO_FIELDS.subContinent]) highCounts.subContinent += 1;
    if (geoPatch[CENSUS_GEO_FIELDS.market]) highCounts.market += 1;
    if (geoPatch[CENSUS_GEO_FIELDS.submarket]) highCounts.submarket += 1;

    proposals.push({
      record_id: row.record_id,
      identity_key: row.identity_key,
      property_name: row.property_name,
      confidence: "High",
      action: "propose_update",
      queue: MARKET_GEOGRAPHY_QUEUE_ID,
      patch: geoPatch,
      sources: row.sources,
      notes: row.notes,
      method: "market_geography_completion",
    });
  }

  return {
    queue_id: MARKET_GEOGRAPHY_QUEUE_ID,
    scanned,
    blanks,
    high_counts: highCounts,
    proposals,
    steward_cases: stewardCases,
    blocked,
  };
}

/**
 * Run queue dry-run style report for orchestrator / controlled mode.
 */
export function runMarketGeographyCompletionQueue(opts = {}) {
  const censusRecords = opts.censusRecords || [];
  const result = buildMarketGeographyProposals(censusRecords, opts);
  const examples = result.proposals.slice(0, 8).map((p) => ({
    record_id: p.record_id,
    property_name: p.property_name,
    patch: p.patch,
    sources: p.sources,
  }));

  let status = MARKET_GEOGRAPHY_STATUS.READY_NEEDS_MISSION;
  if (opts.blocked) status = MARKET_GEOGRAPHY_STATUS.BLOCKED;
  else if (result.steward_cases.length && result.proposals.length === 0) {
    status = MARKET_GEOGRAPHY_STATUS.PARTIAL_STEWARD;
  } else if (result.proposals.length === 0 && result.steward_cases.length === 0) {
    status = MARKET_GEOGRAPHY_STATUS.APPLIED_CLEAN;
  } else if (result.steward_cases.length) {
    status = MARKET_GEOGRAPHY_STATUS.PARTIAL_STEWARD;
  }

  return {
    status,
    queue_id: MARKET_GEOGRAPHY_QUEUE_ID,
    counters: {
      records_scanned: result.scanned,
      continent_blank: result.blanks.continent,
      subcontinent_blank: result.blanks.subContinent,
      market_blank: result.blanks.market,
      submarket_blank: result.blanks.submarket,
      high_continent_proposals: result.high_counts.continent,
      high_subcontinent_proposals: result.high_counts.subContinent,
      high_market_proposals: result.high_counts.market,
      high_submarket_proposals: result.high_counts.submarket,
      steward_cases: result.steward_cases.length,
      blocked: result.blocked.length,
      proposals: result.proposals.length,
    },
    proposals: result.proposals,
    steward_review: result.steward_cases,
    blocked: result.blocked,
    examples_before_after: examples,
  };
}
