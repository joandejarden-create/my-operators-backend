/**
 * Geography audit + Dealality taxonomy expansion proposals (deterministic, non-STR).
 */

import { proposeCensusSubmarketCorridor } from "../../hotel-census/census-dealality-submarket.js";
import { COUNTRY_TO_SUB_CONTINENT } from "../../hotel-census/geography-enrichment-contract.js";
import { resolveStateRegion } from "../census-autopilot-v3/state-region-pipeline.js";
import {
  resolveDealalityMarketStrict,
  CITY_TO_DEALALITY_MARKET,
} from "../census-autopilot-v3/geography/dealality-market-registry.js";

/**
 * @deprecated Do not use country→same-string as Market. Kept only for incident forensics.
 * Live resolution uses resolveDealalityMarketStrict (no country fallback).
 */
export const COUNTRY_DEFAULT_MARKET_LEGACY_BUG = Object.freeze({
  Mexico: "Mexico",
  "Dominican Republic": "Dominican Republic",
  Jamaica: "Jamaica",
  "Puerto Rico": "Puerto Rico",
  Bahamas: "Bahamas",
  Barbados: "Barbados",
  Aruba: "Aruba",
  "Costa Rica": "Costa Rica",
  Panama: "Panama",
  Guatemala: "Guatemala",
  Belize: "Belize",
  Honduras: "Honduras",
  Nicaragua: "Nicaragua",
  "El Salvador": "El Salvador",
  Brazil: "Brazil",
  Argentina: "Argentina",
  Colombia: "Colombia",
  Chile: "Chile",
  Peru: "Peru",
  Ecuador: "Ecuador",
  Paraguay: "Paraguay",
  Uruguay: "Uruguay",
  Bolivia: "Bolivia",
  "Trinidad and Tobago": "Trinidad and Tobago",
  "Saint Lucia": "Saint Lucia",
  "Cayman Islands": "Cayman Islands",
  Curaçao: "Curaçao",
  Curacao: "Curaçao",
});

/** @deprecated use CITY_TO_DEALALITY_MARKET */
export const CITY_MARKET_OVERRIDES = CITY_TO_DEALALITY_MARKET;

/**
 * Resolve Dealality Market. Never returns Country as a silent fallback for
 * multi-market countries. Single-market island taxonomies may equal Country.
 * @param {string} country
 * @param {string} [city]
 * @param {{ state?: string, latitude?: number, longitude?: number }} [opts]
 */
export function resolveDealalityMarket(country, city, opts = {}) {
  const r = resolveDealalityMarketStrict(country, city, opts);
  return r.ok ? r.market : null;
}

export function resolveDealalityGeography(hotel) {
  const country = hotel.country || null;
  const city = hotel.city || hotel.best_snapshot?.city || null;
  const market = resolveDealalityMarket(country, city, {
    state: hotel.state_region || hotel.state || hotel.best_snapshot?.state_region || null,
    latitude: hotel.latitude ?? hotel.lat ?? null,
    longitude: hotel.longitude ?? hotel.lng ?? null,
  });
  const sub = proposeCensusSubmarketCorridor({
    country,
    city,
    name: hotel.name,
    market,
  });
  const continent = country ? "Americas" : null;
  const subContinent = country ? COUNTRY_TO_SUB_CONTINENT[country] || null : null;
  const stateRes = resolveStateRegion({
    country,
    city,
    address: hotel.address || hotel.best_snapshot?.address || null,
    official_state: hotel.state_region || hotel.state || hotel.best_snapshot?.state_region || null,
  });

  return {
    country,
    city,
    market,
    submarket: sub.submarket || null,
    submarket_confidence: sub.confidence,
    submarket_reason: sub.reason,
    continent,
    sub_continent: subContinent,
    state_region: stateRes.ok ? stateRes.normalized_state_region : null,
    state_region_raw: stateRes.raw_state_region,
    state_region_confidence: stateRes.confidence,
    state_region_derivation: stateRes.derivation,
    state_region_source: stateRes.source,
    mapped: Boolean(market),
    unmapped: !market || sub.confidence === "No Match",
  };
}

/**
 * Audit geography failures across wave results / cohort.
 */
export function auditMarketSubmarket(cohort, results = []) {
  const rows = [];
  let marketOk = 0;
  let subOk = 0;
  const unmapped = [];
  const expansions = new Map();

  for (const h of cohort) {
    const r = results.find((x) => x.candidate_id === h.candidate_id);
    const city = h.city || r?.best_snapshot?.address?.split(",")?.[1]?.trim() || null;
    const geo = resolveDealalityGeography({ ...h, city });
    if (geo.market) marketOk += 1;
    if (geo.submarket && geo.submarket_confidence !== "No Match") subOk += 1;
    else {
      unmapped.push({
        candidate_id: h.candidate_id,
        name: h.name,
        country: h.country,
        city,
        reason: geo.submarket_reason || "no_market_or_submarket",
      });
      const key = `${h.country}|${city || "unknown"}`;
      if (!expansions.has(key)) {
        expansions.set(key, {
          country: h.country,
          city: city || null,
          proposed_market: geo.market || null,
          proposed_market_status: geo.market ? "RESOLVED" : "UNRESOLVED",
          proposed_submarket: null,
          rule: "no_country_as_market_fallback",
          count: 0,
        });
      }
      expansions.get(key).count += 1;
    }
    rows.push({ candidate_id: h.candidate_id, ...geo });
  }

  return {
    version: "market-submarket-audit-v2.2",
    n: cohort.length,
    market_coverage_pct: Math.round((100 * marketOk) / Math.max(1, cohort.length)),
    submarket_coverage_pct: Math.round((100 * subOk) / Math.max(1, cohort.length)),
    unmapped_count: unmapped.length,
    unmapped_queue: unmapped.slice(0, 200),
    sample_rows: rows.slice(0, 50),
  };
}

export function proposeGeographyTaxonomyExpansion(audit) {
  const proposals = [];
  const seen = new Set();
  for (const u of audit.unmapped_queue || []) {
    const key = `${u.country}|${u.city || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    proposals.push({
      country: u.country,
      state_province: null,
      market: resolveDealalityMarket(u.country, u.city) || null,
      market_status: resolveDealalityMarket(u.country, u.city) ? "RESOLVED" : "UNRESOLVED",
      submarket: null,
      city: u.city,
      rule_type: "dealality_deterministic_no_country_fallback",
      non_proprietary: true,
      not_str: true,
      maintainable: true,
    });
  }
  return {
    version: "geography-taxonomy-expansion-v2.2",
    rules: [
      "Country → State/Province (when known) → Market (Dealality commercial) → Submarket (corridor) → City",
      "Never import STR Market/Submarket as product geography",
      "Unmapped cities enter exception queue with proposed corridor label for steward review",
    ],
    proposals: proposals.slice(0, 150),
    proposal_count: proposals.length,
  };
}
