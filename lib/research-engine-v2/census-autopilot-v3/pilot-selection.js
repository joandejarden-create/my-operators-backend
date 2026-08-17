/**
 * Pilot cohort selection + Census matching for V3 Phase 1.
 */

import { createHash } from "node:crypto";
import {
  COUNTRY_SHORT,
  MATCH_CLASS,
  VERIFIED_STATE,
  CIRCUIT_BREAKERS,
} from "./constants.js";
import {
  indexHotelPropertyCensus,
  matchDiscoveredProperty,
  MATCH_CLASS as SD_MATCH,
} from "../census-autopilot-source-discovery.js";
import { resolveDealalityGeography } from "../census-autopilot-v2-2/geography-expansion.js";
import { normName } from "../census-autopilot-v2/identity-dedupe.js";

function countryShort(country) {
  return COUNTRY_SHORT[country] || String(country || "xx").slice(0, 2).toLowerCase();
}

/**
 * Production Property Identity Key from official family + code.
 */
export function buildProductionIdentityKey(rec) {
  const fam = String(rec.affiliation?.brand_family || "").toLowerCase();
  const code = String(rec.physical?.official_property_id || "").toLowerCase();
  const cc = countryShort(rec.physical?.country);
  if (code && ["ihg", "hilton", "marriott", "choice"].includes(fam)) {
    return `ind_${fam}_${cc}_${code}`;
  }
  // Stable Dealality key — not Cvent
  return `ind_dealality_${cc}_${createHash("sha1")
    .update(`${rec.physical?.country}|${normName(rec.physical?.current_name)}|${code || rec.physical?.official_url || ""}`)
    .digest("hex")
    .slice(0, 12)}`;
}

function mapSdMatch(sd) {
  if (!sd) return MATCH_CLASS.IDENTITY_CONFLICT;
  const c = sd.classification;
  if (c === SD_MATCH.NEW_CANDIDATE) return MATCH_CLASS.NEW_INSERT;
  if (c === SD_MATCH.EXISTING_EXACT) return MATCH_CLASS.EXACT_EXISTING_MATCH;
  if (c === SD_MATCH.EXISTING_PROBABLE) return MATCH_CLASS.HIGH_EXISTING_MATCH;
  if (c === SD_MATCH.DUPLICATE_RISK) return MATCH_CLASS.POSSIBLE_DUPLICATE;
  if (c === SD_MATCH.IDENTITY_CONFLICT) return MATCH_CLASS.IDENTITY_CONFLICT;
  if (c === SD_MATCH.STEWARD) return MATCH_CLASS.HIGH_EXISTING_MATCH;
  if (c === SD_MATCH.SOURCE_INSUFFICIENT) return MATCH_CLASS.IDENTITY_CONFLICT;
  return MATCH_CLASS.HIGH_EXISTING_MATCH;
}

/**
 * Select pilot candidates from V2.3 independent freeze.
 * Prefer official directory; exclude SerpApi-only and Cvent evidence.
 */
export function selectPilotCandidates(independentRecords, censusRecords, opts = {}) {
  const target = opts.target || CIRCUIT_BREAKERS.pilot_b_target_total;
  const index = indexHotelPropertyCensus(censusRecords);

  const pool = [];
  const exclusions = [];

  for (const r of independentRecords) {
    const src = r.discovery_evidence?.source_type;
    const cvent = r.cvent_used_as_production_evidence === true;
    if (cvent) {
      exclusions.push({
        property_identity_id: r.property_identity_id,
        reason: "cvent_production_evidence_flag",
      });
      continue;
    }
    if (src === "serpapi_google_hotels_discovery") {
      exclusions.push({
        property_identity_id: r.property_identity_id,
        reason: "serpapi_only_discovery_excluded_from_first_pilot_writes",
      });
      continue;
    }
    if (src !== "official_brand_directory" && src !== "verified_independent_census_seed") {
      exclusions.push({
        property_identity_id: r.property_identity_id,
        reason: `source_not_pilot_eligible:${src}`,
      });
      continue;
    }
    if (!r.physical?.current_name || !r.physical?.country) {
      exclusions.push({
        property_identity_id: r.property_identity_id,
        reason: "missing_name_or_country",
      });
      continue;
    }
    if (!r.physical?.official_url && !r.physical?.official_property_id) {
      exclusions.push({
        property_identity_id: r.property_identity_id,
        reason: "missing_official_url_and_property_id",
      });
      continue;
    }

    const identity_key = buildProductionIdentityKey(r);
    // Current Brand must be property-level — never fall back to brand_family/parent.
    const rawBrand = r.affiliation?.current_brand || null;
    const discovered = {
      property_name: r.physical.current_name,
      brand: rawBrand,
      parent_company: r.affiliation?.brand_family,
      source_family: r.affiliation?.brand_family,
      country: r.physical.country,
      city: r.physical.city,
      official_property_id: r.physical.official_property_id,
      official_property_url: r.physical.official_url,
      identity_key,
      identity_confidence: "High",
      source_confidence: "High",
    };

    const sd = matchDiscoveredProperty(discovered, index);
    const match_class = mapSdMatch(sd);
    const geo = resolveDealalityGeography({
      name: r.physical.current_name,
      country: r.physical.country,
      city: r.physical.city,
    });

    const eligible_auto =
      match_class === MATCH_CLASS.NEW_INSERT ||
      match_class === MATCH_CLASS.EXACT_EXISTING_MATCH;

    pool.push({
      research_property_identity_id: r.property_identity_id,
      property_identity_key: identity_key,
      name: r.physical.current_name,
      brand: discovered.brand,
      family: r.affiliation?.brand_family,
      country: r.physical.country,
      city: r.physical.city,
      official_url: r.physical.official_url,
      official_property_id: r.physical.official_property_id,
      latitude: r.physical.lat ?? null,
      longitude: r.physical.lng ?? null,
      address: r.physical.address || null,
      phone: r.physical.phone || null,
      source_type: src,
      discovery_lane: r.strata?.discovery_lane,
      cvent_used_as_production_evidence: false,
      legacy_used_as_production_evidence: false,
      match_class,
      match_detail: sd,
      census_record_id: sd.census_record_id || null,
      geography: geo,
      eligible_auto_write: eligible_auto,
      verified_state: VERIFIED_STATE.ROOMS_PENDING, // Rooms unknown by default this pilot
      rooms_value: null,
      rooms_inferred: false,
      priority_completeness_proxy: estimateCompleteness(r, geo),
    });
  }

  // Stratify: multiple families + countries; prefer NEW_INSERT then EXACT
  const selected = [];
  const used = new Set();
  const excludeKeys = new Set(opts.excludeKeys || []);
  const quotas = opts.quotas || [
    { pred: (p) => p.match_class === MATCH_CLASS.NEW_INSERT && p.family === "IHG", n: 35 },
    { pred: (p) => p.match_class === MATCH_CLASS.NEW_INSERT && p.family === "Hilton", n: 25 },
    { pred: (p) => p.match_class === MATCH_CLASS.NEW_INSERT && p.family === "Marriott", n: 30 },
    { pred: (p) => p.match_class === MATCH_CLASS.NEW_INSERT && p.family === "Choice", n: 20 },
    { pred: (p) => p.match_class === MATCH_CLASS.NEW_INSERT, n: 20 },
    { pred: (p) => p.match_class === MATCH_CLASS.EXACT_EXISTING_MATCH, n: 20 },
  ];

  // Drop excluded cohort keys (e.g. original V3 150)
  const eligiblePool = pool.filter((p) => !excludeKeys.has(p.property_identity_key));

  for (const q of quotas) {
    let a = 0;
    for (const p of eligiblePool) {
      if (selected.length >= target) break;
      if (a >= q.n) break;
      if (!p.eligible_auto_write) continue;
      if (!q.pred(p)) continue;
      if (used.has(p.property_identity_key)) continue;
      // Prefer ≥90% proxy when possible but don't require if pool thin
      used.add(p.property_identity_key);
      selected.push(p);
      a += 1;
    }
  }

  for (const p of eligiblePool) {
    if (selected.length >= target) break;
    if (!p.eligible_auto_write) continue;
    if (used.has(p.property_identity_key)) continue;
    used.add(p.property_identity_key);
    selected.push(p);
  }

  const byMatch = {};
  for (const p of pool) {
    byMatch[p.match_class] = (byMatch[p.match_class] || 0) + 1;
  }

  return {
    evaluated: pool.length,
    excluded: exclusions.length,
    exclusions: exclusions.slice(0, 200),
    match_distribution: byMatch,
    selected,
    pilot_a: selected.slice(0, CIRCUIT_BREAKERS.pilot_a_size),
    pilot_b_remainder: selected.slice(CIRCUIT_BREAKERS.pilot_a_size),
    target,
    actual: selected.length,
  };
}

function estimateCompleteness(r, geo) {
  let n = 0;
  const checks = [
    r.physical?.current_name,
    r.affiliation?.brand_family,
    r.physical?.country,
    r.physical?.city,
    r.physical?.official_url,
    r.physical?.official_property_id,
    geo?.market,
    geo?.continent,
    geo?.sub_continent,
  ];
  for (const c of checks) if (c) n += 1;
  // Rooms missing → still can be ~90% of this thin set
  return Math.round((100 * n) / (checks.length + 1)); // +1 for rooms unknown
}
