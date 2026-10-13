/**
 * Deterministic 100-hotel ownership evaluation cohort.
 * Custom evaluation regions (not raw Census Sub-Continent).
 */

import { createHash } from "node:crypto";
import { COUNTRY_TO_SUB_CONTINENT } from "../../../hotel-census/geography-enrichment-contract.js";
import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";

export const OWNERSHIP_COHORT_VERSION = "ownership-eval-cohort-v1";
export const OWNERSHIP_COHORT_SEED = "ownership-intelligence-100-v1";

export const OWNERSHIP_EVAL_REGIONS = Object.freeze([
  "Mexico",
  "Caribbean",
  "Central America",
  "Andean / Northern SA",
  "Brazil",
]);

const ANDEAN_COUNTRIES = new Set([
  "Colombia",
  "Peru",
  "Ecuador",
  "Venezuela",
  "Venezuela (Bolivarian Republic of)",
  "Bolivia",
  "Chile",
]);

/**
 * @param {string} country
 * @returns {string | null}
 */
export function ownershipEvalRegion(country) {
  const c = String(country || "").trim();
  if (!c) return null;
  if (/^mexico$/i.test(c)) return "Mexico";
  if (/^brazil$|^brasil$/i.test(c)) return "Brazil";
  if (ANDEAN_COUNTRIES.has(c)) return "Andean / Northern SA";
  const sub = COUNTRY_TO_SUB_CONTINENT[c] || "";
  if (sub === "Caribbean") return "Caribbean";
  if (sub === "Central America") return "Central America";
  // Fallback fuzzy
  const key = c.toLowerCase();
  for (const [name, s] of Object.entries(COUNTRY_TO_SUB_CONTINENT)) {
    if (name.toLowerCase() === key) {
      if (s === "Caribbean") return "Caribbean";
      if (s === "Central America") return "Central America";
    }
  }
  return null;
}

function stableHash(id) {
  return createHash("sha256")
    .update(`${OWNERSHIP_COHORT_SEED}:${id}`)
    .digest("hex");
}

function blank(v) {
  return v == null || !String(v).trim();
}

function qualityBucket(f) {
  const branded = !blank(f[MAP_CENSUS_FIELDS.brandName]);
  const website = !blank(f[MAP_CENSUS_FIELDS.website]);
  const operator = !blank(f["Operator / Management Company"]);
  const owner = !blank(f["Owner Name"]);
  const parts = [
    branded ? "branded" : "independent",
    website ? "has_web" : "no_web",
    operator ? "has_op" : "no_op",
    owner ? "has_owner_field" : "no_owner_field",
  ];
  return parts.join("|");
}

/**
 * @param {object[]} censusRecords — { id, fields }
 * @param {{ targetPerRegion?: number, seed?: string }} [opts]
 */
export function buildOwnershipEvalCohort(censusRecords, opts = {}) {
  const targetPerRegion = Math.max(
    1,
    Number(opts.targetPerRegion ?? 20)
  );
  const byRegion = new Map(OWNERSHIP_EVAL_REGIONS.map((r) => [r, []]));

  for (const r of censusRecords || []) {
    const country = r.fields?.[MAP_CENSUS_FIELDS.country] || r.fields?.Country;
    const region = ownershipEvalRegion(country);
    if (!region || !byRegion.has(region)) continue;
    byRegion.get(region).push(r);
  }

  for (const [, arr] of byRegion) {
    arr.sort((a, b) => stableHash(a.id).localeCompare(stableHash(b.id)));
  }

  const selected = [];
  const selectedIds = new Set();
  const regionStats = {};

  for (const region of OWNERSHIP_EVAL_REGIONS) {
    const pool = byRegion.get(region) || [];
    const buckets = new Map();
    for (const r of pool) {
      const qb = qualityBucket(r.fields || {});
      if (!buckets.has(qb)) buckets.set(qb, []);
      buckets.get(qb).push(r);
    }
    const bucketKeys = [...buckets.keys()].sort();
    let taken = 0;
    let guard = 0;
    while (taken < targetPerRegion && guard < pool.length * 3) {
      for (const bk of bucketKeys) {
        const arr = buckets.get(bk);
        if (!arr?.length) continue;
        const row = arr.shift();
        if (selectedIds.has(row.id)) continue;
        selected.push(toCohortRow(row, region));
        selectedIds.add(row.id);
        taken += 1;
        if (taken >= targetPerRegion) break;
      }
      guard += 1;
    }
    // Fill remainder from hashed pool
    if (taken < targetPerRegion) {
      for (const row of pool) {
        if (selectedIds.has(row.id)) continue;
        selected.push(toCohortRow(row, region));
        selectedIds.add(row.id);
        taken += 1;
        if (taken >= targetPerRegion) break;
      }
    }
    regionStats[region] = {
      pool_size: pool.length,
      selected: taken,
      target: targetPerRegion,
    };
  }

  return {
    version: OWNERSHIP_COHORT_VERSION,
    seed: OWNERSHIP_COHORT_SEED,
    target_per_region: targetPerRegion,
    actual_total: selected.length,
    region_stats: regionStats,
    hotels: selected,
  };
}

function toCohortRow(r, region) {
  const f = r.fields || {};
  return {
    census_record_id: r.id,
    eval_region: region,
    country: f[MAP_CENSUS_FIELDS.country] || null,
    name:
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      null,
    city: f[MAP_CENSUS_FIELDS.city] || null,
    brand: f[MAP_CENSUS_FIELDS.brandName] || null,
    website: f[MAP_CENSUS_FIELDS.website] || null,
    chain_scale: f[MAP_CENSUS_FIELDS.chainScale] || null,
    property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    quality_bucket: qualityBucket(f),
    stable_hash: stableHash(r.id),
  };
}
