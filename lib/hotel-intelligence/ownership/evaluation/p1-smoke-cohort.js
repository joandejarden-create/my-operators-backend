/**
 * P1 authoritative-source smoke cohort (20 hotels).
 */

import { createHash } from "node:crypto";
import { ownershipEvalRegion } from "./cohort.js";
import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";

export const OWNERSHIP_P1_SMOKE_VERSION = "ownership-p1-authoritative-smoke-v1";
export const OWNERSHIP_P1_SMOKE_SEED = "ownership-intelligence-p1-auth-20-v1";

const TARGETS = Object.freeze([
  { key: "Mexico", n: 5 },
  { key: "Brazil", n: 5 },
  { key: "Colombia", n: 2 },
  { key: "Guatemala", n: 2 },
  { key: "Peru", n: 2 },
  { key: "Ecuador", n: 2 },
  { key: "other_cala", n: 2 },
]);

function hash(id) {
  return createHash("sha256")
    .update(`${OWNERSHIP_P1_SMOKE_SEED}:${id}`)
    .digest("hex");
}

function countryBucket(country) {
  const c = String(country || "").trim();
  if (/^mexico$/i.test(c)) return "Mexico";
  if (/^brazil$|^brasil$/i.test(c)) return "Brazil";
  if (/^colombia$/i.test(c)) return "Colombia";
  if (/^guatemala$/i.test(c)) return "Guatemala";
  if (/^peru$|^perú$/i.test(c)) return "Peru";
  if (/^ecuador$/i.test(c)) return "Ecuador";
  const region = ownershipEvalRegion(c);
  if (region) return "other_cala";
  return null;
}

/**
 * Prefer hotels from prior 100-hotel results; fill from census records if needed.
 *
 * @param {object[]} priorHotels
 * @param {object[]} censusRecords
 */
export function buildP1AuthoritativeSmokeCohort(priorHotels, censusRecords) {
  const byBucket = new Map(TARGETS.map((t) => [t.key, []]));

  for (const h of priorHotels || []) {
    const bucket = countryBucket(h.country);
    if (!bucket || !byBucket.has(bucket)) continue;
    byBucket.get(bucket).push({
      source: "prior_100",
      hotel_id: h.hotel_id,
      census_record_id: h.census_record_id,
      name: h.name,
      country: h.country,
      city: h.city,
      brand: h.brand,
      website_present: h.website_present,
      eval_region: h.eval_region,
      _sort: hash(h.census_record_id || h.hotel_id),
    });
  }

  // Fill gaps from census
  for (const rec of censusRecords || []) {
    const f = rec.fields || {};
    const country = f[MAP_CENSUS_FIELDS.country] || f.Country;
    const bucket = countryBucket(country);
    if (!bucket || !byBucket.has(bucket)) continue;
    const name =
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      f["Property Name"];
    if (!name) continue;
    const id = rec.id;
    if (byBucket.get(bucket).some((x) => x.census_record_id === id)) continue;
    byBucket.get(bucket).push({
      source: "census_fill",
      hotel_id: null,
      census_record_id: id,
      name,
      country,
      city: f[MAP_CENSUS_FIELDS.city] || f.City || null,
      brand: f[MAP_CENSUS_FIELDS.brandName] || null,
      website_present: Boolean(f[MAP_CENSUS_FIELDS.website]),
      eval_region: ownershipEvalRegion(country),
      _sort: hash(id),
    });
  }

  const selected = [];
  for (const t of TARGETS) {
    const pool = [...(byBucket.get(t.key) || [])].sort((a, b) =>
      a._sort.localeCompare(b._sort)
    );
    // Prefer prior_100 first
    const prior = pool.filter((p) => p.source === "prior_100");
    const fill = pool.filter((p) => p.source !== "prior_100");
    const picks = [...prior, ...fill].slice(0, t.n);
    selected.push(...picks.map((p) => ({ ...p, smoke_bucket: t.key })));
  }

  return {
    version: OWNERSHIP_P1_SMOKE_VERSION,
    seed: OWNERSHIP_P1_SMOKE_SEED,
    targets: TARGETS,
    total: selected.length,
    by_bucket: Object.fromEntries(
      TARGETS.map((t) => [
        t.key,
        selected.filter((h) => h.smoke_bucket === t.key).length,
      ])
    ),
    hotels: selected,
  };
}
