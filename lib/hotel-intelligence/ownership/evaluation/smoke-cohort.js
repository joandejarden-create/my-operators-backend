/**
 * Deterministic 20-hotel accuracy smoke cohort from the v1 100-hotel set.
 */

import { createHash } from "node:crypto";

export const OWNERSHIP_SMOKE_VERSION = "ownership-smoke-cohort-v1";
export const OWNERSHIP_SMOKE_SEED = "ownership-intelligence-smoke-20-v1";

const REGIONS = [
  "Mexico",
  "Caribbean",
  "Central America",
  "Andean / Northern SA",
  "Brazil",
];

function hash(id) {
  return createHash("sha256")
    .update(`${OWNERSHIP_SMOKE_SEED}:${id}`)
    .digest("hex");
}

/**
 * Build stratified 20-hotel subset (4 per region) from prior 100-hotel results.
 * Prefers mix of: prior false OWNED_BY, UNKNOWN, website/no website, branded/independent.
 *
 * @param {object[]} hotelResults — rows from 03-hotel-results.json
 * @param {{ perRegion?: number }} [opts]
 */
export function buildOwnershipSmokeCohort(hotelResults, opts = {}) {
  const perRegion = Math.max(1, Number(opts.perRegion) || 4);
  const byRegion = new Map(REGIONS.map((r) => [r, []]));

  for (const h of hotelResults || []) {
    const region = h.eval_region;
    if (!byRegion.has(region)) continue;
    const owned = (h.relationships || []).filter(
      (r) => r.relationship_type === "OWNED_BY"
    );
    const implausibleOwned = owned.filter((r) =>
      looksImplausibleOwnerName(
        r.object_display_name || r.object_legal_name || ""
      )
    );
    const propcoStatus = String(
      h.status_summary?.propco || h.verification?.propco || "unknown"
    ).toLowerCase();
    byRegion.get(region).push({
      ...h,
      _owned_count: owned.length,
      _implausible: implausibleOwned.length,
      _unknown: propcoStatus === "unknown" || owned.length === 0,
      _has_web: Boolean(h.website_present),
      _branded: Boolean(h.brand && String(h.brand).trim()),
      _sort: hash(h.hotel_id),
    });
  }

  const selected = [];
  for (const region of REGIONS) {
    const pool = byRegion.get(region) || [];
    // Score for diversity: prioritize prior FPs, then unknowns, then mix web/brand
    const ranked = [...pool].sort((a, b) => {
      const score = (x) =>
        (x._implausible > 0 ? 1000 : 0) +
        (x._unknown ? 100 : 0) +
        (x._owned_count > 0 && x._implausible === 0 ? 50 : 0) +
        (x._has_web ? 10 : 5) +
        (x._branded ? 3 : 7);
      const d = score(b) - score(a);
      if (d !== 0) return d;
      return a._sort.localeCompare(b._sort);
    });

    const picks = [];
    const usedBuckets = new Set();
    for (const h of ranked) {
      if (picks.length >= perRegion) break;
      const bucket = [
        h._implausible > 0 ? "fp" : h._unknown ? "unk" : "claim",
        h._has_web ? "web" : "noweb",
        h._branded ? "brand" : "ind",
      ].join("|");
      // Prefer unused diversity buckets first
      if (usedBuckets.has(bucket) && picks.length + (ranked.length - ranked.indexOf(h)) > perRegion) {
        continue;
      }
      usedBuckets.add(bucket);
      picks.push(h);
    }
    // Fill if diversity filter was too strict
    for (const h of ranked) {
      if (picks.length >= perRegion) break;
      if (picks.some((p) => p.hotel_id === h.hotel_id)) continue;
      picks.push(h);
    }
    selected.push(...picks.slice(0, perRegion));
  }

  return {
    version: OWNERSHIP_SMOKE_VERSION,
    seed: OWNERSHIP_SMOKE_SEED,
    per_region: perRegion,
    total: selected.length,
    by_region: Object.fromEntries(
      REGIONS.map((r) => [
        r,
        selected.filter((h) => h.eval_region === r).length,
      ])
    ),
    hotels: selected.map((h) => ({
      hotel_id: h.hotel_id,
      census_record_id: h.census_record_id,
      eval_region: h.eval_region,
      name: h.name,
      country: h.country,
      city: h.city,
      brand: h.brand,
      website_present: h.website_present,
      prior_owned_by_count: h._owned_count,
      prior_implausible_owned_by: h._implausible,
      prior_propco_unknown: h._unknown,
      quality_bucket: h.quality_bucket,
    })),
  };
}

function looksImplausibleOwnerName(name) {
  const s = String(name || "").trim();
  if (!s) return false;
  if (s.length < 4) return true;
  if (/^(esta|este|un alojamiento|at |en |del |l |s de |no ha indicado|property|hotel|resort)/i.test(s)) {
    return true;
  }
  if (/responded to this|respondi[oó]|detector de humo|parking|habitaciones/i.test(s)) {
    return true;
  }
  return false;
}
