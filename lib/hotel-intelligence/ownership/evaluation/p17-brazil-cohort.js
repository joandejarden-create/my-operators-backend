/**
 * P1.7 Brazil 50-hotel deterministic coverage cohort.
 */

import { createHash } from "node:crypto";
import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";
import { WEBHOUND_APRIME_REGRESSION } from "./p16-brazil-cohort.js";

export const OWNERSHIP_P17_BRAZIL_VERSION = "ownership-p17-brazil-eval-v1";
export const OWNERSHIP_P17_BRAZIL_SEED = "ownership-intelligence-p17-brazil-50-v1";

function hash(id) {
  return createHash("sha256")
    .update(`${OWNERSHIP_P17_BRAZIL_SEED}:${id}`)
    .digest("hex");
}

function isBrazil(country) {
  return /brazil|brasil/i.test(String(country || ""));
}

/**
 * @param {object[]} censusRecords
 * @param {object[]} priorHotels
 * @param {{ size?: number }} [opts]
 */
export function buildP17BrazilEvalCohort(censusRecords, priorHotels = [], opts = {}) {
  const size = Number(opts.size || 50);
  const pool = [];
  const seen = new Set();

  for (const h of priorHotels || []) {
    if (!isBrazil(h.country)) continue;
    const key = h.census_record_id || h.hotel_id;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    pool.push({
      source: "prior_eval",
      hotel_id: h.hotel_id,
      census_record_id: h.census_record_id,
      name: h.name,
      country: h.country,
      city: h.city,
      brand: h.brand,
      website_present: h.website_present,
      _sort: hash(key),
    });
  }

  for (const rec of censusRecords || []) {
    const f = rec.fields || {};
    const country = f[MAP_CENSUS_FIELDS.country] || f.Country;
    if (!isBrazil(country)) continue;
    const name =
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      f["Property Name"];
    if (!name) continue;
    if (seen.has(rec.id)) continue;
    seen.add(rec.id);
    pool.push({
      source: "census_fill",
      hotel_id: null,
      census_record_id: rec.id,
      name,
      country,
      city: f[MAP_CENSUS_FIELDS.city] || f.City || null,
      brand: f[MAP_CENSUS_FIELDS.brandName] || null,
      website: f[MAP_CENSUS_FIELDS.website] || null,
      website_present: Boolean(f[MAP_CENSUS_FIELDS.website]),
      _sort: hash(rec.id),
    });
  }

  pool.sort((a, b) => a._sort.localeCompare(b._sort));

  const selected = pool.slice(0, size).map((h, idx) => {
    const n = String(h.name || "").toLowerCase();
    let segment = "independent";
    if (/ibis|novotel|mercure|accor|marriott|hilton|hyatt|ihg|wyndham|choice|sleep inn|comfort|radisson/i.test(n)) {
      segment = /sleep|ibis|hampton|fairfield|courtyard|aloft/i.test(n)
        ? "select_service"
        : "branded_full_service";
    }
    if (/portal|flat|condo|residence|apart/i.test(n)) segment = "condo_apart";
    return { ...h, eval_segment: segment, eval_index: idx + 1 };
  });

  return {
    version: OWNERSHIP_P17_BRAZIL_VERSION,
    seed: OWNERSHIP_P17_BRAZIL_SEED,
    total: selected.length,
    hotels: selected,
    webhound_aprime_regression: WEBHOUND_APRIME_REGRESSION,
  };
}
