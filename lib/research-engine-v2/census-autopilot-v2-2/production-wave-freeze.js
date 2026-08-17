/**
 * Real production wave freeze — 500 from unresolved Census queue with P0–P5 priority.
 * Excludes prior V2.1 wave IDs when provided. Not a benchmark cohort.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { inferBrandFamily, normName } from "../census-autopilot-v2/identity-dedupe.js";
import { CANDIDATE_ORIGINS } from "../census-autopilot-v2/constants.js";
import { PRIORITY_WAVE } from "./constants.js";
import { classifyFamilyExtended } from "./brand-family-universe.js";

const REGIONS = {
  Mexico: "Mexico",
  Caribbean: [
    "Dominican Republic",
    "Jamaica",
    "Puerto Rico",
    "Bahamas",
    "Barbados",
    "Aruba",
    "Curaçao",
    "Trinidad and Tobago",
    "Saint Lucia",
    "Cayman Islands",
  ],
  "Central America": [
    "Costa Rica",
    "Panama",
    "Guatemala",
    "Belize",
    "Honduras",
    "Nicaragua",
    "El Salvador",
  ],
  "South America": [
    "Brazil",
    "Argentina",
    "Colombia",
    "Chile",
    "Peru",
    "Ecuador",
    "Paraguay",
    "Uruguay",
    "Bolivia",
  ],
};

function regionOf(country) {
  if (country === "Mexico") return "Mexico";
  for (const [reg, list] of Object.entries(REGIONS)) {
    if (reg === "Mexico") continue;
    if (list.includes(country)) return reg;
  }
  return "Other";
}

function isWeakIdentity(name) {
  const n = String(name || "");
  if (!n || n.length < 5) return true;
  if (/^choice property mx\d+/i.test(n)) return true;
  if (/^hotel \d+$/i.test(n)) return true;
  return false;
}

function assignPriority(c) {
  if (c.strata?.sibling_risk && c.strata?.weak_identity) return "P0";
  if (c.strata?.existing_vic) return "P1";
  if (c.strata?.branded && c.strata?.cvent_origin && !c.strata?.weak_identity) return "P2";
  if (c.strata?.cvent_origin && c.strata?.branded) return "P3";
  if (c.strata?.cvent_origin && !c.strata?.weak_identity) return "P3";
  if (c.strata?.independent && !c.strata?.weak_identity) return "P4";
  return "P5";
}

/**
 * @param {object[]} candidates
 * @param {object[]} vicRecords
 * @param {number} targetN
 * @param {{ excludeIds?: Set<string> }} [opts]
 */
export function freezeProductionWave(candidates, vicRecords, targetN = 500, opts = {}) {
  const excludeIds = opts.excludeIds || new Set();
  const vicById = new Map(vicRecords.map((v) => [v.independent_record_id, v]));

  const pool = candidates
    .map((c) => {
      const name = c.origin_name;
      const { family } = classifyFamilyExtended(name, c.family);
      const branded = family !== "Independent" && family !== "Unknown";
      return {
        ...c,
        brand_family_inferred: family,
        strata: {
          branded,
          independent: !branded,
          cvent_origin:
            c.candidate_origin === CANDIDATE_ORIGINS.CVENT_CHALLENGE ||
            c.candidate_origin === "CVENT_CHALLENGE",
          existing_vic:
            c.candidate_origin === CANDIDATE_ORIGINS.VERIFIED_INDEPENDENT ||
            Boolean(c.family && c.website),
          soft_collection: /curio|tapestry|autograph|voco|canopy|joia|ascend/i.test(String(name || "")),
          weak_identity: isWeakIdentity(name),
          sibling_risk: /airport|aeropuerto|zona hotelera|downtown|centro|polanco/i.test(
            `${name || ""} ${c.origin_city || ""}`
          ),
          region: regionOf(c.origin_country),
          country: c.origin_country,
        },
      };
    })
    .filter((c) => c.origin_name && !excludeIds.has(c.candidate_id));

  const vicSeeds = vicRecords
    .filter((v) => !excludeIds.has(`cand_vic_${v.independent_record_id}`))
    .slice(0, 120)
    .map((v) => ({
      candidate_id: `cand_vic_${v.independent_record_id}`,
      candidate_origin: CANDIDATE_ORIGINS.VERIFIED_INDEPENDENT,
      origin_name: v.name,
      origin_country: v.country || "Mexico",
      origin_city: v.city,
      origin_url: v.website,
      origin_source_record_id: v.independent_record_id,
      brand: v.brand,
      family: v.family,
      website: v.website,
      property_ids: v.property_ids,
      brand_family_inferred: v.family,
      strata: {
        branded: true,
        independent: false,
        cvent_origin: false,
        existing_vic: true,
        soft_collection: false,
        weak_identity: false,
        sibling_risk: /airport|downtown|centro/i.test(`${v.name || ""} ${v.city || ""}`),
        region: "Mexico",
        country: "Mexico",
      },
    }));

  const combined = [...vicSeeds, ...pool].map((c) => ({
    ...c,
    wave_priority: assignPriority(c),
  }));

  // Quotas by priority — real queue mix, not easy-only
  const quotas = [
    { p: "P0", n: 40 },
    { p: "P1", n: 80 },
    { p: "P2", n: 120 },
    { p: "P3", n: 120 },
    { p: "P4", n: 90 },
    { p: "P5", n: 50 },
  ];

  const picked = [];
  const used = new Set();

  for (const q of quotas) {
    let added = 0;
    for (const c of combined) {
      if (picked.length >= targetN) break;
      if (added >= q.n) break;
      if (c.wave_priority !== q.p) continue;
      if (used.has(c.candidate_id)) continue;
      used.add(c.candidate_id);
      picked.push(c);
      added += 1;
    }
  }

  for (const c of combined) {
    if (picked.length >= targetN) break;
    if (used.has(c.candidate_id)) continue;
    used.add(c.candidate_id);
    picked.push(c);
  }

  const cohort = picked.slice(0, targetN).map((c, i) => ({
    wave_index: i,
    candidate_id: c.candidate_id,
    property_identity_id:
      c.origin_source_record_id ||
      `pid_${createHash("sha1").update(`${c.origin_country}|${normName(c.origin_name)}`).digest("hex").slice(0, 16)}`,
    name: c.origin_name,
    country: c.origin_country,
    city: c.origin_city || (vicById.get(c.origin_source_record_id)?.city ?? null),
    brand: c.brand || null,
    family: c.family || c.brand_family_inferred || inferBrandFamily(c.origin_name),
    website: c.website || (String(c.origin_url || "").includes("cvent.com") ? null : c.origin_url),
    property_ids: c.property_ids || [],
    candidate_origin: c.candidate_origin,
    wave_priority: c.wave_priority,
    priority_label: PRIORITY_WAVE[c.wave_priority] || c.wave_priority,
    strata: c.strata,
  }));

  const priority_distribution = {};
  for (const c of cohort) {
    priority_distribution[c.wave_priority] = (priority_distribution[c.wave_priority] || 0) + 1;
  }

  const countries = [...new Set(cohort.map((c) => c.country).filter(Boolean))];
  const families = {};
  for (const c of cohort) {
    families[c.family || "Unknown"] = (families[c.family || "Unknown"] || 0) + 1;
  }

  return {
    version: "production-wave-freeze-v2.2",
    wave_type: "REAL_PRODUCTION_RESEARCH_WAVE",
    not_a_benchmark: true,
    target: targetN,
    actual: cohort.length,
    excluded_prior_wave_ids: excludeIds.size,
    priority_distribution,
    priority_labels: PRIORITY_WAVE,
    composition: {
      countries,
      country_count: countries.length,
      families,
      branded: cohort.filter((c) => c.strata?.branded).length,
      independent: cohort.filter((c) => c.strata?.independent).length,
      cvent_origin: cohort.filter((c) => c.strata?.cvent_origin).length,
      existing_vic: cohort.filter((c) => c.strata?.existing_vic).length,
    },
    cohort,
  };
}

export function loadPriorWaveExcludeIds(repoRoot) {
  const p = path.join(
    repoRoot,
    "data/research-engine-v2/census-autopilot-v2-1-production-readiness/08-wave-cohort-freeze.json"
  );
  if (!fs.existsSync(p)) return new Set();
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return new Set((j.cohort || []).map((c) => c.candidate_id).filter(Boolean));
}
