/**
 * Freeze a representative 250-hotel controlled wave (not cherry-picked).
 */

import { createHash } from "node:crypto";
import { inferBrandFamily, normName } from "../census-autopilot-v2/identity-dedupe.js";
import { CANDIDATE_ORIGINS } from "../census-autopilot-v2/constants.js";

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

function isSoftCollection(name) {
  return /curio|tapestry|autograph|tribute|design hotels|mgallery|unbound|lxr|joia|voco|canopy|tempo |spark by|ascend |cambria|collection by/i.test(
    String(name || "")
  );
}

function isWeakIdentity(name) {
  const n = String(name || "");
  if (!n || n.length < 5) return true;
  if (/^choice property mx\d+/i.test(n)) return true;
  if (/^hotel \d+$/i.test(n)) return true;
  return false;
}

function isSiblingRisk(name, city) {
  // Airport / same-brand patterns
  return /airport|aeropuerto|zona hotelera|downtown|centro|polanco|reforma/i.test(
    `${name || ""} ${city || ""}`
  );
}

/**
 * @param {object[]} candidates raw V2 candidates (+ optional classification overlays)
 * @param {object[]} vicRecords
 * @param {number} targetN
 */
export function freezeWaveCohort(candidates, vicRecords, targetN = 250) {
  const vicById = new Map(vicRecords.map((v) => [v.independent_record_id, v]));

  // Enrich candidates lightly
  const pool = candidates.map((c) => {
    const name = c.origin_name;
    const family = c.family || inferBrandFamily(name);
    const branded = family !== "Independent";
    const region = regionOf(c.origin_country);
    return {
      ...c,
      brand_family_inferred: family,
      strata: {
        branded,
        independent: !branded,
        cvent_origin: c.candidate_origin === CANDIDATE_ORIGINS.CVENT_CHALLENGE || c.candidate_origin === "CVENT_CHALLENGE",
        existing_vic: c.candidate_origin === CANDIDATE_ORIGINS.VERIFIED_INDEPENDENT || Boolean(c.family && c.website),
        soft_collection: isSoftCollection(name),
        weak_identity: isWeakIdentity(name),
        sibling_risk: isSiblingRisk(name, c.origin_city),
        region,
        country: c.origin_country,
        resort_like: /resort|spa|all.?inclusive|beach/i.test(String(name || "")),
        urban_like: /inn|suites|express|garden|downtown|city|centro/i.test(String(name || "")),
      },
    };
  }).filter((c) => c.origin_name && !c.strata.weak_identity); // exclude pure placeholders from primary wave (still sample a few)

  const weakPool = candidates
    .filter((c) => isWeakIdentity(c.origin_name))
    .slice(0, 20)
    .map((c) => ({
      ...c,
      brand_family_inferred: inferBrandFamily(c.origin_name),
      strata: {
        branded: false,
        independent: true,
        cvent_origin: true,
        existing_vic: false,
        soft_collection: false,
        weak_identity: true,
        sibling_risk: false,
        region: regionOf(c.origin_country),
        country: c.origin_country,
        resort_like: false,
        urban_like: false,
      },
    }));

  // VIC existing incomplete seeds
  const vicSeeds = vicRecords.slice(0, 80).map((v) => ({
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
      soft_collection: isSoftCollection(v.name),
      weak_identity: false,
      sibling_risk: isSiblingRisk(v.name, v.city),
      region: "Mexico",
      country: "Mexico",
      resort_like: /resort|spa|all.?inclusive/i.test(v.name || ""),
      urban_like: !/resort|spa|all.?inclusive/i.test(v.name || ""),
    },
  }));

  const picked = [];
  const used = new Set();

  function take(list, n, pred = () => true) {
    for (const c of list) {
      if (picked.length >= targetN) break;
      if (picked.length - (targetN - n) >= n && n < 999) {
        // soft stop for quota buckets — ignore when n is filler
      }
      if (!pred(c)) continue;
      const id = c.candidate_id || c.origin_source_record_id;
      if (used.has(id)) continue;
      used.add(id);
      picked.push(c);
      if ([...picked].filter(pred).length >= n && n < 500) {
        // continue filling other buckets via separate calls
      }
    }
  }

  // Stratified quotas (~250)
  const quotas = [
    { n: 50, pred: (c) => c.strata.existing_vic },
    { n: 40, pred: (c) => c.strata.region === "Mexico" && c.strata.cvent_origin && c.strata.branded },
    { n: 30, pred: (c) => c.strata.region === "Caribbean" && c.strata.cvent_origin },
    { n: 30, pred: (c) => c.strata.region === "Central America" && c.strata.cvent_origin },
    { n: 40, pred: (c) => c.strata.region === "South America" && c.strata.cvent_origin },
    { n: 25, pred: (c) => c.strata.independent && c.strata.cvent_origin && !c.strata.weak_identity },
    { n: 15, pred: (c) => c.strata.soft_collection },
    { n: 10, pred: (c) => c.strata.sibling_risk },
    { n: 10, pred: (c) => c.strata.weak_identity },
  ];

  const combined = [...vicSeeds, ...pool, ...weakPool];

  for (const q of quotas) {
    let added = 0;
    for (const c of combined) {
      if (picked.length >= targetN) break;
      if (added >= q.n) break;
      if (!q.pred(c)) continue;
      const id = c.candidate_id;
      if (used.has(id)) continue;
      used.add(id);
      picked.push(c);
      added += 1;
    }
  }

  // Fill remainder with diverse countries
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
    family: c.family || c.brand_family_inferred,
    website: c.website || (String(c.origin_url || "").includes("cvent.com") ? null : c.origin_url),
    property_ids: c.property_ids || [],
    candidate_origin: c.candidate_origin,
    strata: c.strata,
    cvent_used_as_production_evidence: false,
  }));

  const frozen_at = new Date().toISOString();
  return {
    frozen_at,
    target: targetN,
    actual: cohort.length,
    composition: {
      existing_vic: cohort.filter((c) => c.strata.existing_vic).length,
      cvent_origin: cohort.filter((c) => c.strata.cvent_origin).length,
      branded: cohort.filter((c) => c.strata.branded).length,
      independent: cohort.filter((c) => c.strata.independent).length,
      soft_collection: cohort.filter((c) => c.strata.soft_collection).length,
      sibling_risk: cohort.filter((c) => c.strata.sibling_risk).length,
      weak_identity: cohort.filter((c) => c.strata.weak_identity).length,
      by_region: cohort.reduce((a, c) => {
        a[c.strata.region] = (a[c.strata.region] || 0) + 1;
        return a;
      }, {}),
      countries: [...new Set(cohort.map((c) => c.country))].sort(),
    },
    cohort,
    note: "Frozen before live research. Not cherry-picked for easy wins.",
  };
}
